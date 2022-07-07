import json
import os
from typing import List, Optional, Union

import dask.array as da
import geopandas as gpd
import pandas as pd
import requests  # type: ignore[import]
import s3fs
from dask_kubernetes import KubeCluster, make_pod_spec
from distributed import Client
from prefect import Flow, Parameter, task
from prefect.client import Secret
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from sentinelhub import CRS, BBox, SHConfig

from oxeo.core.constants import BAND_PREDICTOR_ORDER
from oxeo.core.stac.constants import ELEMENT84_URL, USWEST_URL
from oxeo.flows.models import EventCreate
from oxeo.water.models.segmentation import (
    DaskSegmentationPredictor,
    reconstruct_image_from_patches,
)

Box = tuple[float, float, float, float]


@task(log_stdout=True)
def get_box(aoi_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Box:
    base_url = "https://api.oxfordeo.com/"
    authurl = os.path.join(base_url, "auth", "token")
    r = requests.post(
        authurl, data={"username": "admin@oxfordeo.com", "password": "Helsinki"}
    )
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    query = dict(id=aoi_id)
    aoiurl = os.path.join(base_url, "aoi")
    r = requests.get(aoiurl, headers=headers, json=query)
    j = json.loads(r.text)
    gdf = gpd.GeoDataFrame.from_features(j)
    box: Box = tuple(gdf.total_bounds)  # type: ignore[assignment]
    print(box)
    return box


@task(log_stdout=True)
def create_cluster(n_workers=1, cpu=2, memory="8G") -> Union[KubeCluster, None]:
    if n_workers == 0:
        return None
    image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest"
    pod_spec = make_pod_spec(
        image=image,
        cpu_request=cpu,
        cpu_limit=cpu,
        memory_request=memory,
        memory_limit=memory,
    )
    root_spec = make_pod_spec(image=image)
    return KubeCluster(
        n_workers=n_workers,
        pod_template=pod_spec,
        scheduler_pod_template=root_spec,
    )


@task(log_stdout=True)
def predict(
    aoi_id,
    box: Box,
    start_date,
    end_date,
    cluster: Optional[KubeCluster] = None,
) -> list[EventCreate]:
    shconfig = SHConfig()
    shconfig.sh_base_url = USWEST_URL
    shconfig.sh_client_id = str(Secret("SH_CLIENT_ID").get())
    shconfig.sh_client_secret = str(Secret("SH_CLIENT_SECRET").get())
    os.environ["AWS_REQUEST_PAYER"] = "requester"

    ckpt_path = "s3://oxeo-models/semseg_epoch_012.ckpt"
    # ckpt_path = "../water/data/semseg_epoch_012.ckpt"
    fs = s3fs.S3FileSystem() if ckpt_path.startswith("s3") else None

    bbox = BBox(box, crs=CRS.WGS84)

    print("Creating graph")
    s2_predictor = DaskSegmentationPredictor(
        ckpt_path=ckpt_path,
        fs=fs,
        bands=BAND_PREDICTOR_ORDER["sentinel-2"],
    )
    preds, aoi = s2_predictor.predict_stac_aoi(
        constellation="sentinel-2",
        catalog=ELEMENT84_URL,
        data_collection="sentinel-s2-l2a-cogs",
        bbox=bbox,
        time_interval=(start_date, end_date),
        search_params={},
    )

    print("Submitting to compute")
    if cluster:
        client = Client(cluster)
    else:
        client = Client(n_workers=8, threads_per_worker=1, memory_limit="64GB")
    res = client.compute(preds)

    print("Getting stack")
    stack = da.vstack([e.result() for e in res])

    print(f"Reconstruct stack, {aoi.shape=}, {stack.shape=}")
    mask = reconstruct_image_from_patches(
        stack,
        aoi.shape[0],
        aoi.shape[-2],
        aoi.shape[-1],
        patch_size=250,
    )

    if cluster:
        cluster.close()

    print("Reduce to time series")
    dates = aoi.time.data
    water = (mask == 1).sum(axis=(1, 2)).compute()
    events = [
        EventCreate(
            labels="water_extents",
            aoi_id=aoi_id,
            datetime=pd.Timestamp(d).date(),
            keyed_values={"water_pixels": int(w)},
        )
        for d, w in zip(dates, water)
    ]

    return events


@task(log_stdout=True)
def load(events: List[EventCreate]) -> bool:
    print("Loading events into DB")

    for secret in ["PG_DB_USER", "PG_DB_PW", "PG_DB_HOST", "PG_DB_NAME"]:
        os.environ[secret] = str(Secret(secret).get())

    # TODO
    # These must only be imported here, as api.models.database
    # loads the env vars in global scope and creates the DB URL
    from oxeo.api.controllers.geom import create_events
    from oxeo.api.models.database import SessionLocal

    db = SessionLocal()
    create_events(events, db, None)
    db.close()

    print(f"Successfully inserted {len(events)=} events into the db")

    return True


image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest"
repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/water.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=image,
    )
    with Flow(
        "water",
        storage=storage,
        run_config=run_config,
    ) as flow:
        n_workers = Parameter("n_workers", default=0)
        cpu = Parameter("cpu_per_worker", default=2)
        memory = Parameter("memory_per_worker", default="8G")

        api_username = "admin@oxfordeo.com"
        api_password = PrefectSecret("API_PASSWORD")
        aoi_id = Parameter(name="aoi_id", default=2010)
        start_date = Parameter(name="start_date", default="2020-12-10")
        end_date = Parameter(name="end_date", default="2021-02-01")

        box = get_box(aoi_id, api_username, api_password)
        cluster = create_cluster(n_workers, cpu, memory)
        events = predict(aoi_id, box, start_date, end_date, cluster)
        _ = load(events)

    return flow


flow = create_flow()

if __name__ == "__main__":
    flow.run(executor=LocalExecutor())
