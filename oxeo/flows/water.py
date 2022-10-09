import json
import os
from typing import Optional, Union

import geopandas as gpd
import httpx
import pandas as pd
import prefect
import s3fs
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, Parameter, task
from prefect.client import Secret
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from sentinelhub import CRS, BBox, SHConfig

from oxeo.core.stac.constants import ELEMENT84_URL, LANDSATLOOK_URL, USWEST_URL

Box = tuple[float, float, float, float]

image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:torch-latest"
repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


@task(log_stdout=True)
def get_box(aoi_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Box:
    print(f"{aoi_id=}")
    # login
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    base_url = "https://api.oxfordeo.com/"
    client = httpx.Client(base_url=base_url)

    print("Get token")
    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    print("Get AOI")
    r = client.get("aoi/", params=dict(id=aoi_id), headers=headers)
    j = json.loads(r.text)

    print("Close httpx client")
    client.close()
    gdf = gpd.GeoDataFrame.from_features(j)
    box: Box = tuple(gdf.total_bounds)  # type: ignore[assignment]
    print(box)
    return box


@task(log_stdout=True)
def create_cluster(n_workers=1, cpu=2, memory="8G") -> Union[KubeCluster, None]:
    if n_workers == 0:
        return None
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
    constellation,
    cluster: Optional[KubeCluster] = None,
) -> list[dict]:
    from oxeo.water.models.segmentation import (
        DaskSegmentationPredictor,
        reconstruct_image_from_patches,
        reduce_to_timeseries,
        stack_preds,
    )

    assert constellation in [
        "sentinel-2",
        "landsat-5",
        "landsat-7",
        "landsat-8",
    ], """constalltion must be one of ['sentinel-2','landsat-5','landsat-7','landsat-8']"""

    shconfig = SHConfig()
    shconfig.sh_client_id = str(Secret("SH_CLIENT_ID").get())
    shconfig.sh_client_secret = str(Secret("SH_CLIENT_SECRET").get())
    os.environ["AWS_REQUEST_PAYER"] = "requester"

    ckpt_path = "s3://oxeo-models/semseg_epoch_012.ckpt"
    # ckpt_path = "../water/data/semseg_epoch_012.ckpt"
    fs = s3fs.S3FileSystem() if ckpt_path.startswith("s3") else None

    bbox = BBox(box, crs=CRS.WGS84)

    # print("Connect client")
    # if cluster:
    #     client = Client(cluster)
    # else:
    #     client = Client(n_workers=8, threads_per_worker=1, memory_limit="64GB")

    if "sentinel" in constellation.lower():
        URL = ELEMENT84_URL
        collection = "sentinel-s2-l2a-cogs"
        search_params = {
            "query": {
                "eo:cloud_cover": {"gte": 0, "lte": 10},
            }
        }
        const_str = "sentinel-2"
    elif "landsat" in constellation.lower():
        URL = LANDSATLOOK_URL
        shconfig.sh_base_url = USWEST_URL
        collection = "landsat-c2l2-sr"
        search_params = {
            "query": {
                "platform": {"in": [constellation.upper().replace("-", "_")]},
                "eo:cloud_cover": {"gte": 0, "lte": 20},
            }
        }  # noqa
        const_str = "landsat"

    print(f"URL/catalog: {URL}")
    print(f"const_str: {const_str}")
    print(f"collection: {collection}")
    print("search params")
    print(json.dumps(search_params))

    print("Creating graph")
    predictor = DaskSegmentationPredictor(
        ckpt_path=ckpt_path,
        fs=fs,
    )
    preds, aoi = predictor.predict_stac_aoi(
        constellation=const_str,
        catalog=URL,
        data_collection=collection,
        bbox=bbox,
        time_interval=(start_date, end_date),
        search_params=search_params,
        resolution=10,
    )

    stack = stack_preds(preds)
    revisits, _, target_h, target_w = aoi.shape
    mask = reconstruct_image_from_patches(
        stack, revisits, target_h, target_w, patch_size=250
    )
    ts = reduce_to_timeseries(mask).compute()
    dates = aoi.time.data

    print("Create Events")
    dates = aoi.time.data
    events = [
        dict(
            labels=["water_extents"],
            aoi_id=aoi_id,
            datetime=pd.Timestamp(d).date().isoformat()[0:10],
            keyed_values={"water_pixels": int(w)},
        )
        for d, w in zip(dates, ts)
    ]

    return events


@task(log_stdout=True)
def load(events: list[dict], U: Optional[str] = None, P: Optional[str] = None) -> bool:
    print("Loading events into DB")

    logger = prefect.context.get("logger")

    # refresh headers
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    base_url = "https://api.oxfordeo.com/"
    client = httpx.Client(base_url=base_url)

    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    r = client.post("events/", json=events, headers=headers)
    if str(r.status_code) != "201":
        raise ValueError(f"Status code: {r.status_code}")

    client.close()

    logger.info(f"Successfully inserted {len(events)} events into the db")

    return True


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/water.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=image,
        cpu_limit=8,
        cpu_request=8,
        memory_limit="64G",
        memory_request="64G",
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
        constellation = Parameter(name="constellation", default="sentinel-2")
        start_date = Parameter(name="start_date", default="2020-12-10")
        end_date = Parameter(name="end_date", default="2021-02-01")

        box = get_box(aoi_id, api_username, api_password)
        cluster = create_cluster(n_workers, cpu, memory)
        events = predict(aoi_id, box, start_date, end_date, constellation, cluster)
        _ = load(events, api_username, api_password)

    return flow


flow = create_flow()

if __name__ == "__main__":
    flow.run(
        parameters=dict(
            aoi_id=2015,
            start_date="2021-01-01",
            end_date="2021-12-31",
            constellation="sentinel-2",
        ),
        executor=LocalExecutor(),
    )
