import json
import os
from typing import Optional, Union

import geopandas as gpd
import httpx
import pandas as pd
import prefect
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, Parameter, task
from prefect.client import Secret
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from sentinelhub import CRS, BBox, DataCollection, SentinelHubCatalog, SHConfig

from oxeo.water.models.soil_moisture import SoilMoisturePredictor

Box = tuple[float, float, float, float]

image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest"
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
    cluster: Optional[KubeCluster] = None,
) -> list[dict]:

    shconfig = SHConfig()
    shconfig.sh_client_id = str(Secret("SH_CLIENT_ID").get())
    shconfig.sh_client_secret = str(Secret("SH_CLIENT_SECRET").get())
    os.environ["AWS_REQUEST_PAYER"] = "requester"

    eu_catalog = SentinelHubCatalog(shconfig)

    bbox = BBox(box, crs=CRS.WGS84)

    # s2_predictor = DaskSegmentationPredictor(
    #    ckpt_path=ckpt_path,
    #    fs=fs,
    #    bands=BAND_PREDICTOR_ORDER["sentinel-2"],
    # )

    bbox = BBox(box, crs=CRS.WGS84)

    soil_predictor = SoilMoisturePredictor()

    soil_moisture = soil_predictor.predict_stac_aoi(
        catalog=eu_catalog,
        data_collection=DataCollection.SENTINEL1,
        bbox=bbox,
        time_interval=(start_date, end_date),
        search_params={},
        orbit_state="all",
        resolution=10,
    )

    soil_moisture = soil_moisture.mean(dim=("x", "y")).compute()

    # preds, aoi = s2_predictor.predict_stac_aoi(
    #    constellation="sentinel-2",
    #    catalog=ELEMENT84_URL,
    #    data_collection="sentinel-s2-l2a-cogs",
    #    bbox=bbox,
    #    time_interval=
    #    search_params={},
    # )

    print("got reduced aoi")
    soil_moisture_ts = soil_moisture.data
    dates = soil_moisture.time.data

    print("Create Events")
    events = [
        dict(
            labels=["soil_moisture"],
            aoi_id=aoi_id,
            datetime=pd.Timestamp(d).date().isoformat()[0:10],
            keyed_values={"mean_value": float(w)},
        )
        for d, w in zip(dates, soil_moisture_ts)
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
        path="oxeo/flows/soil_moisture.py",
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
        "soil_moisture",
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
        _ = load(events, api_username, api_password)

    return flow


flow = create_flow()

if __name__ == "__main__":
    flow.run(
        parameters=dict(
            aoi_id=2179,
            start_date="2016-01-01",
            end_date="2021-01-01",
        ),
        executor=LocalExecutor(),
    )
