import json
import os
from datetime import timedelta
from typing import Optional

import geopandas as gpd
import httpx
import numpy as np
import pandas as pd
import prefect
from dask.distributed import LocalCluster
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, Parameter, task
from prefect.executors import DaskExecutor, LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from sentinelhub import CRS, BBox

from oxeo.flows.models import EventCreate
from oxeo.water.models.ndvi import NDVIPredictor

Box = tuple[float, float, float, float]


@task(log_stdout=True)
def get_box(aoi_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Box:
    print(f"aoi_id={aoi_id}")
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


@task(log_stdout=True, max_retries=1, retry_delay=timedelta(seconds=10))
def transform(
    aoi_id: int,
    box: Box,
    start_datetime: str,
    end_datetime: str,
    catalog: str,
    data_collection: str,
    search_params: str,
    AWS_ACCESS_KEY_ID: str,
    AWS_SECRET_ACCESS_KEY: str,
) -> list[EventCreate]:
    logger = prefect.context.get("logger")
    logger.info("NDVI transforming.")

    search_params = json.loads(search_params)
    logger.info(f"Search params: {search_params}")

    os.environ[
        "AWS_ROLE_ARN"
    ] = "arn:aws:iam::413730540186:role/eksctl-oxeo-eks-cluster-ServiceRole-EGMEOTXRMYW9"
    os.environ["AWS_REGION"] = "eu-central-1"
    os.environ["AWS_REQUEST_PAYER"] = "requester"
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"

    AWS_REQUEST_PAYER = os.environ.get("AWS_REQUEST_PAYER")
    AWS_REGION = os.environ.get("AWS_REGION")

    logger.info("PRINT ENV")
    logger.info(f"AWS_REQUEST_PAYER={AWS_REQUEST_PAYER}")
    logger.info(f"AWS_REGION={AWS_REGION}")
    logger.info(f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}")
    logger.info(f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}")
    logger.info("ENVIRON")
    logger.info(json.dumps({kk: vv for kk, vv in os.environ.items()}))

    if os.path.exists("~/.aws/credentials"):
        with open("~/.aws/credentials") as f:
            creds = f.read()
            logger.info("CREDS")
            logger.info(creds)

    else:
        if not os.path.exists("~/.aws"):
            os.mkdir("~/.aws")
        with open("~/.aws/credentials", "w") as f:
            f.write("[default]\n")
            f.write(f"aws_access_key_id={AWS_ACCESS_KEY_ID}\n")
            f.write(f"aws_secret_access_key={AWS_SECRET_ACCESS_KEY}\n")
            f.write("region=eu-central-1\n")

    # test rasterio open
    # url = "s3://usgs-landsat/collection02/level-2/standard/etm/2012/169/074/LE07_L2SP_169074_20120519_20200908_02_T1/LE07_L2SP_169074_20120519_20200908_02_T1_SR_B4.TIF"
    # ds = rasterio.open(url)

    # 1. test read window
    # w = Window(1000, 1000, 500, 500)
    # data = ds.read(window=w)
    # logger.info(f"DATA={data.mean()}")
    # logger.info(f"WIDTH={ds.width}")

    # 2. test subprocess cli : aws s3 ls s3://usgs-landsat/collection02/level-2/standard/etm/2013/169/074/LE07_L2SP_169074_20130506_20200907_02_T1/ --request-payer | grep LE07_L2SP_169074_20130506_20200907_02_T1_SR_B3 # noqa

    # 3. boto3 session as env
    # s = boto3.session.Session(
    #     aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    #     aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    # )

    env = None  # LayeredEnv(always=rasterio.Env(AWSSession(s)))

    # logger.info("PKGS")
    # pkgs = freeze.freeze()
    # pkgs = [pkg for pkg in pkgs]
    # logger.info(";".join(pkgs))

    bbox = BBox(box, crs=CRS.WGS84)

    predictor = NDVIPredictor()
    ndvi = predictor.predict_stac_aoi(
        catalog=catalog,
        data_collection=data_collection,
        bbox=bbox,
        time_interval=(start_datetime, end_datetime),
        search_params=search_params,
        env=env,
    )

    # call the compute with the dask backend
    ndvi_red = ndvi.mean(dim=["x", "y"])
    res = ndvi_red.compute()

    ndvi_ts = res.data
    ndvi_ts = np.nan_to_num(ndvi_ts, nan=0)
    dates = ndvi_red.time.data

    events = [
        dict(
            labels=["ndvi"],
            aoi_id=aoi_id,
            datetime=pd.Timestamp(d).date().isoformat()[0:10],
            keyed_values={"mean_value": float(w)},
        )
        for d, w in zip(dates, ndvi_ts)
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


image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest"
repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


def dynamic_cluster(**kwargs):
    n_workers = prefect.context.parameters["n_workers"]
    memory = prefect.context.parameters["memory_per_worker"]
    cpu = prefect.context.parameters["cpu_per_worker"]
    gpu = prefect.context.parameters["gpu_per_worker"]

    if n_workers == 0:
        return LocalCluster()

    logger = prefect.context.get("logger")
    logger.info(f"Creating cluster with cpu={cpu}, memory={memory}, gpu={gpu}")
    if gpu > 0:
        logger.warning("Creating GPU cluster!")

    container_config = {
        "resources": {
            "limits": {
                "cpu": cpu,
                "memory": memory,
                "nvidia.com/gpu": gpu,
            },
            "requests": {
                "cpu": cpu,
                "memory": memory,
                "nvidia.com/gpu": gpu,
            },
        }
    }

    pod_spec = make_pod_spec(
        image=image,
        extra_container_config=container_config,
        memory_limit=memory,
    )
    pod_spec.spec.containers[0].args.append("--no-dashboard")

    root_spec = make_pod_spec(image=image)
    root_spec.spec.containers[0].args.append("--no-dashboard")

    return KubeCluster(
        n_workers=n_workers,
        pod_template=pod_spec,
        scheduler_pod_template=root_spec,
        **kwargs,
    )


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/ndvi.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=image,
        cpu_limit=4,
        cpu_request=4,
        memory_limit="16G",
        memory_request="16G",
        env={
            "AWS_REQUEST_PAYER": "requester",
            "AWS_REGION": "eu-central-1",
        },
    )
    executor = DaskExecutor(
        cluster_class=dynamic_cluster,
        adapt_kwargs={"maximum": 8},
    )

    with Flow(
        "ndvi",
        storage=storage,
        run_config=run_config,
        executor=executor,
    ) as flow:
        flow.add_task(Parameter("n_workers", default=0))
        flow.add_task(Parameter("memory_per_worker", default="8G"))
        flow.add_task(Parameter("cpu_per_worker", default=2))
        flow.add_task(Parameter("gpu_per_worker", default=0))

        api_username = "admin@oxfordeo.com"
        api_password = PrefectSecret("API_PASSWORD")
        AWS_ACCESS_KEY_ID = PrefectSecret("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = PrefectSecret("AWS_SECRET_ACCESS_KEY")
        aoi_id = Parameter(name="aoi_id", default=1)

        start_datetime = Parameter(name="start_datetime", default="2020-01-01")
        end_datetime = Parameter(name="end_datetime", default="2020-01-08")
        catalog = Parameter(
            name="catalog", default="https://earth-search.aws.element84.com/v0"
        )
        data_collection = Parameter(
            name="data_collection", default="sentinel-s2-l2a-cogs"
        )
        search_params = Parameter(
            name="search_params", default=json.dumps({"max_items": None})
        )

        box = get_box(aoi_id, api_username, api_password)
        events = transform(
            aoi_id,
            box,
            start_datetime,
            end_datetime,
            catalog,
            data_collection,
            search_params,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
        )
        _ = load(events, api_username, api_password)

    return flow


flow = create_flow()

if __name__ == "__main__":
    flow.run(
        parameters=dict(
            aoi_id=2179,
            start_datetime="1981-01-01",
            end_datetime="2021-12-31",
            catalog="https://landsatlook.usgs.gov/stac-server",
            data_collection="landsat-c2l2-sr",
            search_params=json.dumps(
                {
                    "query": {
                        "eo:cloud_cover": {"gte": 0, "lte": 10},
                    }
                }
            ),
        ),
        executor=LocalExecutor(),
    )
