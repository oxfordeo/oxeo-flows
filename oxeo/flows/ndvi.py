import json
import os
from datetime import datetime, timedelta
from typing import List, Optional

import dateparser  # type: ignore
import prefect
import pystac_client
import requests  # type: ignore
import stackstac
from dask.distributed import LocalCluster
from dask_kubernetes import KubeCluster, make_pod_spec
from prefect import Flow, Parameter, task, unmapped
from prefect.client import Secret
from prefect.executors import DaskExecutor, LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from pyproj import CRS, Transformer
from pystac.item import Item
from rasterio import features
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from shapely import geometry
from shapely.ops import transform as transform_proj

from oxeo.flows.models import EventCreate, Feature


@task(log_stdout=True, max_retries=1, retry_delay=timedelta(seconds=10))
def extract(_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Feature:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting AOI {_id}")

    logger.warning("Checking torch and CUDA")
    try:
        import torch

        logger.warning(f"{torch.cuda.is_available()=}")
        logger.warning(f"{torch.cuda.device_count()=}")
    except Exception:
        pass

    # login
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    base_url = "https://api.oxfordeo.com/"

    authurl = os.path.join(base_url, "auth", "token")
    r = requests.post(authurl, data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]

    headers = {"Authorization": f"Bearer {token}"}

    # query
    query = dict(
        id=_id,
    )

    aoiurl = os.path.join(base_url, "aoi")
    r = requests.get(aoiurl, headers=headers, json=query)
    j = json.loads(r.text)
    return Feature(**j["features"][0])


@task(log_stdout=True)
def stac(
    aoi: Feature,
    start_datetime: datetime,
    end_datetime: datetime,
) -> List[Item]:
    logger = prefect.context.get("logger")

    URL = "https://earth-search.aws.element84.com/v0"
    catalog = pystac_client.Client.open(URL)

    start_datetime = dateparser.parse(start_datetime)
    end_datetime = dateparser.parse(end_datetime)
    print(start_datetime, end_datetime)

    items = catalog.search(
        intersects=geometry.mapping(
            geometry.box(*(geometry.shape(aoi.geometry.__dict__).bounds))
        ),
        collections=["sentinel-s2-l2a-cogs"],
        datetime=f"{start_datetime.isoformat()[0:10]}/{end_datetime.isoformat()[0:10]}",
    ).get_all_items()

    logger.info(f"Got {len(items)} STAC items")

    return items


@task(log_stdout=True)
def transform(aoi: Feature, item: Item) -> EventCreate:
    logger = prefect.context.get("logger")
    logger.info("NDVI transforming.")

    geom = geometry.shape(aoi.geometry.__dict__)

    xr_stack = stackstac.stack(
        item,
        resolution=10,
        bounds_latlon=geom.bounds,
        resampling=Resampling.bilinear,
        errors_as_nodata=(
            IOError(r"HTTP response code: \d\d\d"),
            RasterioIOError(".*"),
        ),
    )

    # swap geom to utm
    crs_wgs84 = CRS("EPSG:4326")
    crs_utm = CRS(xr_stack.crs)
    wgs2utm = Transformer.from_crs(crs_wgs84, crs_utm, always_xy=True).transform
    utm_geom = transform_proj(wgs2utm, geom)

    out_shp = (xr_stack.coords["y"].shape[0], xr_stack.coords["x"].shape[0])

    # burn in mask
    mask_arr = features.rasterize(
        [utm_geom],
        out_shape=out_shp,
        fill=0,
        transform=xr_stack.transform,
        all_touched=False,
        default_value=1,
        dtype=None,
    )

    # build computation graph for NDVI: (NIR-red) / (NIR+RED)
    xr_stack.coords["mask"] = (("y", "x"), mask_arr)

    xr_stack = xr_stack.where(xr_stack.mask == 1)

    xr_ndvi = (xr_stack.sel({"band": "B08"}) - xr_stack.sel({"band": "B04"})) / (
        xr_stack.sel({"band": "B08"}) + xr_stack.sel({"band": "B04"})
    )
    xr_ndvi = xr_ndvi.mean(dim=["x", "y"])

    # call the compute with the dask backend
    result = xr_ndvi.compute()

    # cast to pandas
    df = result.to_pandas()
    df.index = df.index.date

    event = EventCreate(
        labels="ndvi",
        aoi_id=aoi.id,
        datetime=df.index[0],
        keyed_values={"mean_value": df[0]},
    )

    return event


@task(log_stdout=True)
def load(events: List[EventCreate]) -> bool:
    print("Loading events into DB")

    for secret in ["PG_DB_USER", "PG_DB_PW", "PG_DB_HOST", "PG_DB_NAME"]:
        os.environ[secret] = Secret(secret).get()

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


def dynamic_cluster(**kwargs):
    n_workers = prefect.context.parameters["n_workers"]
    memory = prefect.context.parameters["memory_per_worker"]
    cpu = prefect.context.parameters["cpu_per_worker"]
    gpu = prefect.context.parameters["gpu_per_worker"]
    
    if n_workers == 0:
        return LocalCluster()

    logger = prefect.context.get("logger")
    logger.info(f"Creating cluster with {cpu=}, {memory=}, {gpu=}")
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
        flow.add_task(Parameter("n_workers", default=3))
        flow.add_task(Parameter("memory_per_worker", default="8G"))
        flow.add_task(Parameter("cpu_per_worker", default=2))
        flow.add_task(Parameter("gpu_per_worker", default=0))

        api_username = "admin@oxfordeo.com"
        api_password = PrefectSecret("API_PASSWORD")
        aoi_id = Parameter(name="aoi_id", default=1)
        start_datetime = Parameter(name="start_datetime", default="2020-01-01")
        end_datetime = Parameter(name="end_datetime", default="2020-01-08")

        aoi = extract(aoi_id, api_username, api_password)
        items = stac(aoi, start_datetime, end_datetime)
        events = transform.map(unmapped(aoi), items)
        _ = load(events)

    return flow


flow = create_flow()

if __name__ == "__main__":
    flow.run(executor=LocalExecutor())
