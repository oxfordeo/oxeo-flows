from pathlib import Path
from typing import List

import gcsfs
import numpy as np
import prefect
import zarr
from dask_cloudprovider.gcp import GCPCluster
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GitHub

from oxeo.water.models.pekel.pekel import PekelPredictor


@task
def get_paths(
    project: str,
    credentials: Path,
    bucket: str,
    aoi_id: str,
    constellations: List[str],
) -> List[Path]:
    logger = prefect.context.get("logger")
    logger.info("Getting all tiles for and constellations")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    tiles = [p.split("/")[-1] for p in fs.ls(f"oxeo-water/{aoi_id}")]

    paths = []
    for tile in tiles:
        for cons in constellations:
            paths.append(f"{bucket}/{aoi_id}/{tile}/{cons}")
    logger.info(f"Got all paths: {paths}")

    return paths


@task
def create_masks(
    project: str,
    credentials: str,
    path: Path,
) -> None:
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Creating mask for {path} on: {task_full_name}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    predictor = PekelPredictor()

    mapper = fs.get_mapper(f"{path}/data")
    arr = zarr.open(mapper, "r")

    masks = predictor.predict(
        arr,
        bands_indexes={
            "red": 3,
            "green": 2,
            "blue": 1,
            "nir": 7,
            "swir1": 11,
            "swir2": 12,
        },
        compute=False,
    )
    masks = np.array(masks)

    mask_mapper = fs.get_mapper(f"{path}/masks")
    mask_arr = zarr.open_array(
        mask_mapper,
        "w",
        shape=masks.shape,
        chunks=(1, 1000, 1000),
        dtype=np.uint8,
    )
    mask_arr[:] = masks
    logger.info(f"Successfully created masks for {path} on: {task_full_name}")


ephemeral_executor = DaskExecutor(
    cluster_class=GCPCluster,
    adapt_kwargs={"maximum": 10},
    cluster_kwargs={
        "projectid": "oxeo-main",
        "zone": "europe-west4-a",
        "network": "dask",
        "machine_type": "n1-highmem-2",
        "source_image": "packer-1636915174",
        "docker_image": "eu.gcr.io/oxeo-main/oxeo-flows:latest",
    },
)
long_executor = DaskExecutor(address="tcp://<ip>:<port>")
local_executor = DaskExecutor()
storage = GitHub(
    repo="oxfordeo/oxeo-flows",
    path="flows/predict.py",
    access_token_secret="GITHUB",
)
run_config = VertexRun(
    labels=["vertex"],
    image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
    machine_type="n1-highmem-2",
    network="projects/292453623103/global/networks/dask",
)
with Flow(
    "predict",
    executor=local_executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # parameters
    aoi_id = Parameter(name="aoi_id", required=True)

    credentials = Parameter(name="credentials", default="token.json")
    project = Parameter(name="project", default="oxeo-main")
    gcp_region = Parameter(name="gcp_region", default="europe-west4")
    bucket = Parameter(name="bucket", default="oxeo-water")

    constellations = Parameter(
        name="constellations",
        default=["sentinel-2"],
        # default=["sentinel-2", "landsat-5", "landsat-7", "landsat-8"],
    )

    paths = get_paths(project, credentials, bucket, aoi_id, constellations)
    create_masks.map(
        path=paths,
        project=unmapped(project),
        credentials=unmapped(credentials),
    )
