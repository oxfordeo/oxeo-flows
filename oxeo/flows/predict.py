from datetime import datetime
from pathlib import Path
from typing import List

import gcsfs
import numpy as np
import pandas as pd
import prefect
import zarr
from dask_cloudprovider.gcp import GCPCluster
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GitHub

from oxeo.flows import (
    dask_gcp_zone,
    dask_image,
    dask_network,
    dask_network_full,
    dask_projectid,
    default_gcp_token,
    docker_oxeo_flows,
    prefect_secret_github_token,
    repo_name,
)
from oxeo.flows.utils import rename_flow_run
from oxeo.water.metrics import metrics
from oxeo.water.models import model_factory
from oxeo.water.models.utils import merge_masks


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
    path: Path,
    model_name: str,
    project: str,
    credentials: str,
) -> None:
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Creating mask for {path} on: {task_full_name}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    predictor = model_factory(model_name).predictor()

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

    mask_mapper = fs.get_mapper(f"{path}/{model_name}")
    mask_arr = zarr.open_array(
        mask_mapper,
        "w",
        shape=masks.shape,
        chunks=(1, 1000, 1000),
        dtype=np.uint8,
    )
    mask_arr[:] = masks
    logger.info(f"Successfully created masks for {path} on: {task_full_name}")
    return path


@task
def merge_to_bq(
    paths: List[str],
    model_name: str,
) -> None:
    logger = prefect.context.get("logger")
    # TODO Fix this
    # oxeo-water merge_masks wants constellation as a parameter
    # but we should merge all constellations together?
    # Removed last bit of paths as parse_xy in model/utils expects
    # a different path structure (without contellation)
    paths = [p.split("/sentinel")[0] for p in paths]

    logger.info(f"Merge all masks in {paths}")
    full_mask, dates = merge_masks(
        paths,
        patch_size=1000,
        data=model_name,
    )
    areas = metrics.segmentation_area(full_mask)

    logger.info("Convert results to DataFrame and dict ")
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    df_ts = pd.DataFrame(data={"date": dates, "area": areas}).assign(
        water_id=9876,
        model=model_name,
        generated=timestamp,
    )
    df_ts.date = df_ts.date.apply(lambda x: x.date())

    tiles = [p.split("/")[-1] for p in paths]
    dict_water = dict(
        water_id=9876,
        model="pekel",
        generated=timestamp,
        tiles=tiles,
        north_lat=42.2,
        south_lat=42.2,
        west_lon=42.2,
        east_lon=42.2,
    )

    logger.info("Insert results into BigQuery")
    client = bigquery.Client()

    table = client.get_table("oxeo-main.water.water_ts")
    errors = client.insert_rows_from_dataframe(table, df_ts)
    if errors != [[]]:
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )

    errors = client.insert_rows_json("oxeo-main.water.water_model_runs", [dict_water])
    if errors != []:
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )


executor = DaskExecutor(
    cluster_class=GCPCluster,
    adapt_kwargs={"maximum": 10},
    cluster_kwargs={
        "projectid": dask_projectid,
        "zone": dask_gcp_zone,
        "network": dask_network,
        "machine_type": "n1-highmem-2",
        "source_image": dask_image,
        "docker_image": docker_oxeo_flows,
    },
)
storage = GitHub(
    repo=repo_name,
    path="oxeo/flows/predict.py",
    access_token_secret=prefect_secret_github_token,
)
run_config = VertexRun(
    labels=["vertex"],
    image=docker_oxeo_flows,
    machine_type="n1-highmem-2",
    network=dask_network_full,
)
with Flow(
    "predict",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # parameters
    aoi_id = Parameter(name="aoi_id", required=True)
    model_name = Parameter(name="model_name", default="pekel")

    credentials = Parameter(name="credentials", default=default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    bucket = Parameter(name="bucket", default="oxeo-water")

    constellations = Parameter(
        name="constellations",
        default=["sentinel-2"],
        # default=["sentinel-2", "landsat-5", "landsat-7", "landsat-8"],
    )

    rename_flow_run(aoi_id)

    paths = get_paths(project, credentials, bucket, aoi_id, constellations)

    # create_masks() is mapped in parallel across all the paths
    paths = create_masks.map(
        path=paths,
        model_name=unmapped(model_name),
        project=unmapped(project),
        credentials=unmapped(credentials),
    )

    # this is called without map() so paths are
    # automatically reduced back down to a List[str]
    merge_to_bq(paths, model_name=model_name)
