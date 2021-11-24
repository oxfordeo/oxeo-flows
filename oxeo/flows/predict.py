from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union
from uuid import uuid4

import gcsfs
import geopandas as gpd
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
from prefect.tasks.secrets import PrefectSecret
from satextractor.models import Tile
from satextractor.tiler import split_region_in_utm_tiles
from shapely.geometry import MultiPolygon, Polygon

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
from oxeo.flows.utils import (
    data2gdf,
    fetch_water_list,
    generate_run_id,
    parse_water_list,
    rename_flow_run,
)
from oxeo.water.metrics import metrics
from oxeo.water.models import model_factory
from oxeo.water.models.utils import merge_masks


def make_paths(bucket, tiles, constellations):
    return [
        f"{bucket}/prod/{tile.id}/{cons}" for tile in tiles for cons in constellations
    ]


def get_tiles(
    geom: Union[Polygon, MultiPolygon, gpd.GeoSeries, gpd.GeoDataFrame]
) -> List[Tile]:
    try:
        geom = geom.unary_union
    except AttributeError:
        pass
    return split_region_in_utm_tiles(region=geom, bbox_size=10000)


@task
def get_all_paths(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    constellations: List[str],
) -> List[str]:
    logger = prefect.context.get("logger")
    logger.info("Getting all tiles/paths for the total supplied geometry")
    all_tiles = get_tiles(gdf)
    all_paths = make_paths(bucket, all_tiles, constellations)
    return all_paths


@task
def get_water_paths(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    constellations: List[str],
) -> List[Dict]:
    logger = prefect.context.get("logger")
    logger.info("Getting separate paths for each waterbody")
    water_list = gdf.to_dict(orient="records")
    for water in water_list:
        tiles = get_tiles(water["geometry"])
        water["paths"] = make_paths(bucket, tiles, constellations)
    return water_list


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
    return


@task
def merge_to_bq(
    water_dict: Dict,
    model_name: str,
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")
    # TODO Fix this
    # oxeo-water merge_masks wants constellation as a parameter
    # but we should merge all constellations together?
    # Removed last bit of paths as parse_xy in model/utils expects
    # a different path structure (without contellation)
    paths = water_dict["paths"]
    paths = [p.split("/sentinel")[0] for p in paths]

    logger.info(f"Merge all masks in {paths}")
    full_mask, dates = merge_masks(
        paths,
        patch_size=1000,
        data=model_name,
    )
    areas = metrics.segmentation_area(full_mask)

    area_id = water_dict["area_id"]
    run_id = f"{area_id}-{model_name}-{str(uuid4())[:8]}"

    logger.info("Convert results to DataFrame and dict ")
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    df_ts = pd.DataFrame(data={"date": dates, "area": areas}).assign(
        area_id=area_id,
        run_id=run_id,
        pfaf2=pfaf2,
    )
    df_ts.date = df_ts.date.apply(lambda x: x.date())  # remove time

    minx, miny, maxx, maxy = water_dict["geometry"].bounds

    tiles = [p.split("/")[-1] for p in paths]
    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        model=model_name,
        timestamp=timestamp,
        tiles=tiles,
        bbox_n=maxy,
        bbox_s=miny,
        bbox_w=minx,
        bbox_e=maxx,
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
        "machine_type": "n1-highmem-32",
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
    # secrets
    postgis_password = PrefectSecret("POSTGIS_PASSWORD")

    # parameters
    water_list = Parameter(name="water_list", default=[25906112, 25906127])
    model_name = Parameter(name="model_name", default="pekel")

    credentials = Parameter(name="credentials", default=default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    bucket = Parameter(name="bucket", default="oxeo-water")

    constellations = Parameter(name="constellations", default=["sentinel-2"])

    # rename the Flow run to reflect the parameters
    water_list = parse_water_list(water_list)
    run_id = generate_run_id(water_list)
    rename_flow_run(run_id)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)

    # start processing
    all_paths = get_all_paths(gdf, bucket, constellations)

    # create_masks() is mapped in parallel across all the paths
    # the returned masks is an empty list purely for the DAG
    masks = create_masks.map(
        path=all_paths,
        model_name=unmapped(model_name),
        project=unmapped(project),
        credentials=unmapped(credentials),
    )

    # now instead of mapping across all paths, we map across
    # individual lakes
    water_paths = get_water_paths(gdf, bucket, constellations)
    merge_to_bq.map(
        water_dict=water_paths,
        model_name=unmapped(model_name),
        upstream_tasks=[unmapped(masks)],
    )
