import json
import subprocess
import time
from pathlib import Path
from typing import List, Tuple, Union
from uuid import uuid4

import geopandas as gpd
import prefect
import pystac
from google.cloud import bigquery
from prefect import Flow, Parameter, task
from prefect.client import Client
from prefect.executors import DaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GitHub
from satextractor.deployer import deploy_tasks
from satextractor.models import ExtractionTask, Tile
from satextractor.preparer.gcp_preparer import gcp_prepare_archive
from satextractor.scheduler import create_tasks_by_splits
from satextractor.stac import gcp_region_to_item_collection
from satextractor.tiler import split_region_in_utm_tiles
from shapely.geometry import MultiPolygon, Polygon


@task
def rename_flow_run(
    aoi_id: int,
) -> None:
    logger = prefect.context.get("logger")
    old_name = prefect.context.get("flow_run_name")
    new_name = f"run_{aoi_id}"
    logger.info(f"Rename the Flow Run from {old_name} to {new_name}")
    Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)


@task
def get_id_and_geom(
    aoi: str,
) -> Tuple[str, Union[Polygon, MultiPolygon]]:
    logger = prefect.context.get("logger")
    logger.info("Determining how to parse AOI")
    try:  # first assume that aoi is dict GeoJSON
        geom = gpd.GeoDataFrame.from_features(aoi).unary_union
        aoi_id = str(uuid4())[:8]
        logger.info("Loaded AOI as dict GeoJSON and created aoi_id")
    except TypeError:
        try:  # then try with aoi as a str GeoJSON
            geom = gpd.GeoDataFrame.from_features(json.load(aoi)).unary_union
            aoi_id = str(uuid4())[:8]
            logger.info("Loaded AOI as str GeoJSON and created aoi_id")
        except (json.decoder.JSONDecodeError, AttributeError):
            try:  # then try load it from a file
                geom = gpd.read_file(aoi).unary_union
                aoi_id = str(uuid4())[:8]
                logger.info("Loaded AOI as file and created aoi_id")
            except ValueError:  # fall back to pulling it from the DB
                aoi_id = aoi
                geom = get_db_geom(aoi_id)
                logger.info("Used AOI as id to load from DB")
    return aoi_id, geom


@task
def get_db_geom(
    aoi_id: str,
) -> Union[Polygon, MultiPolygon]:
    raise NotImplementedError("DB geometry functionality not created yet!")


@task
def get_storage_path(
    storage_root: str,
    aoi_id: str,
) -> str:
    return f"gs://{storage_root}/{aoi_id}"


@task
def build(
    project: str,
    gcp_region: str,
    storage_root: str,
    credentials: Path,
    user_id: str,
) -> None:
    logger = prefect.context.get("logger")

    logger.info("Checking that Google Cloud Run and PubSub resources exist")
    name = f"{user_id}-stacextractor"
    cmd = [
        "gcloud",
        "run",
        "services",
        "describe",
        name,
        f"--project={project}",
        f"--region={gcp_region}",
        "--platform=managed",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if not p.stderr == "":
        logger.error(p.stderr)
        logger.error(f"Couldn't find Cloud Run: {name}")
        raise Exception

    cmd = [
        "gcloud",
        "pubsub",
        "subscriptions",
        "describe",
        name,
        f"--project={project}",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if not p.stderr == "":
        logger.error(p.stderr)
        logger.error(f"Couldn't find PubSub subscription: {name}")
        raise Exception

    return True


@task
def stac(
    credentials: Path,
    start_date: str,
    end_date: str,
    constellations: str,
    region: Union[Polygon, MultiPolygon],
) -> pystac.ItemCollection:
    logger = prefect.context.get("logger")
    logger.info("Converting data to STAC")
    item_collection = gcp_region_to_item_collection(
        credentials=credentials,
        region=region,
        start_date=start_date,
        end_date=end_date,
        constellations=constellations,
    )
    return item_collection


@task
def tiler(
    bbox_size: int,
    region: Union[Polygon, MultiPolygon],
) -> List[Tile]:
    logger = prefect.context.get("logger")
    logger.info("Creating tiles")
    tiles = split_region_in_utm_tiles(
        region=region,
        # crs=,
        bbox_size=(bbox_size, bbox_size),
    )
    return tiles


@task
def scheduler(
    constellations: List[str],
    tiles: List[Tile],
    item_collection: pystac.ItemCollection,
    split_m: int,
) -> List[ExtractionTask]:
    logger = prefect.context.get("logger")
    logger.info("Create extraction tasks")
    extraction_tasks = create_tasks_by_splits(
        tiles=tiles,
        split_m=split_m,
        item_collection=item_collection,
        constellations=constellations,
        bands=None,
        interval=1,
        n_jobs=-1,
        verbose=0,
    )
    return extraction_tasks


@task
def preparer(
    credentials: Path,
    constellations: List[str],
    storage_path: str,
    bbox_size: int,
    chunk_size: int,
    tiles: List[Tile],
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Prepare Cloud Storage bucket")
    gcp_prepare_archive(
        credentials=credentials,
        tasks=extraction_tasks,
        tiles=tiles,
        constellations=constellations,
        storage_root=storage_path,
        patch_size=bbox_size,
        chunk_size=chunk_size,
        n_jobs=-1,
        verbose=0,
    )


@task
def deployer(
    project: str,
    user_id: str,
    credentials: Path,
    storage_path: str,
    chunk_size: int,
    extraction_tasks: List[ExtractionTask],
) -> str:
    logger = prefect.context.get("logger")
    logger.info("Deploy tasks to Cloud RUn")

    topic = f"projects/{project}/topics/{'-'.join([user_id, 'stacextractor'])}"
    job_id = deploy_tasks(
        credentials=credentials,
        extraction_tasks=extraction_tasks,
        storage_path=storage_path,
        chunk_size=chunk_size,
        topic=topic,
    )
    return job_id


@task
def check_deploy_completion(
    project: str,
    user_id: str,
    job_id: str,
    extraction_tasks: List[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    tot = len(extraction_tasks)

    prev_done = -1
    loops_unchanged = 0
    max_loops_no_progress = 5
    base_sleep = 10

    while True:
        client = bigquery.Client()
        table = f"{project}.satextractor.{user_id}"
        query = f"SELECT COUNT(msg_type) FROM {table} WHERE job_id = '{job_id}' AND msg_type = 'FINISHED'"
        df = client.query(query).result().to_dataframe()
        done = int(df.iat[0, 0])
        logger.info(f"Cloud Run extraction tasks: {done} of {tot}")

        if done == tot:
            logger.info(f"All {tot} extraction tasks done!")
            return
        elif done == prev_done:
            loops_unchanged += 1
            if loops_unchanged >= max_loops_no_progress:
                logger.info(f"No progress on tasks after {loops_unchanged} loops")
                raise Exception("No further progress on tasks")
        else:
            loops_unchanged = 0

        # Exponential backoff, only when nothing has changed
        sleep = base_sleep * (2 ** (loops_unchanged + 1) - 1)
        logger.info(f"Sleep for {sleep} seconds")
        time.sleep(sleep)


executor = DaskExecutor()
storage = GitHub(
    repo="oxfordeo/oxeo-flows",
    path="flows/extract.py",
    access_token_secret="GITHUB",
)
run_config = VertexRun(
    labels=["vertex"],
    image="eu.gcr.io/oxeo-main/oxeo-flows:latest",
    machine_type="e2-highmem-2",
)
with Flow(
    "extract",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # parameters
    aoi = Parameter(name="aoi", required=True)

    credentials = Parameter(name="credentials", default="token.json")
    project = Parameter(name="project", default="oxeo-main")
    gcp_region = Parameter(name="gcp_region", default="europe-west4")
    user_id = Parameter(name="user_id", default="oxeo")
    storage_root = Parameter(name="storage_root", default="oxeo-water")

    start_date = Parameter(name="start_date", default="2020-01-01")
    end_date = Parameter(name="end_date", default="2020-02-01")
    constellations = Parameter(
        name="constellations",
        default=["sentinel-2", "landsat-5", "landsat-7", "landsat-8"],
    )

    bbox_size = Parameter(name="bbox_size", default=10000)
    split_m = Parameter(name="split_m", default=100000)
    chunk_size = Parameter(name="chunk_size", default=1000)

    # figure out geom and ID
    aoi_id, aoi_geom = get_id_and_geom(aoi)

    # rename the Flow run to reflect the parameters
    rename_flow_run(aoi_id)

    # run the flow
    storage_path = get_storage_path(storage_root, aoi_id)
    built = build(project, gcp_region, storage_root, credentials, user_id)
    item_collection = stac(credentials, start_date, end_date, constellations, aoi_geom)
    tiles = tiler(bbox_size, aoi_geom)
    extraction_tasks = scheduler(constellations, tiles, item_collection, split_m)
    prepped = preparer(
        credentials,
        constellations,
        storage_path,
        bbox_size,
        chunk_size,
        tiles,
        extraction_tasks,
    )
    job_id = deployer(
        project,
        user_id,
        credentials,
        storage_path,
        chunk_size,
        extraction_tasks,
        upstream_tasks=[built, prepped],
    )

    check_deploy_completion(project, user_id, job_id, extraction_tasks)
