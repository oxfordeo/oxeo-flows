import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List, Union
from uuid import uuid4

import prefect
import pystac
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from satextractor.deployer import deploy_tasks
from satextractor.models import ExtractionTask, Tile
from satextractor.preparer.gcp_preparer import gcp_prepare_archive
from satextractor.scheduler import create_tasks_by_splits
from satextractor.stac import gcp_region_to_item_collection
from satextractor.tiler import split_region_in_utm_tiles
from shapely.geometry import MultiPolygon, Polygon

from oxeo.flows import (
    default_gcp_token,
    docker_oxeo_flows,
    prefect_secret_github_token,
    repo_name,
)
from oxeo.flows.utils import (
    WaterDict,
    data2gdf,
    fetch_water_list,
    gdf2geom,
    generate_run_id,
    get_water_dicts,
    parse_water_list,
    rename_flow_run,
)


@task
def get_storage_path(
    storage_root: str,
) -> str:
    return f"gs://{storage_root}/prod"


@task
def build(
    project: str,
    gcp_region: str,
    storage_root: str,
    credentials: Path,
    user_id: str,
) -> bool:
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
        bbox_size=bbox_size,
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
) -> bool:
    logger = prefect.context.get("logger")
    tot = len(extraction_tasks)

    prev_done = -1
    loops_unchanged = 0
    max_loops_no_progress = 5
    base_sleep = 60

    while True:
        client = bigquery.Client()
        table = f"{project}.satextractor.{user_id}"
        query = f"SELECT COUNT(msg_type) FROM {table} WHERE job_id = '{job_id}' AND msg_type = 'FINISHED'"
        df = client.query(query).result().to_dataframe()
        done = int(df.iat[0, 0])
        logger.info(f"Cloud Run extraction tasks: {done} of {tot}")

        if done >= tot:
            logger.info(f"All {tot} extraction tasks done!")
            return True
        elif done == prev_done:
            loops_unchanged += 1
            if loops_unchanged >= max_loops_no_progress:
                logger.info(f"No progress on tasks after {loops_unchanged} loops")
                raise Exception("No further progress on tasks")
        else:
            loops_unchanged = 0

        # TODO This isn't working
        # Just sleeps for 10 seconds every time and never quits
        # But Cloud Run has resource limits so maybe don't want it to quit?

        # Exponential backoff, only when nothing has changed
        sleep = base_sleep * (2 ** (loops_unchanged + 1) - 1)
        logger.info(f"Sleep for {sleep} seconds")
        time.sleep(sleep)


@task
def log_to_bq(
    water_dict: WaterDict,
    start_date: str,
    end_date: str,
    constellations: List[str],
    bbox_size: int,
    split_m: int,
    chunk_size: int,
) -> None:
    logger = prefect.context.get("logger")

    area_id = water_dict.area_id
    run_id = f"{area_id}-{str(uuid4())[:8]}"
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    minx, miny, maxx, maxy = water_dict.geometry.bounds

    logger.info(f"Log lake {area_id} extraction to BQ")

    tiles = [p.tile.id for p in water_dict.paths]
    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        timestamp=timestamp,
        start_date=start_date,
        end_date=end_date,
        constellations=constellations,
        bbox_size=bbox_size,
        split_m=split_m,
        chunk_size=chunk_size,
        tiles=tiles,
        bbox_n=maxy,
        bbox_s=miny,
        bbox_w=minx,
        bbox_e=maxx,
    )

    logger.info("Insert results into BigQuery")
    client = bigquery.Client()

    errors = client.insert_rows_json("oxeo-main.water.water_extractions", [dict_water])
    if errors != []:
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )


executor = DaskExecutor()
storage = GitHub(
    repo=repo_name,
    path="oxeo/flows/extract.py",
    access_token_secret=prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=docker_oxeo_flows,
)
with Flow(
    "extract",
    executor=executor,
    storage=storage,
    run_config=run_config,
) as flow:
    # secrets
    postgis_password = PrefectSecret("POSTGIS_PASSWORD")

    # parameters
    water_list = Parameter(
        name="water_list", required=True, default=[25906112, 25906127]
    )

    credentials = Parameter(name="credentials", default=default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    gcp_region = Parameter(name="gcp_region", default="europe-west4")
    user_id = Parameter(name="user_id", default="oxeo")
    bucket = Parameter(name="storage_root", default="oxeo-water")

    start_date = Parameter(name="start_date", default="2020-01-01")
    end_date = Parameter(name="end_date", default="2020-02-01")
    constellations = Parameter(name="constellations", default=["sentinel-2"])

    bbox_size = Parameter(name="bbox_size", default=10000)
    split_m = Parameter(name="split_m", default=100000)
    chunk_size = Parameter(name="chunk_size", default=1000)

    # rename the Flow run to reflect the parameters
    water_list = parse_water_list(water_list)
    run_id = generate_run_id(water_list)
    rename_flow_run(run_id)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)
    aoi_geom = gdf2geom(gdf)

    # run the flow
    storage_path = get_storage_path(bucket)
    built = build(project, gcp_region, bucket, credentials, user_id)
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

    complete = check_deploy_completion(project, user_id, job_id, extraction_tasks)

    water_dicts = get_water_dicts(gdf, bucket, constellations)
    log_to_bq.map(
        water_dict=water_dicts,
        start_date=unmapped(start_date),
        end_date=unmapped(end_date),
        constellations=unmapped(constellations),
        bbox_size=unmapped(bbox_size),
        split_m=unmapped(split_m),
        chunk_size=unmapped(chunk_size),
        upstream_tasks=[unmapped(complete)],
    )
