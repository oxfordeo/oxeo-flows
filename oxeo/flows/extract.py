import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Union

import gcsfs
import prefect
import pystac
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from satextractor.deployer import deploy_tasks
from satextractor.models import ExtractionTask, Tile
from satextractor.plugins import copy_mtl_files
from satextractor.preparer.gcp_preparer import gcp_prepare_archive
from satextractor.scheduler import create_tasks_by_splits
from satextractor.stac import gcp_region_to_item_collection
from satextractor.tiler import split_region_in_utm_tiles
from shapely.geometry import MultiPolygon, Polygon

import oxeo.flows.config as cfg
from oxeo.flows.utils import (
    data2gdf,
    fetch_water_list,
    gdf2geom,
    get_job_id,
    get_waterbodies,
    parse_constellations,
    parse_water_list,
)
from oxeo.water.models.utils import WaterBody


@task(log_stdout=True)
def get_storage_path(
    bucket: str,
    root_dir: str,
) -> str:
    return f"gs://{bucket}/{root_dir}"


@task(log_stdout=True)
def build(
    project: str,
    gcp_region: str,
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


@task(log_stdout=True)
def stac(
    credentials: Path,
    start_date: str,
    end_date: str,
    constellations: str,
    region: Union[Polygon, MultiPolygon],
) -> pystac.ItemCollection:
    logger = prefect.context.get("logger")
    logger.info("Converting data to STAC")
    if start_date > end_date:
        raise ValueError("Start date must be before end date!")
    item_collection = gcp_region_to_item_collection(
        credentials=credentials,
        region=region,
        start_date=start_date,
        end_date=end_date,
        constellations=constellations,
    )
    return item_collection


@task(log_stdout=True)
def tiler(
    bbox_size: int,
    region: Union[Polygon, MultiPolygon],
) -> list[Tile]:
    logger = prefect.context.get("logger")
    logger.info("Creating tiles")
    tiles = split_region_in_utm_tiles(
        region=region,
        bbox_size=bbox_size,
    )
    logger.warning(f"Got the following tiles: {[t.id for t in tiles]}")
    return tiles


@task(log_stdout=True)
def scheduler(
    constellations: list[str],
    tiles: list[Tile],
    item_collection: pystac.ItemCollection,
    split_m: int,
    overwrite: bool,
    storage_path: str,
    project: str,
    credentials: str,
) -> list[ExtractionTask]:
    logger = prefect.context.get("logger")
    logger.info("Create extraction tasks")
    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    extraction_tasks = create_tasks_by_splits(
        tiles=tiles,
        split_m=split_m,
        item_collection=item_collection,
        constellations=constellations,
        bands=None,
        interval=1,
        n_jobs=-1,
        verbose=0,
        overwrite=overwrite,
        storage_path=storage_path,
        fs_mapper=fs.get_mapper,
    )
    return extraction_tasks


@task(log_stdout=True)
def preparer(
    credentials: Path,
    constellations: list[str],
    storage_path: str,
    bbox_size: int,
    chunk_size: int,
    tiles: list[Tile],
    extraction_tasks: list[ExtractionTask],
    overwrite: bool,
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
        overwrite=overwrite,
        chunk_size=chunk_size,
        n_jobs=-1,
        verbose=0,
    )


@task(log_stdout=True)
def deployer(
    job_id: str,
    project: str,
    user_id: str,
    credentials: Path,
    storage_path: str,
    chunk_size: int,
    extraction_tasks: list[ExtractionTask],
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Deploy tasks to Cloud RUn")

    topic = f"projects/{project}/topics/{'-'.join([user_id, 'stacextractor'])}"
    deploy_tasks(
        job_id=job_id,
        credentials=credentials,
        extraction_tasks=extraction_tasks,
        storage_path=storage_path,
        chunk_size=chunk_size,
        topic=topic,
    )


@task(log_stdout=True)
def copy_metadata(
    credentials: str,
    extraction_tasks: list[ExtractionTask],
    storage_path: str,
) -> None:
    logger = prefect.context.get("logger")
    logger.info("Copying MTL metadata files for any landsat data")
    copy_mtl_files(credentials, extraction_tasks, storage_path)


@task(log_stdout=True)
def check_deploy_completion(
    project: str,
    user_id: str,
    job_id: str,
    extraction_tasks: list[ExtractionTask],
) -> bool:
    logger = prefect.context.get("logger")
    tot = len(extraction_tasks)

    prev_done = -1
    loops_unchanged = 0
    max_loops_no_progress = 3
    base_sleep = 60

    while True:
        client = bigquery.Client()
        table = f"{project}.satextractor.{user_id}"
        query = f"SELECT COUNT(msg_type) FROM `{table}` WHERE job_id = '{job_id}' AND msg_type = 'FINISHED'"
        df = client.query(query).result().to_dataframe()
        done = int(df.iat[0, 0])
        logger.info(f"Cloud Run extraction tasks: {done} of {tot}")

        if done >= tot:
            logger.info(f"All {done} of {tot} extraction tasks done!")
            return True
        elif done >= prev_done:
            loops_unchanged = 0
        else:
            loops_unchanged += 1
            if loops_unchanged > max_loops_no_progress:
                logger.info(f"No progress on tasks after {loops_unchanged} loops")
                raise Exception("No further progress on tasks")

        # Exponential backoff, only when nothing has changed
        sleep = base_sleep * (2 ** (loops_unchanged + 1) - 1)
        logger.info(f"Sleep for {sleep} seconds")
        time.sleep(sleep)


@task(log_stdout=True)
def log_to_bq(
    waterbody: WaterBody,
    extraction_tasks: list[ExtractionTask],
    job_id: str,
    start_date: str,
    end_date: str,
    constellations: list[str],
    bbox_size: int,
    split_m: int,
    chunk_size: int,
    overwrite: bool,
) -> None:
    logger = prefect.context.get("logger")

    area_id = waterbody.area_id
    run_id = f"{job_id}_{area_id}"
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    minx, miny, maxx, maxy = waterbody.geometry.bounds

    written_start = min(e.sensing_time for e in extraction_tasks).date().isoformat()
    written_end = max(e.sensing_time for e in extraction_tasks).date().isoformat()

    logger.info(f"Log lake {area_id} extraction to BQ")

    tiles = list({p.tile.id for p in waterbody.paths})
    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        timestamp=timestamp,
        start_date=start_date,
        end_date=end_date,
        written_start_date=written_start,
        written_end_date=written_end,
        overwrite=overwrite,
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


clock_params = dict(
    water_list="chosen",
    start_date="1980-01-01",
    end_date="2100-01-01",
)
clock = CronClock("45 8 * * 1", parameter_defaults=clock_params)
schedule = Schedule(clocks=[clock])

executor = DaskExecutor()
storage = GitHub(
    repo=cfg.repo_name,
    path="oxeo/flows/extract.py",
    access_token_secret=cfg.prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=cfg.docker_oxeo_flows,
    cpu_request=8,
    memory_request="32Gi",
)
with Flow(
    "extract",
    executor=executor,
    storage=storage,
    run_config=run_config,
    schedule=schedule,
) as flow:
    # secrets
    postgis_password = PrefectSecret("POSTGIS_PASSWORD")

    # parameters
    water_list = Parameter(
        name="water_list", required=True, default=[25906112, 25906127]
    )

    credentials = Parameter(name="credentials", default=cfg.default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    gcp_region = Parameter(name="gcp_region", default="europe-west4")
    user_id = Parameter(name="user_id", default="oxeo")
    bucket = Parameter(name="bucket", default="oxeo-water")
    root_dir = Parameter(name="root_dir", default="prod")

    start_date = Parameter(name="start_date", default="2020-01-01")
    end_date = Parameter(name="end_date", default="2020-02-01")
    constellations = Parameter(
        name="constellations",
        default=["landsat-5", "landsat-7", "landsat-8", "sentinel-2"],
    )
    overwrite = Parameter(name="overwrite", default=False)

    bbox_size = Parameter(name="bbox_size", default=10000)
    split_m = Parameter(name="split_m", default=100000)
    chunk_size = Parameter(name="chunk_size", default=1000)

    # rename the Flow run to reflect the parameters
    constellations = parse_constellations(constellations)
    water_list = parse_water_list(water_list, postgis_password)
    job_id = get_job_id()
    # run_id = generate_run_id(water_list)
    # rename_flow_run(run_id)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)
    aoi_geom = gdf2geom(gdf)

    # run the flow
    storage_path = get_storage_path(bucket, root_dir)
    built = build(project, gcp_region, credentials, user_id)
    item_collection = stac(credentials, start_date, end_date, constellations, aoi_geom)
    tiles = tiler(bbox_size, aoi_geom)
    extraction_tasks = scheduler(
        constellations,
        tiles,
        item_collection,
        split_m,
        overwrite,
        storage_path,
        project,
        credentials,
    )
    prepped = preparer(
        credentials,
        constellations,
        storage_path,
        bbox_size,
        chunk_size,
        tiles,
        extraction_tasks,
        overwrite,
    )
    deployed = deployer(
        job_id,
        project,
        user_id,
        credentials,
        storage_path,
        chunk_size,
        extraction_tasks,
        upstream_tasks=[built, prepped],
    )
    copy_metadata(credentials, extraction_tasks, storage_path)

    complete = check_deploy_completion(
        project,
        user_id,
        job_id,
        extraction_tasks,
        upstream_tasks=[deployed],
    )

    waterbodies = get_waterbodies(gdf, bucket, constellations)
    log_to_bq.map(
        waterbody=waterbodies,
        extraction_tasks=unmapped(extraction_tasks),
        job_id=unmapped(job_id),
        start_date=unmapped(start_date),
        end_date=unmapped(end_date),
        constellations=unmapped(constellations),
        bbox_size=unmapped(bbox_size),
        split_m=unmapped(split_m),
        chunk_size=unmapped(chunk_size),
        overwrite=unmapped(overwrite),
        upstream_tasks=[unmapped(complete)],
    )
