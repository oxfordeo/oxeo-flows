import time
from datetime import datetime
from pathlib import Path

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
from prefect.utilities.notifications import slack_notifier
from satextractor.deployer import deploy_tasks
from satextractor.models import ExtractionTask, Tile
from satextractor.plugins import copy_mtl_files
from satextractor.preparer.gcp_preparer import gcp_prepare_archive
from satextractor.scheduler import create_tasks_by_splits
from satextractor.stac import gcp_region_to_item_collection
from satextractor.tiler import split_region_in_utm_tiles

import oxeo.flows.config as cfg
from oxeo.core.models.waterbody import WaterBody
from oxeo.flows.utils import (
    data2gdf_task,
    fetch_water_list_task,
    get_job_id_task,
    get_waterbodies_task,
    parse_constellations_task,
    parse_water_list_task,
)


@task(log_stdout=True)
def stac(
    waterbody: WaterBody,
    credentials: Path,
    start_date: str,
    end_date: str,
    constellations: str,
) -> pystac.ItemCollection:
    logger = prefect.context.get("logger")
    logger.info(f"Converting data to STAC for {waterbody.area_id=}")
    if start_date > end_date:
        raise ValueError("Start date must be before end date!")
    item_collection = gcp_region_to_item_collection(
        credentials=credentials,
        region=waterbody.geometry,
        start_date=start_date,
        end_date=end_date,
        constellations=constellations,
    )
    return item_collection


@task(log_stdout=True)
def tiler(
    waterbody: WaterBody,
    bbox_size: int,
) -> list[Tile]:
    logger = prefect.context.get("logger")
    logger.info(f"Creating tiles for {waterbody.area_id=}")
    tiles = split_region_in_utm_tiles(
        region=waterbody.geometry,
        bbox_size=bbox_size,
    )
    logger.warning(f"Got the following tiles: {[t.id for t in tiles]}")
    return tiles


@task(log_stdout=True)
def scheduler(
    item_collection: pystac.ItemCollection,
    tiles: list[Tile],
    waterbody: WaterBody,
    constellations: list[str],
    split_m: int,
    overwrite: bool,
    storage_path: str,
    project: str,
    credentials: str,
) -> list[ExtractionTask]:
    logger = prefect.context.get("logger")
    logger.info(f"Create extraction tasks for {waterbody.area_id=}")
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
    extraction_tasks: list[ExtractionTask],
    tiles: list[Tile],
    waterbody: WaterBody,
    credentials: Path,
    constellations: list[str],
    storage_path: str,
    bbox_size: int,
    chunk_size: int,
    overwrite: bool,
) -> None:
    logger = prefect.context.get("logger")
    logger.info(f"Prepare Cloud Storage bucket for {waterbody.area_id=}")
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
    extraction_tasks: list[ExtractionTask],
    waterbody: WaterBody,
    job_id: str,
    project: str,
    user_id: str,
    credentials: Path,
    storage_path: str,
    chunk_size: int,
) -> str:
    logger = prefect.context.get("logger")

    run_id = f"{job_id}_{waterbody.area_id}"
    logger.info(f"Deploy tasks to Cloud Run for {waterbody.area_id=} with {run_id=}")

    topic = f"projects/{project}/topics/{'-'.join([user_id, 'stacextractor'])}"
    deploy_tasks(
        job_id=run_id,
        credentials=credentials,
        extraction_tasks=extraction_tasks,
        storage_path=storage_path,
        chunk_size=chunk_size,
        topic=topic,
    )
    return run_id


@task(log_stdout=True)
def copy_metadata(
    extraction_tasks: list[ExtractionTask],
    waterbody: WaterBody,
    credentials: str,
    storage_path: str,
) -> None:
    logger = prefect.context.get("logger")
    logger.info(
        f"Copying MTL metadata files for any landsat data for {waterbody.area_id=}"
    )
    copy_mtl_files(credentials, extraction_tasks, storage_path)


@task(log_stdout=True, state_handlers=[slack_notifier])
def check_deploy_completion(
    run_ids: list[str],
    extraction_tasks: list[list[ExtractionTask]],
    project: str,
    user_id: str,
) -> bool:
    logger = prefect.context.get("logger")
    tot = sum(len(ext) for ext in extraction_tasks)

    run_ids = ",".join(f"'{r}'" for r in run_ids)

    prev_done = -1
    loops_unchanged = 0
    max_loops_no_progress = 3
    base_sleep = 60

    while True:
        client = bigquery.Client()
        query = f"""
        SELECT COUNT(msg_type)
        FROM {project}.satextractor.{user_id}
        WHERE job_id IN ({run_ids})
        AND msg_type = 'FINISHED'
        """
        df = client.query(query).result().to_dataframe()
        done = int(df.iat[0, 0])
        logger.info(f"Cloud Run extraction tasks: {done} of {tot}")

        if done >= tot:
            logger.info(f"All {done} of {tot} extraction tasks done!")
            return True
        elif done > prev_done:
            loops_unchanged = 0
        else:
            loops_unchanged += 1
            if loops_unchanged > max_loops_no_progress:
                logger.info(f"No progress on tasks after {loops_unchanged} loops")
                raise Exception("No further progress on tasks")
        prev_done = done

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

    if len(extraction_tasks) == 0:
        logger.warning("No extraction tasks, not logging anything to BQ")
        return

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


def create_flow():
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
        user_id = Parameter(name="user_id", default="oxeo")
        root_dir = Parameter(name="root_dir", default="gs://oxeo-water/prod")

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

        # parse inputs
        constellations = parse_constellations_task(constellations)
        water_list = parse_water_list_task(water_list, postgis_password)
        job_id = get_job_id_task()

        # get geom
        db_data = fetch_water_list_task(
            water_list=water_list, password=postgis_password
        )
        gdf = data2gdf_task(db_data)
        waterbodies = get_waterbodies_task(gdf, constellations)

        # run the flow
        storage_path = root_dir
        item_collection = stac.map(
            waterbody=waterbodies,
            credentials=unmapped(credentials),
            start_date=unmapped(start_date),
            end_date=unmapped(end_date),
            constellations=unmapped(constellations),
        )
        tiles: list[list[Tile]] = tiler.map(
            waterbody=waterbodies,
            bbox_size=unmapped(bbox_size),
        )
        extraction_tasks: list[list[ExtractionTask]] = scheduler.map(
            item_collection=item_collection,
            tiles=tiles,
            waterbody=waterbodies,
            constellations=unmapped(constellations),
            split_m=unmapped(split_m),
            overwrite=unmapped(overwrite),
            storage_path=unmapped(storage_path),
            project=unmapped(project),
            credentials=unmapped(credentials),
        )
        prepped = preparer.map(
            extraction_tasks=extraction_tasks,
            tiles=tiles,
            waterbody=waterbodies,
            credentials=unmapped(credentials),
            constellations=unmapped(constellations),
            storage_path=unmapped(storage_path),
            bbox_size=unmapped(bbox_size),
            chunk_size=unmapped(chunk_size),
            overwrite=unmapped(overwrite),
        )
        run_ids = deployer.map(
            extraction_tasks=extraction_tasks,
            waterbody=waterbodies,
            job_id=unmapped(job_id),
            project=unmapped(project),
            user_id=unmapped(user_id),
            credentials=unmapped(credentials),
            storage_path=unmapped(storage_path),
            chunk_size=unmapped(chunk_size),
            upstream_tasks=[prepped],
        )
        copy_metadata.map(
            extraction_tasks,
            waterbody=waterbodies,
            credentials=unmapped(credentials),
            storage_path=unmapped(storage_path),
        )

        complete = check_deploy_completion(
            run_ids=run_ids,
            extraction_tasks=extraction_tasks,
            project=project,
            user_id=user_id,
        )

        log_to_bq.map(
            waterbody=waterbodies,
            extraction_tasks=extraction_tasks,
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
    return flow


flow = create_flow()
