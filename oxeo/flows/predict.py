from datetime import datetime

import gcsfs
import pandas as pd
import prefect
from dask_kubernetes import KubeCluster, make_pod_spec
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.notifications import slack_notifier

import oxeo.flows.config as cfg
from oxeo.core.models.tile import TilePath
from oxeo.core.models.timeseries import merge_masks_all_constellations
from oxeo.core.models.waterbody import WaterBody
from oxeo.flows.utils import (
    data2gdf_task,
    fetch_water_list_task,
    get_all_paths_task,
    get_job_id_task,
    get_waterbodies_task,
    parse_constellations_task,
    parse_water_list_task,
)
from oxeo.water.metrics import seg_area_all
from oxeo.water.models.factory import model_factory
from oxeo.water.models.tile_utils import predict_tile


@task(log_stdout=True, state_handlers=[slack_notifier])
def create_masks(
    path: TilePath,
    model_name: str,
    project: str,
    credentials: str,
    ckpt_path: str,
    target_size: int,
    bands: list[str],
    cnn_batch_size: int,
    revisit_chunk_size: int,
    start_date: str,
    end_date: str,
    overwrite: bool,
) -> tuple[str, str]:
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Creating mask for {path.path} on: {task_full_name}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)

    predictor = model_factory(model_name).predictor(
        ckpt_path=ckpt_path,
        fs=fs,
        batch_size=cnn_batch_size,
        bands=bands,
        target_size=target_size,
    )

    gpu = prefect.context.parameters["gpu_per_worker"]
    written_start, written_end = predict_tile(
        path,
        model_name,
        predictor,
        revisit_chunk_size,
        start_date,
        end_date,
        fs,
        overwrite=overwrite,
        gpu=gpu,
    )

    logger.info(f"Successfully created masks for {path.path} on: {task_full_name}")
    return written_start, written_end


@task(state_handlers=[slack_notifier])
def get_written_dates_per_waterbody(
    all_paths: list[TilePath],
    written_dates: list[tuple[str, str]],
    waterbodies: list[WaterBody],
) -> dict[int, tuple[str, str]]:
    """The mapping is keyed by area_id (int)."""

    logger = prefect.context.get("logger")
    logger.info("Getting the written_dates for each waterbody")
    waterbody_dates_mapping = {}
    all_tile_ids: list[str] = [tp.tile.id for tp in all_paths]
    for wb in waterbodies:
        wb_tile_ids: list[str] = [tp.tile.id for tp in wb.paths]
        wb_dates = [
            dates
            for tile_id, dates in zip(all_tile_ids, written_dates)
            if tile_id in wb_tile_ids
        ]
        start = min(w[0] for w in wb_dates)
        end = max(w[1] for w in wb_dates)
        waterbody_dates_mapping[wb.area_id] = (start, end)
    return waterbody_dates_mapping


@task(log_stdout=True, state_handlers=[slack_notifier])
def merge_to_timeseries(
    waterbody: WaterBody,
    written_dates_mapping: dict[int, tuple[str, str]],
    model_name: str,
    label: int,
) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info(
        f"Merge all masks in {[(tp.tile.id, tp.constellation) for tp in waterbody.paths]}"
    )
    timeseries_masks = merge_masks_all_constellations(
        waterbody=waterbody, mask=model_name
    )

    written_start, _ = written_dates_mapping[waterbody.area_id]
    logger.warning(f"Get seg area, starting at data {written_start=}")

    df = seg_area_all(timeseries_masks, waterbody, written_start, label)
    df.date = df.date.apply(lambda x: x.date())  # remove time component

    return df


@task(log_stdout=True, state_handlers=[slack_notifier])
def log_to_bq(
    df: pd.DataFrame,
    waterbody: WaterBody,
    job_id: str,
    overwrite: bool,
    start_date: str,
    end_date: str,
    written_dates_mapping: dict[int, tuple[str, str]],
    model_name: str,
    ckpt_path: str,
    constellations: list[str],
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")
    tiles = list({p.tile.id for p in waterbody.paths})

    written_start, written_end = written_dates_mapping[waterbody.area_id]
    logger.warning(f"Got {written_start=}, {written_end=} from mapping")

    logger.info("Prepare ts dataframe and model_run dict")
    area_id = waterbody.area_id
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    run_id = f"{job_id}_{area_id}"
    df = df.assign(
        area_id=area_id,
        run_id=run_id,
        pfaf2=pfaf2,
    )

    minx, miny, maxx, maxy = waterbody.geometry.bounds

    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        model=model_name,
        model_checkpoint=ckpt_path,
        overwrite=overwrite,
        start_date=start_date,
        end_date=end_date,
        written_start_date=written_start,
        written_end_date=written_end,
        timestamp=timestamp,
        tiles=tiles,
        constellations=constellations,
        bbox_n=maxy,
        bbox_s=miny,
        bbox_w=minx,
        bbox_e=maxx,
    )

    logger.info("Insert results into BigQuery")
    client = bigquery.Client()

    table = client.get_table("oxeo-main.water.water_ts")
    errors = client.insert_rows_from_dataframe(table, df)
    logger.info(f"Inserting DataFrame response: (empty is good) {errors}")
    if not all(len(l) == 0 for l in errors):
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )

    errors = client.insert_rows_json("oxeo-main.water.water_model_runs", [dict_water])
    logger.info(f"Inserting dict response: (empty is good) {errors}")
    if not len(errors) == 0:
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )


env = {"PREFECT__LOGGING__EXTRA_LOGGERS": '["oxeo.water"]'}


def dynamic_cluster(**kwargs):
    n_workers = prefect.context.parameters["n_workers"]
    memory = prefect.context.parameters["memory_per_worker"]
    cpu = prefect.context.parameters["cpu_per_worker"]
    gpu = prefect.context.parameters["gpu_per_worker"]

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

    image = cfg.docker_oxeo_flows_gpu

    pod_spec = make_pod_spec(
        image=image,
        extra_container_config=container_config,
        env=env,
        memory_limit=memory,
    )
    pod_spec.spec.containers[0].args.append("--no-dashboard")
    return KubeCluster(
        n_workers=n_workers,
        pod_template=pod_spec,
        scheduler_pod_template=make_pod_spec(image=image, env=env),
        **kwargs,
    )


def create_flow():
    clock_params = dict(
        water_list="chosen",
        constellations=["landsat-5", "landsat-7", "landsat-8", "sentinel-2"],
        start_date="1980-01-01",
        end_date="2100-01-01",
        gpu_per_worker=1,
        n_workers=2,
    )
    clock = CronClock("45 8 * * 2", parameter_defaults=clock_params)
    schedule = Schedule(clocks=[clock])

    executor = DaskExecutor(
        cluster_class=dynamic_cluster,
        adapt_kwargs={"maximum": 80},
        cluster_kwargs={},
    )
    storage = GitHub(
        repo=cfg.repo_name,
        path="oxeo/flows/predict.py",
        access_token_secret=cfg.prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=cfg.docker_oxeo_flows_gpu,
        env=env,
    )

    with Flow(
        "predict",
        executor=executor,
        storage=storage,
        run_config=run_config,
        schedule=schedule,
    ) as flow:
        # secrets
        postgis_password = PrefectSecret("POSTGIS_PASSWORD")

        # parameters
        flow.add_task(Parameter("n_workers", default=1))
        flow.add_task(Parameter("memory_per_worker", default="56G"))
        flow.add_task(Parameter("cpu_per_worker", default=14))
        flow.add_task(Parameter("gpu_per_worker", default=0))

        water_list = Parameter(name="water_list", default=[25906112, 25906127])
        model_name = Parameter(name="model_name", default="cnn")

        credentials = Parameter(name="credentials", default=cfg.default_gcp_token)
        project = Parameter(name="project", default="oxeo-main")
        root_dir = Parameter(name="root_dir", default="gs://oxeo-water/prod")

        overwrite = Parameter(name="overwrite", default=False)
        start_date = Parameter(name="start_date", default="1984-01-01")
        end_date = Parameter(name="end_date", default="2100-02-01")

        constellations = Parameter(name="constellations", default=["sentinel-2"])
        ckpt_path = Parameter(
            name="cktp_path", default="gs://oxeo-models/semseg/epoch_012.ckpt"
        )
        target_size = Parameter(name="target_size", default=1000)
        bands = Parameter(
            name="bands", default=["nir", "red", "green", "blue", "swir1", "swir2"]
        )
        timeseries_label = Parameter(name="timeseries_label", default=1)

        cnn_batch_size = Parameter(name="cnn_batch_size", default=32)
        revisit_chunk_size = Parameter(name="revisit_chunk_size", default=8)

        # rename the Flow run to reflect the parameters
        constellations = parse_constellations_task(constellations)
        water_list = parse_water_list_task(water_list, postgis_password)

        # get geom
        db_data = fetch_water_list_task(
            water_list=water_list, password=postgis_password
        )
        gdf = data2gdf_task(db_data)

        # start processing
        all_paths = get_all_paths_task(gdf, constellations, root_dir)

        # create_masks() is mapped in parallel across all the paths
        written_dates = create_masks.map(
            path=all_paths,
            model_name=unmapped(model_name),
            project=unmapped(project),
            credentials=unmapped(credentials),
            ckpt_path=unmapped(ckpt_path),
            target_size=unmapped(target_size),
            bands=unmapped(bands),
            cnn_batch_size=unmapped(cnn_batch_size),
            revisit_chunk_size=unmapped(revisit_chunk_size),
            overwrite=unmapped(overwrite),
            start_date=unmapped(start_date),
            end_date=unmapped(end_date),
        )

        # now instead of mapping across all paths, we map across
        # individual lakes
        waterbodies = get_waterbodies_task(gdf, constellations, root_dir)
        written_dates_mapping = get_written_dates_per_waterbody(
            all_paths=all_paths,
            written_dates=written_dates,
            waterbodies=waterbodies,
        )
        ts_dfs = merge_to_timeseries.map(
            waterbody=waterbodies,
            written_dates_mapping=unmapped(written_dates_mapping),
            model_name=unmapped(model_name),
            label=unmapped(timeseries_label),
        )
        job_id = get_job_id_task()
        log_to_bq.map(
            df=ts_dfs,
            waterbody=waterbodies,
            job_id=unmapped(job_id),
            overwrite=unmapped(overwrite),
            start_date=unmapped(start_date),
            end_date=unmapped(end_date),
            written_dates_mapping=unmapped(written_dates_mapping),
            model_name=unmapped(model_name),
            ckpt_path=unmapped(ckpt_path),
            constellations=unmapped(constellations),
        )
    return flow


flow = create_flow()
