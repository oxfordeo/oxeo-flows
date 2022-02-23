from datetime import datetime

import gcsfs
import numpy as np
import pandas as pd
import prefect
import zarr
from dask_kubernetes import KubeCluster, make_pod_spec
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from zarr.errors import ArrayNotFoundError

import oxeo.flows.config as cfg
from oxeo.flows.utils import (
    data2gdf,
    fetch_water_list,
    get_all_paths,
    get_job_id,
    get_waterbodies,
    parse_constellations,
    parse_water_list,
)
from oxeo.water.metrics import metrics
from oxeo.water.models import model_factory
from oxeo.water.models.utils import TilePath, WaterBody, merge_masks_all_constellations


@task(log_stdout=True)
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

    gpu = prefect.context.parameters["gpu_per_worker"]
    if gpu > 0:
        import torch

        cuda = torch.cuda.is_available()
        logger.info(f"CUDA available: {cuda}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    predictor = model_factory(model_name).predictor(
        ckpt_path=ckpt_path,
        fs=fs,
        batch_size=cnn_batch_size,
        bands=bands,
        target_size=target_size,
    )

    # get revisits shape from timestamps
    timestamps = zarr.open(fs.get_mapper(path.timestamps_path), "r")[:]
    timestamps = np.array(
        [np.datetime64(datetime.fromisoformat(el)) for el in timestamps],
    )

    sdt = np.datetime64(datetime.strptime(start_date, "%Y-%m-%d"))
    edt = np.datetime64(datetime.strptime(end_date, "%Y-%m-%d"))

    date_overlap = np.where((timestamps >= sdt) & (timestamps <= edt))[0]
    min_idx = date_overlap.min()
    max_idx = date_overlap.max() + 1
    logger.info(f"From overlap with imagery and dates entered: {min_idx=}, {max_idx=}")

    mask_path = f"{path.mask_path}/{model_name}"
    mask_mapper = fs.get_mapper(mask_path)

    if not overwrite:
        # check existing masks and only do new ones
        try:
            mask_arr = zarr.open_array(mask_mapper, "r")
            prev_max_idx = int(mask_arr.attrs["max_filled"])
            min_idx = prev_max_idx + 1
            logger.warning(f"Found {prev_max_idx=}, set {min_idx=}")
        except ArrayNotFoundError:
            logger.warning("Set overwrite=False, but there was no existing array")
        except KeyError:
            logger.warning(
                "Set overwrite=False, but attrs['max_filled'] had not been set"
            )

    mask_list = []
    for i in range(min_idx, max_idx, revisit_chunk_size):
        logger.info(
            f"creating mask for {path.path}, revisits {i} to {min(i + revisit_chunk_size,max_idx)} of {max_idx}"
        )
        revisit_masks = predictor.predict(
            path,
            revisit=slice(i, min(i + revisit_chunk_size, max_idx)),
        )
        mask_list.append(revisit_masks)
    masks = np.vstack(mask_list)

    # open as 'append' -> create if doesn't exist
    time_shape = timestamps.shape[0]
    geo_shape = masks.shape[1:]
    output_shape = (time_shape, *geo_shape)
    logger.info(f"Saving mask to {mask_path}")
    logger.info(f"Output zarr shape: {output_shape}")

    mask_arr = zarr.open_array(
        mask_mapper,
        "a",
        shape=output_shape,
        chunks=(1, 1000, 1000),
        dtype=np.uint8,
    )
    mask_arr.resize(*output_shape)
    mask_arr.attrs["max_filled"] = int(max_idx)

    # write data to archive
    mask_arr[min_idx:max_idx, ...] = masks
    logger.info(f"Successfully created masks for {path.path} on: {task_full_name}")

    written_start = np.datetime_as_string(timestamps[min_idx], unit="D")
    written_end = np.datetime_as_string(timestamps[max_idx], unit="D")

    return written_start, written_end


@task
def minmax_written_dates(
    written_dates: list[tuple[str, str]],
) -> tuple[str, str]:
    written_start = min(w[0] for w in written_dates)
    written_end = max(w[1] for w in written_dates)
    return written_start, written_end


@task(log_stdout=True)
def merge_to_timeseries(
    waterbody: WaterBody,
    mask: str,
    label: int,
) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info(f"Merge all masks in {waterbody.paths}")
    timeseries_masks = merge_masks_all_constellations(
        waterbody=waterbody,
        mask=mask,
    )
    df = metrics.segmentation_area_multiple(timeseries_masks, waterbody, label)
    df.date = df.date.apply(lambda x: x.date())  # remove time component

    return df


@task(log_stdout=True)
def log_to_bq(
    df: pd.DataFrame,
    waterbody: WaterBody,
    job_id: str,
    overwrite: bool,
    start_date: str,
    end_date: str,
    written_start: str,
    written_end: str,
    model_name: str,
    ckpt_path: str,
    constellations: list[str],
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")
    tiles = list({p.tile.id for p in waterbody.paths})

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

    # TODO: Always use GPU image
    image = cfg.docker_oxeo_flows_gpu if gpu > 0 else cfg.docker_oxeo_flows

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
    # adapt_kwargs={"minimum": 2, "maximum": 100},
    cluster_kwargs={},
)
storage = GitHub(
    repo=cfg.repo_name,
    path="oxeo/flows/predict.py",
    access_token_secret=cfg.prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=cfg.docker_oxeo_flows,
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
    bucket = Parameter(name="bucket", default="oxeo-water")
    root_dir = Parameter(name="root_dir", default="prod")

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

    cnn_batch_size = Parameter(name="cnn_batch_size", default=64)
    revisit_chunk_size = Parameter(name="revisit_chunk_size", default=8)

    # rename the Flow run to reflect the parameters
    constellations = parse_constellations(constellations)
    water_list = parse_water_list(water_list, postgis_password)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)

    # start processing
    all_paths = get_all_paths(gdf, bucket, constellations, root_dir)

    # create_masks() is mapped in parallel across all the paths
    # the returned masks is an empty list purely for the DAG
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

    written_start, written_end = minmax_written_dates(written_dates)

    # now instead of mapping across all paths, we map across
    # individual lakes
    waterbodies = get_waterbodies(gdf, bucket, constellations, root_dir)
    ts_dfs = merge_to_timeseries.map(
        waterbody=waterbodies,
        mask=unmapped(model_name),
        label=unmapped(timeseries_label),
        upstream_tasks=[unmapped(written_dates)],
    )
    job_id = get_job_id()
    log_to_bq.map(
        df=ts_dfs,
        waterbody=waterbodies,
        job_id=unmapped(job_id),
        overwrite=unmapped(overwrite),
        start_date=unmapped(start_date),
        end_date=unmapped(end_date),
        written_start=unmapped(written_start),
        written_end=unmapped(written_end),
        model_name=unmapped(model_name),
        ckpt_path=unmapped(ckpt_path),
        constellations=unmapped(constellations),
    )
