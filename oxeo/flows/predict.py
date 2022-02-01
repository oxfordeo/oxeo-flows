from datetime import datetime
from typing import List
from uuid import uuid4

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
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret

import oxeo.flows.config as cfg
from oxeo.flows.utils import (
    data2gdf,
    fetch_water_list,
    generate_run_id,
    get_all_paths,
    get_waterbodies,
    parse_constellations,
    parse_water_list,
    rename_flow_run,
)
from oxeo.water.metrics import metrics
from oxeo.water.models import model_factory
from oxeo.water.models.utils import TilePath, WaterBody, merge_masks_all_constellations


@task
def create_masks(
    path: TilePath,
    model_name: str,
    project: str,
    credentials: str,
    ckpt_path: str,
    target_size: int,
    bands: List[str],
    cnn_batch_size: int,
    revisit_chunk_size: int,
    start_date: str,
    end_date: str,
) -> None:
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

    sdt = datetime.strptime(start_date, "%Y-%m-%d")
    edt = datetime.strptime(end_date, "%Y-%m-%d")

    min_idx = np.where(
        (timestamps >= np.datetime64(sdt)) & (timestamps <= np.datetime64(edt))
    )[0].min()
    max_idx = (
        np.where(
            (timestamps >= np.datetime64(sdt)) & (timestamps <= np.datetime64(edt))
        )[0].max()
        + 1
    )

    masks = []
    for i in range(min_idx, max_idx, revisit_chunk_size):
        logger.info(
            f"creating mask for {path.path}, revisits {i} to {min(i + revisit_chunk_size,max_idx)}"
        )
        revisit_masks = predictor.predict(
            path,
            revisit=slice(i, min(i + revisit_chunk_size, max_idx)),
        )
        masks.append(revisit_masks)
    masks = np.vstack(masks)

    mask_path = f"{path.mask_path}/{model_name}"
    logger.info(f"Saving mask to {mask_path}")
    mask_mapper = fs.get_mapper(mask_path)

    # open as 'append' -> create if doesn't exist
    time_shape = timestamps.shape[0]
    geo_shape = masks.shape[1:]
    output_shape = (time_shape, *geo_shape)
    logger.info(f"Output zarr shape: {output_shape}")

    mask_arr = zarr.open_array(
        mask_mapper,
        "a",
        shape=output_shape,
        chunks=(1, 1000, 1000),
        dtype=np.uint8,
    )

    # write data to archive
    mask_arr[min_idx:max_idx, ...] = masks
    logger.info(f"Successfully created masks for {path.path} on: {task_full_name}")
    return


@task
def merge_to_timeseries(waterbody: WaterBody, mask: str, label: int) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    # TODO Fix this
    # oxeo-water merge_masks wants constellation as a parameter
    # but we should merge all constellations together?
    # Removed last bit of paths as parse_xy in model/utils expects
    # a different path structure (without contellation)

    logger.info(f"Merge all masks in {waterbody.paths}")
    timeseries_masks = merge_masks_all_constellations(
        waterbody=waterbody,
        mask=mask,
    )
    df = metrics.segmentation_area_multiple(timeseries_masks, waterbody, label)
    df.date = df.date.apply(lambda x: x.date())  # remove time component

    return df


@task
def log_to_bq(
    name: str,
    df: pd.DataFrame,
    waterbody: WaterBody,
    model_name: str,
    constellations: List[str],
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")
    tiles = list({p.tile.id for p in waterbody.paths})

    logger.info("Prepare ts dataframe and model_run dict")
    area_id = waterbody.area_id
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    run_id = f"{area_id}-{model_name}-{name}-{str(uuid4())[:8]}"
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


def dynamic_cluster(**kwargs):
    logger = prefect.context.get("logger")
    n_workers = prefect.context.parameters["n_workers"]
    memory = prefect.context.parameters["memory_per_worker"]
    cpu = prefect.context.parameters["cpu_per_worker"]
    gpu = prefect.context.parameters["gpu_per_worker"]
    if gpu > 0:
        # Even setting gpu: 0 breaks on Autopilot
        logger.warning("GPU is greater than 0 but is not supported by Autopilot.")
        image = cfg.docker_oxeo_flows_gpu
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
    else:
        image = cfg.docker_oxeo_flows
        container_config = {
            "resources": {
                "limits": {
                    "cpu": cpu,
                    "memory": memory,
                },
                "requests": {
                    "cpu": cpu,
                    "memory": memory,
                },
            }
        }
    pod_spec = make_pod_spec(
        image=image,
        extra_container_config=container_config,
    )
    pod_spec.spec.containers[0].args.append("--no-dashboard")
    return KubeCluster(n_workers=n_workers, pod_template=pod_spec, **kwargs)


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
    flow.add_task(Parameter("n_workers", default=2))
    flow.add_task(Parameter("memory_per_worker", default="32G"))
    flow.add_task(Parameter("cpu_per_worker", default=8))
    flow.add_task(Parameter("gpu_per_worker", default=0))

    water_list = Parameter(name="water_list", default=[25906112, 25906127])
    run_name = Parameter(name="run_name", default="noname")
    model_name = Parameter(name="model_name", default="pekel")

    credentials = Parameter(name="credentials", default=cfg.default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    bucket = Parameter(name="bucket", default="oxeo-water")
    root_dir = Parameter(name="root_dir", default="prod")
    start_date = Parameter(name="start_date", default="1984-01-01")
    end_date = Parameter(name="end_date", default="2100-02-01")

    constellations = Parameter(name="constellations", default=["sentinel-2"])
    ckpt_path = Parameter(name="cktp_path", default="gs://oxeo-models/semseg/last.ckpt")
    target_size = Parameter(name="target_size", default=1000)
    bands = Parameter(
        name="bands", default=["nir", "red", "green", "blue", "swir1", "swir2"]
    )
    timeseries_label = Parameter(name="timeseries_label", default=1)

    cnn_batch_size = Parameter(name="cnn_batch_size", default=16)
    revisit_chunk_size = Parameter(name="revisit_chunk_size", default=2)

    # rename the Flow run to reflect the parameters
    constellations = parse_constellations(constellations)
    water_list = parse_water_list(water_list)
    run_id = generate_run_id(water_list)
    rename_flow_run(run_id)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)

    # start processing
    all_paths = get_all_paths(gdf, bucket, constellations, root_dir)

    # create_masks() is mapped in parallel across all the paths
    # the returned masks is an empty list purely for the DAG
    masks = create_masks.map(
        path=all_paths,
        model_name=unmapped(model_name),
        project=unmapped(project),
        credentials=unmapped(credentials),
        ckpt_path=unmapped(ckpt_path),
        target_size=unmapped(target_size),
        bands=unmapped(bands),
        cnn_batch_size=unmapped(cnn_batch_size),
        revisit_chunk_size=unmapped(revisit_chunk_size),
        start_date=unmapped(start_date),
        end_date=unmapped(end_date),
    )

    # now instead of mapping across all paths, we map across
    # individual lakes
    waterbodies = get_waterbodies(gdf, bucket, constellations, root_dir)
    ts_dfs = merge_to_timeseries.map(
        waterbody=waterbodies,
        mask=unmapped(model_name),
        label=unmapped(timeseries_label),
        upstream_tasks=[unmapped(masks)],
    )
    log_to_bq.map(
        name=unmapped(run_name),
        df=ts_dfs,
        waterbody=waterbodies,
        model_name=unmapped(model_name),
        constellations=unmapped(constellations),
    )
