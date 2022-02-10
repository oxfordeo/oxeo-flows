from datetime import datetime
from typing import List

import gcsfs
import numpy as np
import prefect
import zarr
from prefect import Flow, Parameter, task, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret

import oxeo.flows.config as cfg
from oxeo.flows.utils import (
    data2gdf,
    fetch_water_list,
    generate_run_id,
    get_all_paths,
    parse_constellations,
    parse_water_list,
    rename_flow_run,
)
from oxeo.water.models import model_factory
from oxeo.water.models.utils import TilePath


@task
def log_paths(all_paths):
    logger = prefect.context.get("logger")
    logger.warning(f"{all_paths=}")


@task(log_stdout=True)
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

    import torch

    cuda = torch.cuda.is_available()
    num_gpus = torch.cuda.device_count()
    logger.info(f"CUDA available: {cuda=}, {num_gpus=}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    predictor = model_factory(model_name).predictor(
        ckpt_path=ckpt_path,
        fs=fs,
        batch_size=cnn_batch_size,
        bands=bands,
        target_size=target_size,
    )
    logger.info("Got predictor")

    # get revisits shape from timestamps
    timestamps = zarr.open(fs.get_mapper(path.timestamps_path), "r")[:]
    timestamps = np.array(
        [np.datetime64(datetime.fromisoformat(el)) for el in timestamps],
    )

    sdt = datetime.strptime(start_date, "%Y-%m-%d")
    edt = datetime.strptime(end_date, "%Y-%m-%d")
    logger.info("Got timestamps")

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


env = {"PREFECT__LOGGING__EXTRA_LOGGERS": '["oxeo.water"]'}

job_template = """
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
        - name: flow
          resources:
            requests:
              cpu: "15"
              memory: "55G"
              nvidia.com/gpu: 1
            limits:
              cpu: "15"
              memory: "55G"
              nvidia.com/gpu: 1
"""

storage = GitHub(
    repo=cfg.repo_name,
    path="oxeo/flows/predict-no-dask.py",
    access_token_secret=cfg.prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=cfg.docker_oxeo_flows_gpu,
    env=env,
    job_template=job_template,
)
with Flow(
    "predict-no-dask",
    storage=storage,
    run_config=run_config,
) as flow:
    # secrets
    postgis_password = PrefectSecret("POSTGIS_PASSWORD")

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

    _ = log_paths(all_paths)

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
