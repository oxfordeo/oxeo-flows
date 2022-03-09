from datetime import datetime

import gcsfs
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
from zarr.errors import PathNotFoundError

import oxeo.flows.config as cfg
from oxeo.core.models.tile import TilePath
from oxeo.flows.utils import (
    data2gdf_task,
    fetch_water_list_task,
    get_all_paths_task,
    get_job_id_task,
    parse_constellations_task,
    parse_water_list_task,
)
from oxeo.water.models.factory import model_factory
from oxeo.water.models.tile_utils import predict_tile


@task(log_stdout=True)
def create_masks(
    tile_path: TilePath,
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
    job_id: str,
) -> None:
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Creating mask for {tile_path.path} on: {task_full_name}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)

    predictor = model_factory(model_name).predictor(
        ckpt_path=ckpt_path,
        fs=fs,
        batch_size=cnn_batch_size,
        bands=bands,
        target_size=target_size,
    )

    gpu = prefect.context.parameters["gpu_per_worker"]
    try:
        written_start, written_end = predict_tile(
            tile_path,
            model_name,
            predictor,
            revisit_chunk_size,
            start_date,
            end_date,
            fs,
            overwrite=overwrite,
            gpu=gpu,
        )
    except PathNotFoundError as e:
        logger.error(f"Nothing found at {e}, did you run an extract for these tiles?")
        raise

    logger.info(f"Successfully created masks for {tile_path.path} on: {task_full_name}")

    log_to_bq(
        tile_id=tile_path.tile.id,
        constellation=tile_path.constellation,
        job_id=job_id,
        overwrite=overwrite,
        start_date=start_date,
        end_date=end_date,
        written_start=written_start,
        written_end=written_end,
        model_name=model_name,
        ckpt_path=ckpt_path,
    )


def log_to_bq(
    tile_id: str,
    job_id: str,
    overwrite: bool,
    start_date: str,
    end_date: str,
    written_start: str,
    written_end: str,
    model_name: str,
    ckpt_path: str,
    constellation: str,
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")

    logger.info("Prepare model_run dict")
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    run_id = f"{job_id}_{tile_id}"

    dict_water = dict(
        constellation=constellation,
        tile_id=tile_id,
        run_id=run_id,
        model=model_name,
        model_checkpoint=ckpt_path,
        overwrite=overwrite,
        start_date=start_date,
        end_date=end_date,
        written_start=written_start,
        written_end=written_end,
        timestamp=timestamp,
    )

    logger.info("Insert results into BigQuery")
    client = bigquery.Client()

    errors = client.insert_rows_json("oxeo-main.water.predict_runs", [dict_water])
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

    container_config = {"resources": {"limits": {}, "requests": {}}}
    if gpu > 0:
        logger.warning("Creating GPU cluster!")
        container_config["resources"]["limits"]["nvidia.com/gpu"] = gpu
        container_config["resources"]["requests"]["nvidia.com/gpu"] = gpu

    # TODO: Always use GPU image
    image = cfg.docker_oxeo_flows_gpu if gpu > 0 else cfg.docker_oxeo_flows

    pod_spec = make_pod_spec(
        image=image,
        extra_container_config=container_config,
        env=env,
        memory_request=memory,
        memory_limit=memory,
        cpu_request=cpu,
        cpu_limit=cpu,
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
        cnn_batch_size = Parameter(name="cnn_batch_size", default=32)
        revisit_chunk_size = Parameter(name="revisit_chunk_size", default=8)

        constellations = parse_constellations_task(constellations)
        water_list = parse_water_list_task(water_list, postgis_password)

        # get geom
        db_data = fetch_water_list_task(
            water_list=water_list, password=postgis_password
        )
        gdf = data2gdf_task(db_data)

        # start processing
        all_tile_paths = get_all_paths_task(gdf, constellations, root_dir)
        job_id = get_job_id_task()

        # create_masks() is mapped in parallel across all the paths
        # it automatically logs the successful completion of each tile
        create_masks.map(
            tile_path=all_tile_paths,
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
            job_id=unmapped(job_id),
        )

    return flow


flow = create_flow()
