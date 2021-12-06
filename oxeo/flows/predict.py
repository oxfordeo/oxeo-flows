from datetime import datetime
from typing import List
from uuid import uuid4

import gcsfs
import numpy as np
import pandas as pd
import prefect
import zarr
from dask_cloudprovider.gcp import GCPCluster
from google.cloud import bigquery
from prefect import Flow, Parameter, task, unmapped
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from zarr.errors import PathNotFoundError

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
) -> None:
    logger = prefect.context.get("logger")
    task_full_name = prefect.context.get("task_full_name")
    logger.info(f"Creating mask for {path.path} on: {task_full_name}")

    fs = gcsfs.GCSFileSystem(project=project, token=credentials)
    predictor = model_factory(model_name).predictor()

    try:
        data_path = f"{path.path}/data"
        logger.info(f"Getting arr from {data_path=}")
        mapper = fs.get_mapper(data_path)
        constellation = path.constellation
        arr = zarr.open(mapper, "r")
    except (Exception, PathNotFoundError) as e:
        logger.warning(f"Couldn't load zarr at {data_path=} error {e}, ignoring")
        return

    masks = predictor.predict(
        arr,
        constellation=constellation,
        compute=False,
    )
    masks = np.array(masks)

    mask_path = f"{path.path}/mask/{model_name}"
    logger.info(f"Saving mask to {mask_path}")
    mask_mapper = fs.get_mapper(mask_path)
    mask_arr = zarr.open_array(
        mask_mapper,
        "w",
        shape=masks.shape,
        chunks=(1, 1000, 1000),
        dtype=np.uint8,
    )
    mask_arr[:] = masks
    logger.info(f"Successfully created masks for {path.path} on: {task_full_name}")
    return


@task
def merge_to_timeseries(
    waterbody: WaterBody,
    model_name: str,
) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    # TODO Fix this
    # oxeo-water merge_masks wants constellation as a parameter
    # but we should merge all constellations together?
    # Removed last bit of paths as parse_xy in model/utils expects
    # a different path structure (without contellation)

    logger.info(f"Merge all masks in {waterbody.paths}")
    timeseries_masks = merge_masks_all_constellations(
        waterbody=waterbody,
        model_name=model_name,
    )
    df = metrics.segmentation_area_multiple(timeseries_masks, waterbody)
    df.date = df.date.apply(lambda x: x.date())  # remove time component

    return df


@task
def log_to_bq(
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
    run_id = f"{area_id}-{model_name}-{str(uuid4())[:8]}"
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
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
    n_workers = prefect.context.parameters["n_workers"]
    machine_type = prefect.context.parameters["machine_type"]
    return GCPCluster(n_workers=n_workers, machine_type=machine_type, **kwargs)


executor = DaskExecutor(
    cluster_class=dynamic_cluster,
    debug=True,
    # adapt_kwargs={"minimum": 2, "maximum": 30},
    cluster_kwargs={
        "projectid": cfg.dask_projectid,
        "zone": cfg.dask_gcp_zone,
        # "machine_type": "n2-standard-16",
        "source_image": cfg.dask_image,
        "docker_image": cfg.docker_oxeo_flows,
        "bootstrap": False,
        # "n_workers": 2,
    },
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
    flow.add_task(Parameter("machine_type", default="n2-standard-16"))

    water_list = Parameter(name="water_list", default=[25906112, 25906127])
    model_name = Parameter(name="model_name", default="pekel")

    credentials = Parameter(name="credentials", default=cfg.default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    bucket = Parameter(name="bucket", default="oxeo-water")
    root_dir = Parameter(name="root_dir", default="prod")

    constellations = Parameter(name="constellations", default=["sentinel-2"])

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
    )

    # now instead of mapping across all paths, we map across
    # individual lakes
    waterbodies = get_waterbodies(gdf, bucket, constellations, root_dir)
    ts_dfs = merge_to_timeseries.map(
        waterbody=waterbodies,
        model_name=unmapped(model_name),
        upstream_tasks=[unmapped(masks)],
    )
    log_to_bq.map(
        df=ts_dfs,
        waterbody=waterbodies,
        model_name=unmapped(model_name),
        constellations=unmapped(constellations),
    )
