from datetime import datetime
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

from oxeo.flows import (
    dask_gcp_zone,
    dask_image,
    dask_network,
    dask_projectid,
    default_gcp_token,
    docker_oxeo_flows,
    prefect_secret_github_token,
    repo_name,
)
from oxeo.flows.utils import (
    TilePath,
    WaterDict,
    data2gdf,
    fetch_water_list,
    generate_run_id,
    get_all_paths,
    get_water_dicts,
    parse_water_list,
    rename_flow_run,
)
from oxeo.water.metrics import metrics
from oxeo.water.models import model_factory
from oxeo.water.models.utils import merge_masks


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

    mapper = fs.get_mapper(f"{path.path}/data")
    arr = zarr.open(mapper, "r")

    masks = predictor.predict(
        arr,
        bands_indexes={
            "red": 3,
            "green": 2,
            "blue": 1,
            "nir": 7,
            "swir1": 11,
            "swir2": 12,
        },
        compute=False,
    )
    masks = np.array(masks)

    mask_mapper = fs.get_mapper(f"{path.path}/{model_name}")
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
def merge_to_bq(
    water_dict: WaterDict,
    model_name: str,
    pfaf2: int = 12,
) -> None:
    logger = prefect.context.get("logger")
    # TODO Fix this
    # oxeo-water merge_masks wants constellation as a parameter
    # but we should merge all constellations together?
    # Removed last bit of paths as parse_xy in model/utils expects
    # a different path structure (without contellation)
    paths = [p.path.split("/sentinel")[0] for p in water_dict.paths]
    tiles = [p.tile.id for p in water_dict.paths]

    logger.info(f"Merge all masks in {paths}")
    full_mask, dates = merge_masks(
        paths,
        patch_size=1000,
        data=model_name,
    )
    areas = metrics.segmentation_area(full_mask)

    area_id = water_dict.area_id
    run_id = f"{area_id}-{model_name}-{str(uuid4())[:8]}"

    logger.info("Convert results to DataFrame and dict ")
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    df_ts = pd.DataFrame(data={"date": dates, "area": areas}).assign(
        area_id=area_id,
        run_id=run_id,
        pfaf2=pfaf2,
    )
    df_ts.date = df_ts.date.apply(lambda x: x.date())  # remove time

    minx, miny, maxx, maxy = water_dict.geometry.bounds

    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        model=model_name,
        timestamp=timestamp,
        tiles=tiles,
        bbox_n=maxy,
        bbox_s=miny,
        bbox_w=minx,
        bbox_e=maxx,
    )

    logger.info("Insert results into BigQuery")
    client = bigquery.Client()

    table = client.get_table("oxeo-main.water.water_ts")
    errors = client.insert_rows_from_dataframe(table, df_ts)
    if errors != [[]]:
        raise ValueError(
            f"there where {len(errors)} error when inserting. " + str(errors),
        )

    errors = client.insert_rows_json("oxeo-main.water.water_model_runs", [dict_water])
    if errors != []:
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
        "projectid": dask_projectid,
        "zone": dask_gcp_zone,
        "network": dask_network,
        # "machine_type": "n2-standard-16",
        "source_image": dask_image,
        "docker_image": docker_oxeo_flows,
        "bootstrap": False,
        # "n_workers": 2,
    },
)
storage = GitHub(
    repo=repo_name,
    path="oxeo/flows/predict.py",
    access_token_secret=prefect_secret_github_token,
)
run_config = KubernetesRun(
    image=docker_oxeo_flows,
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

    credentials = Parameter(name="credentials", default=default_gcp_token)
    project = Parameter(name="project", default="oxeo-main")
    bucket = Parameter(name="bucket", default="oxeo-water")

    constellations = Parameter(name="constellations", default=["sentinel-2"])

    # rename the Flow run to reflect the parameters
    water_list = parse_water_list(water_list)
    run_id = generate_run_id(water_list)
    rename_flow_run(run_id)

    # get geom
    db_data = fetch_water_list(water_list=water_list, password=postgis_password)
    gdf = data2gdf(db_data)

    # start processing
    all_paths = get_all_paths(gdf, bucket, constellations)

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
    water_dicts = get_water_dicts(gdf, bucket, constellations)
    merge_to_bq.map(
        water_dict=water_dicts,
        model_name=unmapped(model_name),
        upstream_tasks=[unmapped(masks)],
    )
