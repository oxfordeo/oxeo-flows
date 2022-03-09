from datetime import datetime

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

import oxeo.flows.config as cfg
from oxeo.core.models.timeseries import merge_masks_all_constellations
from oxeo.core.models.waterbody import WaterBody
from oxeo.flows.utils import (
    data2gdf_task,
    fetch_water_list_task,
    get_job_id_task,
    get_waterbodies_task,
    parse_constellations_task,
    parse_water_list_task,
)
from oxeo.water.metrics import seg_area_all


@task(log_stdout=True)
def merge_to_timeseries(
    waterbody: WaterBody,
    start_date: str,
    model_name: str,
    label: int,
    overwrite: bool,
    job_id: str,
    constellations: list[str],
) -> None:
    logger = prefect.context.get("logger")
    logger.info(
        f"Merge all masks in {[(tp.tile.id, tp.constellation) for tp in waterbody.paths]}"
    )
    timeseries_masks = merge_masks_all_constellations(
        waterbody=waterbody,
        mask=model_name,
    )

    df = seg_area_all(timeseries_masks, waterbody, start_date, label)
    df.date = df.date.apply(lambda x: x.date())  # remove time component

    log_to_bq(
        df=df,
        waterbody=waterbody,
        job_id=job_id,
        overwrite=overwrite,
        start_date=start_date,
        model_name=model_name,
        constellations=constellations,
    )


@task
def get_start_date(
    waterbody: WaterBody,
    model_name: str,
    overwrite: bool = False,
) -> str:
    logger = prefect.context.get("logger")

    date = "1980-01-01"

    if not overwrite:
        client = bigquery.Client()
        query = f"""
        SELECT run_id
        FROM `oxeo-main.water.reduce_runs`
        WHERE area_id = {waterbody.area_id}
        AND model_name = "{model_name}"
        ORDER BY timestamp DESC
        LIMIT 1
        """
        job = client.query(query)
        try:
            run_id = [row for row in job][0][0]
        except IndexError:
            run_id = None
        logger.warning(f"Got previous_run_id {run_id}")

        if run_id:
            client = bigquery.Client()
            query = f"""
            SELECT date
            FROM `oxeo-main.water.water_ts`
            WHERE run_id = "{run_id}"
            ORDER BY date DESC
            LIMIT 1
            """
            job = client.query(query)
            try:
                date = [row for row in job][0][0].isoformat()
            except IndexError:
                pass
    logger.warning(f"Using start_date {date}")
    return str(date)


def log_to_bq(
    df: pd.DataFrame,
    waterbody: WaterBody,
    job_id: str,
    overwrite: bool,
    start_date: str,
    model_name: str,
    constellations: list[str],
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
    )

    minx, miny, maxx, maxy = waterbody.geometry.bounds

    dict_water = dict(
        run_id=run_id,
        area_id=area_id,
        model_name=model_name,
        overwrite=overwrite,
        start_date=start_date,
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

    errors = client.insert_rows_json("oxeo-main.water.reduce_runs", [dict_water])
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

    logger = prefect.context.get("logger")
    logger.info(f"Creating cluster with {cpu=}, {memory=}")

    image = cfg.docker_oxeo_flows
    pod_spec = make_pod_spec(
        image=image,
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
    clock = CronClock("45 8 * * 3", parameter_defaults=clock_params)
    schedule = Schedule(clocks=[clock])

    executor = DaskExecutor(
        cluster_class=dynamic_cluster,
        adapt_kwargs={"maximum": 80},
        cluster_kwargs={},
    )
    storage = GitHub(
        repo=cfg.repo_name,
        path="oxeo/flows/reduce.py",
        access_token_secret=cfg.prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=cfg.docker_oxeo_flows,
        env=env,
    )

    with Flow(
        "reduce",
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

        water_list = Parameter(name="water_list", default=[25906112, 25906127])
        model_name = Parameter(name="model_name", default="cnn")
        root_dir = Parameter(name="root_dir", default="gs://oxeo-water/prod")

        overwrite = Parameter(name="overwrite", default=False)
        constellations = Parameter(name="constellations", default=["sentinel-2"])
        timeseries_label = Parameter(name="timeseries_label", default=1)

        constellations = parse_constellations_task(constellations)
        water_list = parse_water_list_task(water_list, postgis_password)

        # get geom
        db_data = fetch_water_list_task(
            water_list=water_list, password=postgis_password
        )
        gdf = data2gdf_task(db_data)
        job_id = get_job_id_task()

        # now we map across individual lakes
        waterbodies = get_waterbodies_task(gdf, constellations, root_dir)

        start_dates = get_start_date.map(
            waterbody=waterbodies,
            model_name=unmapped(model_name),
            overwrite=unmapped(overwrite),
        )

        merge_to_timeseries.map(
            waterbody=waterbodies,
            start_date=start_dates,
            model_name=unmapped(model_name),
            label=unmapped(timeseries_label),
            overwrite=unmapped(overwrite),
            job_id=unmapped(job_id),
            constellations=unmapped(constellations),
        )
    return flow


flow = create_flow()
