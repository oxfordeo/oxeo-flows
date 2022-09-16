import json
import os
from datetime import timedelta
from typing import Optional

import gcsfs
import httpx
import numpy as np
import pandas as pd
import prefect
import xarray as xr
from dask.distributed import LocalCluster
from dask_kubernetes import KubeCluster, make_pod_spec
from geojson import Feature
from h2ox.reducer import XRReducer
from prefect import Flow, Parameter, task
from prefect.executors import DaskExecutor, LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from shapely import geometry

from oxeo.flows.models import EventCreate

Box = tuple[float, float, float, float]

BASE_URL = "https://api.oxfordeo.com/"


@task(log_stdout=True)
def get_aoi(aoi_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Box:

    logger = prefect.context.get("logger")
    logger.info("Get AOI")
    # login
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    client = httpx.Client(base_url=BASE_URL)

    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    r = client.get("aoi/", params=dict(id=aoi_id), headers=headers)
    aoi = json.loads(r.text)

    aoi = aoi["features"][0]

    client.close()

    logger.info(aoi)

    return aoi


@task(log_stdout=True, max_retries=1, retry_delay=timedelta(seconds=10))
def transform(
    aoi: Feature,
    start_datetime: str,
    end_datetime: str,
    include: list[str],
) -> list[EventCreate]:
    """
    In this transform, we take an AOI and push a reduced tp.

    If the AOI is an ag_area or a basin, reduce the tp directly overtop

    If the AOI is a waterbody, do nothing for now.

    """

    logger = prefect.context.get("logger")
    logger.info("tp transforming.")

    if not (
        "agricultural_area" in aoi["properties"]["labels"]
        or "basin" in aoi["properties"]["labels"]
    ):
        raise ValueError("AOI is not an agricultural_area or basin")

    CHIRPS_STORE = "gs://oxeo-chirps/build2"
    SEAS_STORE = "gs://oxeo-seasonal/tp"

    zx_chirps = xr.open_zarr(gcsfs.GCSMap(CHIRPS_STORE))
    zx_seas = xr.open_zarr(gcsfs.GCSMap(SEAS_STORE))

    R_seas = XRReducer(zx_seas["tp"])
    R_chirps = XRReducer(zx_chirps["precip"])

    geom = geometry.shape(aoi["geometry"])

    chirps_red = R_chirps.reduce(geom, start_datetime, end_datetime)
    chirps_df = chirps_red.to_dataframe("tp")
    chirps_df["yr-mnth"] = (
        chirps_df.index.year.astype(str)
        + "-"
        + chirps_df.index.month.map(lambda i: f"{i:02d}")
    )

    all_months = pd.to_datetime(chirps_df["yr-mnth"].unique() + "-01")

    records = {m.date(): {} for m in all_months}

    if "back-cast" in include:
        logger.info("INCLUDE: back-cast")
        # do the actual CHIRPS_SPI for each month
        chirps_summary = chirps_df.groupby("yr-mnth").sum()
        chirps_summary["month"] = (
            chirps_summary.index.str.split("-").str[1].astype(int).astype(str)
        )
        chirps_summary["SPI_mean"] = chirps_summary["month"].map(
            aoi["properties"]["CHIRPS_SPI"]["mean"]
        )
        chirps_summary["SPI_std"] = chirps_summary["month"].map(
            aoi["properties"]["CHIRPS_SPI"]["std"]
        )
        chirps_summary["SPI"] = (
            chirps_summary["tp"] - chirps_summary["SPI_mean"]
        ) / chirps_summary["SPI_std"]
        chirps_summary["sdt"] = pd.to_datetime(chirps_summary.index + "-01")
        chirps_summary.loc[
            (chirps_summary["SPI"] == np.inf) & (chirps_summary["tp"] > 0), "SPI"
        ] = 999.0
        chirps_summary.loc[
            (chirps_summary["SPI"].isna()) & (chirps_summary["tp"] == 0), "SPI"
        ] = 0

        for idx, spi in chirps_summary.set_index("sdt")["SPI"].items():
            records[idx.date()]["CHIRPS_SPI_actual"] = spi

    if "month-offset" in include or "forecast" in include:

        # prep the seas dataframe
        seas_red = R_seas.reduce(geom, start_datetime, end_datetime)
        seas_df = seas_red.to_dataframe("tp")

        # add timedelta(days=0) rows to get diff with 0 offset
        add_records = [
            {"time": ts, "member": member, "step": timedelta(days=0), "tp": 0}
            for ts, member in np.unique(
                seas_df.reset_index()[["time", "member"]].to_records(index=False)
            )
        ]

        # get daily precip taking diff
        seas_df = (
            pd.concat([pd.DataFrame(add_records), seas_df.reset_index()])
            .sort_values(["time", "member", "step"])
            .set_index(["time", "member", "step"])
            .diff()
        )

        # rm step==0
        seas_df = (
            seas_df.reset_index()
            .loc[seas_df.reset_index()["step"] > timedelta(days=0)]
            .set_index(["time", "member", "step"])
        )

        # rm all-zero members
        all_zero = (seas_df == 0).reset_index().groupby(["time", "member"])["tp"].prod()
        not_zero_idx = all_zero.loc[all_zero == 0].index
        seas_df = (
            seas_df.reset_index()
            .set_index(["time", "member"])
            .loc[not_zero_idx]
            .reset_index()
            .set_index(["time", "member", "step"])
        )

        # reduce a mean over members
        seas_df_mean = pd.DataFrame(
            seas_df.reset_index().groupby(["time", "step"])["tp"].mean()
        )

        # get the valid time
        seas_df_mean["valid_time"] = pd.Series(
            index=seas_df_mean.index,
            data=(
                pd.to_datetime(seas_df_mean.reset_index()["time"])
                + timedelta(days=12)
                + seas_df_mean.reset_index()["step"]
            ).values,
        )

        # get the yr-month
        seas_df_mean["valid-yr-mo"] = (
            seas_df_mean["valid_time"].dt.year.astype(str)
            + "-"
            + seas_df_mean["valid_time"].dt.month.map(lambda i: f"{i:02d}")
        )

        # sum reduce
        seas_df_summary = (
            seas_df_mean.reset_index()
            .groupby(["time", "valid-yr-mo"])["tp"]
            .sum()
            .reset_index()
            .groupby("time")
            .head(7)
        )  # .groupby('time').tail(6)

        # add the forecast month 'rank'
        seas_df_summary["RANK"] = (
            seas_df_summary.groupby("time")["valid-yr-mo"].rank().astype(int)
        )

        if "month-offset" in include:
            # seas forecast starts at the 13th of the month. For each month, get the first 12 days from CHIRPS, then get the last days from SEAS5
            chirps_df["day"] = chirps_df.index.day
            chirps_first_12 = pd.DataFrame(
                chirps_df.loc[chirps_df["day"] < 13].groupby("yr-mnth")["tp"].sum()
            )
            chirps_first_12["RANK"] = 1

            # merge on to seas_df_summary
            seas_df_summary = pd.merge(
                seas_df_summary,
                chirps_first_12.reset_index().rename(columns={"tp": "chirps"}),
                how="left",
                left_on=["valid-yr-mo", "RANK"],
                right_on=["yr-mnth", "RANK"],
            ).drop(columns=["yr-mnth"])

            seas_df_summary["chirps"] = seas_df_summary["chirps"].fillna(0)

            # seas_df_summary["tp"] += seas_df_summary["chirps"]

        seas_df_summary["valid-mo"] = (
            seas_df_summary["valid-yr-mo"].str.split("-").str[1].astype(int).astype(str)
        )

        seas_df_summary["chirps_SPI_mean"] = seas_df_summary["valid-mo"].map(
            aoi["properties"]["CHIRPS_SPI"]["mean"]
        )
        seas_df_summary["chirps_SPI_std"] = seas_df_summary["valid-mo"].map(
            aoi["properties"]["CHIRPS_SPI"]["std"]
        )
        seas_df_summary["chirps_SPI"] = (
            seas_df_summary["chirps"] - seas_df_summary["chirps_SPI_mean"] * 13 / 30.5
        ) / (seas_df_summary["chirps_SPI_std"] * 13 / 30.5)

        seas_df_summary["SPI_mean"] = seas_df_summary.apply(
            lambda row: aoi["properties"]["SEAS_SPI"][row["valid-mo"]]["mean"][
                str(float(row["RANK"]))
            ],
            axis=1,
        )
        seas_df_summary["SPI_std"] = seas_df_summary.apply(
            lambda row: aoi["properties"]["SEAS_SPI"][row["valid-mo"]]["std"][
                str(float(row["RANK"]))
            ],
            axis=1,
        )
        seas_df_summary["SPI"] = (
            seas_df_summary["tp"] - seas_df_summary["SPI_mean"]
        ) / seas_df_summary["SPI_std"]

        # where Rank==1, blend  CHIRPS and SEAS
        seas_df_summary.loc[seas_df_summary["RANK"] == 1, "SPI"] = (
            seas_df_summary.loc[
                seas_df_summary["RANK"] == 1, ["SPI", "chirps_SPI"]
            ].sum(axis=1)
            / 2
        )

        seas_df_summary.loc[
            (seas_df_summary["SPI"] == np.inf) & (seas_df_summary["tp"] > 0), "SPI"
        ] = 999.0
        seas_df_summary.loc[
            (seas_df_summary["SPI"].isna()) & (seas_df_summary["tp"] == 0), "SPI"
        ] = 0

        if "month-offset" in include:
            logger.info("INCLUDE: month-offset")
            # write all rank==1 to events
            for idx, spi in seas_df_summary.loc[
                seas_df_summary["RANK"] == 1, ["time", "SPI"]
            ].to_records(index=False):
                records[pd.to_datetime(idx).date()]["MIXED_SPI"] = spi

        if "forecast" in include:
            # write all RANK>1 to forecast events
            logger.info("INCLUDE: forecast")

            for idx, ll in (
                seas_df_summary.loc[seas_df_summary["RANK"] > 1]
                .groupby("time")["SPI"]
                .apply(list)
                .items()
            ):
                records[pd.to_datetime(idx).date()]["FORECAST_SPI"] = dict(
                    zip(range(1, 7), ll)
                )

    events = [
        dict(
            labels=["total_precipitation"],
            aoi_id=int(aoi["properties"]["aoi_id"]),
            datetime=idx.isoformat()[0:10],
            keyed_values=kvs,
        )
        for idx, kvs in records.items()
    ]
    logger.info("Got events")
    logger.info(events)

    return events


@task(log_stdout=True)
def load(events: list[dict], U: Optional[str] = None, P: Optional[str] = None) -> bool:

    logger = prefect.context.get("logger")

    # refresh headers
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    client = httpx.Client(base_url=BASE_URL)

    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    r = client.post("events/", json=events, headers=headers)
    if str(r.status_code) != "201":
        raise ValueError(f"Status code: {r.status_code}")

    client.close()

    logger.info(f"Successfully inserted {len(events)=} events into the db")

    return True


image = "413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest"
repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


def dynamic_cluster(**kwargs):
    n_workers = prefect.context.parameters["n_workers"]
    memory = prefect.context.parameters["memory_per_worker"]
    cpu = prefect.context.parameters["cpu_per_worker"]
    gpu = prefect.context.parameters["gpu_per_worker"]

    if n_workers == 0:
        return LocalCluster()

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

    pod_spec = make_pod_spec(
        image=image,
        extra_container_config=container_config,
        memory_limit=memory,
    )
    pod_spec.spec.containers[0].args.append("--no-dashboard")

    root_spec = make_pod_spec(image=image)
    root_spec.spec.containers[0].args.append("--no-dashboard")

    return KubeCluster(
        n_workers=n_workers,
        pod_template=pod_spec,
        scheduler_pod_template=root_spec,
        **kwargs,
    )


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/precip.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image=image,
        cpu_limit=4,
        cpu_request=4,
        memory_limit="16G",
        memory_request="16G",
    )
    executor = DaskExecutor(
        cluster_class=dynamic_cluster,
        adapt_kwargs={"maximum": 8},
    )

    with Flow(
        "precipitation",
        storage=storage,
        run_config=run_config,
        executor=executor,
    ) as flow:
        flow.add_task(Parameter("n_workers", default=0))
        flow.add_task(Parameter("memory_per_worker", default="8G"))
        flow.add_task(Parameter("cpu_per_worker", default=2))
        flow.add_task(Parameter("gpu_per_worker", default=0))

        api_username = "admin@oxfordeo.com"
        api_password = PrefectSecret("API_PASSWORD")
        aoi_id = Parameter(name="aoi_id", default=2179)
        start_datetime = Parameter(name="start_datetime", default="1993-01-01")
        end_datetime = Parameter(name="end_datetime", default="2020-12-31")
        include = Parameter(
            name="include", default=["back-cast", "month-offset", "forecast"]
        )

        aoi = get_aoi(aoi_id, api_username, api_password)
        events = transform(aoi, start_datetime, end_datetime, include)
        _ = load(events, api_username, api_password)

    return flow


flow = create_flow()

if __name__ == "__main__":
    # print (help(flow.run))
    flow.run(
        parameters=dict(
            aoi_id=2179, start_datetime="2012-01-01", end_datetime="2012-12-31"
        ),
        executor=LocalExecutor(),
    )
