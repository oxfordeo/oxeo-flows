import json
import os
from datetime import timedelta
from typing import Optional

import gcsfs
import httpx
import numpy as np
import pandas as pd
import xarray as xr
from geojson import Feature
from h2ox.reducer import XRReducer
from loguru import logger
from shapely import geometry

CHIRPS_STORE = os.environ.get("PRECIP_STORE", "gs://oxeo-chirps/build2")
SEAS_STORE = os.environ.get("SEAS_STORE", "gs://oxeo-seasonal/tp")
API_URL = os.environ.get("API_URL", "https://api.oxfordeo.com/")
U = os.environ.get("OXEO_USERNAME")
P = os.environ.get("OXEO_PASSWORD")


def refresh_headers(client: httpx.Client) -> dict:

    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    return headers


def get_aoi(
    aoi_id: str,
    client: httpx.Client,
    headers: dict,
) -> Feature:

    r = client.get("aoi/", params=dict(id=aoi_id), headers=headers)

    aoi = json.loads(r.text)

    aoi = aoi["features"][0]

    ft = Feature(geometry=aoi["geometry"], properties=aoi["properties"], id=aoi["id"])

    return ft


def reduce_SPI(aoi: Feature, R_seas: XRReducer, R_chirps: XRReducer) -> Feature:

    geom = geometry.shape(aoi["geometry"])

    red_seas = R_seas.reduce(geom, "1993-01-01", "2019-12-31")

    seas_df = red_seas.to_dataframe("tp")

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
    seas_df_summary["RANK"] = seas_df_summary.groupby("time")["valid-yr-mo"].rank()

    # add the time month
    seas_df_summary["mo"] = seas_df_summary["time"].dt.month

    seas_df_spi = (
        seas_df_summary.drop(columns=["valid-yr-mo", "time"])
        .groupby(["mo", "RANK"])
        .mean()
        .rename(columns={"tp": "mean"})
    )
    seas_df_spi["std"] = (
        seas_df_summary.drop(columns=["valid-yr-mo", "time"])
        .groupby(["mo", "RANK"])
        .std()
    )
    seas_df_spi["min"] = (
        seas_df_summary.drop(columns=["valid-yr-mo", "time"])
        .groupby(["mo", "RANK"])
        .min()
    )
    seas_df_spi["max"] = (
        seas_df_summary.drop(columns=["valid-yr-mo", "time"])
        .groupby(["mo", "RANK"])
        .max()
    )

    aoi["properties"]["SEAS_SPI"] = (
        seas_df_spi.groupby(level=0)
        .apply(lambda df: df.xs(df.name).to_dict())
        .to_dict()
    )

    return aoi


def post_aoi(aoi: Feature, client: httpx.Client, headers: dict) -> int:

    r = client.post("aoi/update/", headers=headers, json=aoi)

    if r.status_code == 201:
        return 1
    else:
        raise ValueError(f"aoi push returned with {r.status_code}")


def run_aoi(
    aoi_id: str, client: httpx.Client, R_seas: XRReducer, R_chirps: XRReducer
) -> int:

    # refresh the headers
    headers = refresh_headers(client)

    # EXTRACT - get the aoi
    aoi = get_aoi(aoi_id, client, headers)

    # TRANSFORM - reduce the geometry
    aoi = reduce_SPI(aoi, R_seas, R_chirps)

    # LOAD - push the aoi back up
    post_aoi(aoi, client, headers)

    return aoi["properties"]["seas2chirps"]


def loop_all(ids: list[str], client: Optional[httpx.Client] = None) -> int:

    if client is None:
        client = httpx.Client(base_url=API_URL)

    zx_chirps = xr.open_zarr(gcsfs.GCSMap(CHIRPS_STORE))
    zx_seas = xr.open_zarr(gcsfs.GCSMap(SEAS_STORE))

    R_seas = XRReducer(zx_seas["tp"])
    R_chirps = XRReducer(zx_chirps["precip"])

    for ii, aoi_id in enumerate(ids):

        s2c = run_aoi(aoi_id, client, R_seas, R_chirps)

        logger.info(f"AOI {aoi_id} updated successfully: {s2c:.5f}; {ii}/{len(ids)}")

    return 1


def get_all_ids(client: Optional[httpx.Client] = None) -> list[str]:

    # MOZ and ZIM AOI
    aoi_geom = geometry.Polygon(
        [
            [8.722675794184317, -27.977835868645567],
            [43.35158204418432, -27.977835868645567],
            [43.35158204418432, -7.779492375505522],
            [8.722675794184317, -7.779492375505522],
            [8.722675794184317, -27.977835868645567],
        ]
    )

    if client is None:
        client = httpx.Client(base_url=API_URL)

    # refresh headers
    headers = refresh_headers(client)

    ii = 0

    ag_fc = client.get(
        "aoi/",
        params=(
            dict(
                labels=json.dumps(["agricultural_area"]),
                centroids=True,
                page=ii,
            )
        ),
        headers=headers,
    )
    ag_fc = json.loads(ag_fc.text)

    ag_ids = [ft["id"] for ft in ag_fc["features"]]

    while len(ag_fc["features"]) > 0:

        ii += 1

        ag_fc = client.get(
            "aoi/",
            params=(
                dict(
                    labels=json.dumps(["agricultural_area"]),
                    centroids=True,
                    page=ii,
                )
            ),
            headers=headers,
        )

        ag_fc = json.loads(ag_fc.text)

        ag_ids += [ft["id"] for ft in ag_fc["features"]]

    ii = 0
    basin_fc = client.get(
        "aoi/",
        params=(
            dict(
                labels=json.dumps(["basin"]),
                geometry=json.dumps(geometry.mapping(aoi_geom)),
                centroids=True,
                page=0,
            )
        ),
        headers=headers,
    )

    basin_fc = json.loads(basin_fc.text)

    basin_ids = [ft["id"] for ft in basin_fc["features"]]

    while len(basin_fc["features"]) > 0:

        ii += 1

        basin_fc = client.get(
            "aoi/",
            params=(
                dict(
                    labels=json.dumps(["basin"]),
                    geometry=json.dumps(geometry.mapping(aoi_geom)),
                    centroids=True,
                    page=ii,
                )
            ),
            headers=headers,
        )

        basin_fc = json.loads(basin_fc.text)

        basin_ids += [ft["id"] for ft in basin_fc["features"]]

    all_fts = basin_ids + ag_ids
    logger.info(f"Collected {len(all_fts)} fts")

    return all_fts


if __name__ == "__main__":
    # all_ids = get_all_ids()

    loop_all([2179])

    logger.info("DONE!")
