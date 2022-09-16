import json
import os
from typing import Optional

import gcsfs
import httpx
import xarray as xr
from geojson import Feature
from h2ox.reducer import XRReducer
from loguru import logger
from shapely import geometry

DATA_STORE = os.environ.get("PRECIP_STORE", "gs://oxeo-chirps/build2")
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


def reduce_SPI(aoi: Feature, R: XRReducer) -> Feature:

    geom = geometry.shape(aoi["geometry"])

    red_array = R.reduce(geom, "1981-01-01", "2019-12-31")

    df = red_array.to_dataframe(name="tp")
    df["month"] = df.index.month
    df["year"] = df.index.year
    df["yr-mnth"] = df.index.month.astype(str) + "-" + df.index.year.astype(str)

    df_monthly = df.groupby("yr-mnth").sum()
    df_monthly["month"] = df_monthly.index.str.split("-").str[0]
    df_monthly["year"] = df_monthly.index.str.split("-").str[1]

    df_summary = df_monthly.groupby("month").mean().rename(columns={"tp": "mean"})
    df_summary["std"] = df_monthly.groupby("month").std()["tp"]
    df_summary["median"] = df_monthly.groupby("month").median()["tp"]
    df_summary["max"] = df_monthly.groupby("month").max()["tp"]
    df_summary["min"] = df_monthly.groupby("month").min()["tp"]

    aoi["properties"]["CHIRPS_SPI"] = df_summary.to_dict()

    return aoi


def post_aoi(aoi: Feature, client: httpx.Client, headers: dict) -> int:

    r = client.post("aoi/update/", headers=headers, json=aoi)

    if r.status_code == 201:
        return 1
    else:
        raise ValueError(f"aoi push returned with {r.status_code}")


def run_aoi(aoi_id: str, client: httpx.Client, R: XRReducer) -> int:

    # refresh the headers
    headers = refresh_headers(client)

    # EXTRACT - get the aoi
    aoi = get_aoi(aoi_id, client, headers)

    # TRANSFORM - reduce the geometry
    aoi = reduce_SPI(aoi, R)

    # LOAD - push the aoi back up
    post_aoi(aoi, client, headers)

    return 1


def loop_all(ids: list[str], client: Optional[httpx.Client] = None) -> int:

    if client is None:
        client = httpx.Client(base_url=API_URL)

    zx = xr.open_zarr(gcsfs.GCSMap(DATA_STORE))

    R = XRReducer(zx["precip"])

    for ii, aoi_id in enumerate(ids):

        run_aoi(aoi_id, client, R)
        logger.info(f"AOI {aoi_id} updated successfully; {ii}/{len(ids)}")

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
    all_ids = get_all_ids()

    loop_all(all_ids)

    logger.info("DONE!")
