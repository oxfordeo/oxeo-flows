import json
import os
import pickle
from typing import Optional

import httpx
from loguru import logger
from prefect import Client
from shapely import geometry

API_URL = os.environ.get("API_URL", "https://api.oxfordeo.com/")
FLOW_ID = os.environ.get("FLOW_ID")
U = os.environ.get("OXEO_USERNAME")
P = os.environ.get("OXEO_PASSWORD")


def refresh_headers(client: httpx.Client) -> dict:

    r = client.post("auth/token", data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    return headers


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
        timeout=30,
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
            timeout=30,
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
                limit=100,
                page=0,
            )
        ),
        headers=headers,
        timeout=30,
    )

    basin_fc = json.loads(basin_fc.text)

    # print (basin_fc)

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
                    limit=100,
                    page=ii,
                )
            ),
            headers=headers,
            timeout=30,
        )

        basin_fc = json.loads(basin_fc.text)

        basin_ids += [ft["id"] for ft in basin_fc["features"]]

    all_fts = basin_ids + ag_ids
    logger.info(f"Collected {len(all_fts)} fts")

    return all_fts


def deploy_ids(ids: list[int]) -> int:

    client = Client()

    for ii, _id in enumerate(ids):

        run_id = client.create_flow_run(
            flow_id=FLOW_ID,
            run_name=str(_id),
            parameters=dict(
                aoi_id=_id, start_datetime="1993-01-01", end_datetime="2020-12-31"
            ),
        )
        logger.info(f"Deployed {_id}:  {run_id}    {ii}/{len(ids)}")


if __name__ == "__main__":
    if not os.path.exists("./all_ids.pkl"):
        all_ids = sorted(get_all_ids())
        pickle.dump(all_ids, open("./all_ids.pkl", "wb"))
    else:
        all_ids = pickle.load(open("./all_ids.pkl", "rb"))

    deploy_ids(all_ids[1000:])

    logger.info("DONE!")
