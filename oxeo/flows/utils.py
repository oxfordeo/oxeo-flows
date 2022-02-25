from datetime import datetime
from functools import partial
from typing import Union

import geopandas as gpd
import prefect
from prefect import task
from prefect.tasks.postgres.postgres import PostgresFetch
from pyproj import CRS
from satextractor.models import Tile
from satextractor.models.constellation_info import BAND_INFO
from satextractor.tiler import split_region_in_utm_tiles
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon

import oxeo.flows.config as cfg
from oxeo.water.models.utils import TilePath, WaterBody, get_all_paths, get_waterbodies, data2gdf


@task(log_stdout=True)
def parse_constellations_task(constellations: Union[str, list]) -> list[str]:
    logger = prefect.context.get("logger")

    all_constellations = list(BAND_INFO.keys()) + ["sentinel-1"]
    if isinstance(constellations, str):
        if constellations == "all":
            constellations = all_constellations
        elif "," in constellations:
            constellations = [c for c in constellations.split(",")]
        else:
            constellations = [constellations]

    logger.info(
        f"Chosen constellations: {constellations}. All possible constellations: {all_constellations}."
    )

    return constellations


@task(log_stdout=True)
def parse_water_list(
    water_list: Union[str, int, list],
    postgis_password: str,
) -> tuple[int, ...]:
    if water_list == "chosen":
        water_list = fetch_chosen_water_list(postgis_password)
    # split string of the form: 1234,8765
    if isinstance(water_list, str):
        water_list = water_list.split(",")
    if isinstance(water_list, int):
        water_list = [water_list]
    # ensure water_list is a tuple of ints
    water_list_parsed = tuple(int(w) for w in water_list)
    logger = prefect.context.get("logger")
    logger.warning(f"Parsed {water_list_parsed=}")
    return water_list_parsed


@task
def get_job_id_task() -> str:
    flow_run_name = prefect.context.get("flow_run_name")
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    job_id = f"{flow_run_name}_{timestamp}"
    return job_id


def fetch_chosen_water_list(
    password: str,
) -> list[int]:
    fetch = PostgresFetch(
        db_name=cfg.db_name,
        user=cfg.db_user,
        host=cfg.db_host,
        port=5432,
        fetch="all",
        query="SELECT area_id FROM chosen",
    )

    data = fetch.run(
        password=password,
    )
    data = [d[0] for d in data]
    return data


@task(log_stdout=True)
def fetch_water_list_task(
    water_list: tuple[int],
    password: str,
) -> list[tuple[int, str, str]]:
    fetch = PostgresFetch(
        db_name=cfg.db_name,
        user=cfg.db_user,
        host=cfg.db_host,
        port=5432,
        fetch="all",
        query="SELECT area_id, name, geom FROM water WHERE area_id IN %s",
    )

    data = fetch.run(
        password=password,
        data=(water_list,),  # need the extra comma to make it a tuple
    )
    logger = prefect.context.get("logger")
    logger.warning(f"Got water data with {len(data)=}")
    return data


@task(log_stdout=True)
def data2gdf_task(
    data: list[tuple[int, str, str]],
) -> gpd.GeoDataFrame:
    return data2gdf(data)


@task(log_stdout=True)
def gdf2geom(gdf):
    return gdf.unary_union


@task(log_stdout=True)
def get_all_paths_task(
    gdf: gpd.GeoDataFrame,
    constellations: list[str],
    root_dir: str = "gs://oxeo-water/prod",
) -> list[TilePath]:
    logger = prefect.context.get("logger")
    all_tilepaths = get_all_paths(gdf, constellations, root_dir)
    logger.info(
        f"All tiles for the supplied geometry: {[t.path for t in all_tilepaths]}"
    )
    return all_tilepaths


@task(log_stdout=True)
def get_waterbodies_task(
    gdf: gpd.GeoDataFrame,
    constellations: list[str],
    root_dir: str = "gs://oxeo-water/prod",
) -> list[WaterBody]:
    logger = prefect.context.get("logger")
    logger.info("Getting separate tiles and paths for each waterbody")
    waterbodies = get_waterbodies(gdf, constellations, root_dir)
    return waterbodies
