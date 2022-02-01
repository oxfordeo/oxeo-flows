from functools import partial
from typing import List, Tuple, Union

import geopandas as gpd
import prefect
from prefect import task
from prefect.client import Client
from prefect.tasks.postgres.postgres import PostgresFetch
from pyproj import CRS
from satextractor.models import Tile
from satextractor.models.constellation_info import BAND_INFO
from satextractor.tiler import split_region_in_utm_tiles
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon

import oxeo.flows.config as cfg
from oxeo.water.models.utils import TilePath, WaterBody


@task
def parse_constellations(constellations: Union[str, list]) -> List[str]:
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


@task
def parse_water_list(water_list):
    # split string of the form: 1234,8765
    if isinstance(water_list, str):
        water_list = water_list.split(",")
    if isinstance(water_list, int):
        water_list = [water_list]
    # ensure water_list is a tuple of ints
    water_list = tuple(int(w) for w in water_list)
    return water_list


@task
def generate_run_id(
    water_list: List[int],
) -> str:
    water = "_".join(str(w) for w in water_list)
    return f"lakes_{water}"


@task(max_retries=0)
def rename_flow_run(
    aoi_id: int,
) -> None:
    logger = prefect.context.get("logger")
    old_name = prefect.context.get("flow_run_name")
    new_name = f"run_{aoi_id}"
    logger.info(f"Rename the Flow Run from {old_name} to {new_name}")
    Client().set_flow_run_name(prefect.context.get("flow_run_id"), new_name)


@task
def fetch_water_list(
    water_list: List[int],
    password: str,
) -> List[Tuple[int, str, str]]:
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
    return data


@task
def data2gdf(
    data: List[Tuple[int, str, str]],
) -> gpd.GeoDataFrame:
    wkb_hex = partial(wkb.loads, hex=True)
    gdf = gpd.GeoDataFrame(data, columns=["area_id", "name", "geometry"])
    gdf.geometry = gdf.geometry.apply(wkb_hex)
    gdf.crs = CRS.from_epsg(4326)
    return gdf


@task
def gdf2geom(gdf):
    return gdf.unary_union


def make_paths(bucket, tiles, constellations, root_dir):
    return [
        TilePath(tile=tile, constellation=cons, root=root_dir)
        for tile in tiles
        for cons in constellations
    ]


def get_tiles(
    geom: Union[Polygon, MultiPolygon, gpd.GeoSeries, gpd.GeoDataFrame]
) -> List[Tile]:
    try:
        geom = geom.unary_union
    except AttributeError:
        pass
    return split_region_in_utm_tiles(region=geom, bbox_size=10000)


@task
def get_all_paths(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    constellations: List[str],
    root_dir: str = "prod",
) -> List[TilePath]:
    logger = prefect.context.get("logger")
    all_tiles = get_tiles(gdf)
    all_tilepaths = make_paths(bucket, all_tiles, constellations, root_dir)
    logger.info(
        f"All tiles for the supplied geometry: {[t.path for t in all_tilepaths]}"
    )
    return all_tilepaths


@task
def get_waterbodies(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    constellations: List[str],
    root_dir: str = "prod",
) -> List[WaterBody]:
    logger = prefect.context.get("logger")
    logger.info("Getting separate tiles and paths for each waterbody")
    waterbodies = []
    for water in gdf.to_dict(orient="records"):
        tiles = get_tiles(water["geometry"])
        waterbodies.append(
            WaterBody(
                **water,
                paths=make_paths(bucket, tiles, constellations, root_dir=root_dir),
            )
        )
    return waterbodies
