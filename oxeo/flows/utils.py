from functools import partial
from typing import List, Tuple, Union

import geopandas as gpd
import prefect
from attr import frozen
from prefect import task
from prefect.client import Client
from prefect.tasks.postgres.postgres import PostgresFetch
from satextractor.models import Tile
from satextractor.tiler import split_region_in_utm_tiles
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon


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


DB_NAME = "geom"
DB_USER = "reader"
DB_HOST = "35.204.253.189"


@task
def fetch_water_list(
    water_list: Union[str, List[int]],
    password: str,
) -> List[Tuple[int, str, str]]:
    fetch = PostgresFetch(
        db_name=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
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
    return gdf


@task
def gdf2geom(gdf):
    return gdf.unary_union


@frozen
class TilePath:
    tile: Tile
    path: str


@frozen
class WaterDict:
    area_id: int
    name: str
    geometry: Union[Polygon, MultiPolygon]
    paths: List[TilePath]


def make_paths(bucket, tiles, constellations):
    return [
        TilePath(tile=tile, path=f"{bucket}/prod/{tile.id}/{cons}")
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
) -> List[TilePath]:
    logger = prefect.context.get("logger")
    all_tiles = get_tiles(gdf)
    all_tilepaths = make_paths(bucket, all_tiles, constellations)
    logger.info(
        f"All tiles for the supplied geometry: {[t.path for t in all_tilepaths]}"
    )
    return all_tilepaths


@task
def get_water_dicts(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    constellations: List[str],
) -> List[WaterDict]:
    logger = prefect.context.get("logger")
    logger.info("Getting separate tiles and paths for each waterbody")
    water_dicts = []
    for water in gdf.to_dict(orient="records"):
        tiles = get_tiles(water["geometry"])
        water_dicts.append(
            WaterDict(**water, paths=make_paths(bucket, tiles, constellations))
        )
    return water_dicts
