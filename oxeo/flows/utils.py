from functools import partial
from typing import List, Tuple, Union

import geopandas as gpd
import prefect
from prefect import task
from prefect.client import Client
from prefect.tasks.postgres.postgres import PostgresFetch
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon


@task
def generate_run_id(
    water_list: List[int],
) -> str:
    water = "_".join(water_list)
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
        port="5432",
        fetch="all",
        query="SELECT area_id, name, geom FROM water WHERE area_id IN %s",
    )

    # split string of the form: 1234,8765
    if isinstance(water_list, str):
        water_list = water_list.split(",")
    # ensure water_list is a tuple of ints
    water_list = tuple(int(w) for w in water_list)

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
def gdf2geom(
    gdf: gpd.GeoDataFrame,
) -> Union[MultiPolygon, Polygon]:
    return gdf.unary_union
