import json
import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple, Union

import dateparser  # type: ignore
import prefect
import pystac_client
import requests  # type: ignore
import stackstac
from prefect import Flow, Parameter, task, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from pydantic import BaseModel, Field, conlist
from pyproj import CRS, Transformer
from pystac.item import Item
from rasterio import features
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from shapely import geometry
from shapely.ops import transform as transform_proj

Point = Tuple[float, float]
LinearRing = conlist(Point, min_items=4)
PolygonCoords = conlist(LinearRing, min_items=1)
MultiPolygonCoords = conlist(PolygonCoords, min_items=1)
BBox = Tuple[float, float, float, float]  # 2D bbox


class Geometry(BaseModel):
    type: str = Field(..., example="Polygon")
    coordinates: Union[PolygonCoords, MultiPolygonCoords, Point] = Field(  # type: ignore
        ..., example=[[[1, 3], [2, 2], [4, 4], [1, 3]]]
    )

    def get(self, attr):
        return getattr(self, attr)


class Feature(BaseModel):
    type: str = Field("Feature", const=True)
    geometry: Geometry
    properties: Optional[dict]
    id: Optional[str]  # id corresponding to db entry. If present, updates db entry.
    bbox: Optional[BBox]
    labels: Optional[List[str]]


class FeatureCollection(BaseModel):
    type: str = Field("FeatureCollection", const=True)
    features: List[Feature]
    properties: Optional[dict]

    def __iter__(self):
        """iterate over features"""
        return iter(self.features)

    def __len__(self):
        """return features length"""
        return len(self.features)

    def __getitem__(self, index):
        """get feature at a given index"""
        return self.features[index]


class EventCreate(BaseModel):
    labels: Union[str, List[str]]
    aoi_id: int
    datetime: date
    keyed_values: dict


repo_name = "oxfordeo/oxeo-flows"
prefect_secret_github_token = "GITHUB"


@task(log_stdout=True, max_retries=1, retry_delay=timedelta(seconds=10))
def extract(_id: int, U: Optional[str] = None, P: Optional[str] = None) -> Feature:

    logger = prefect.context.get("logger")
    logger.info(f"Extracting AOI {_id}")

    # login
    if not U or not P:
        U = os.environ.get("username")
        P = os.environ.get("password")

    base_url = "https://api.oxfordeo.com/"

    authurl = os.path.join(base_url, "auth", "token")
    r = requests.post(authurl, data={"username": U, "password": P})
    token = json.loads(r.text)["access_token"]

    headers = {"Authorization": f"Bearer {token}"}

    # query
    query = dict(
        id=_id,
    )

    aoiurl = os.path.join(base_url, "aoi")
    r = requests.get(aoiurl, headers=headers, json=query)
    j = json.loads(r.text)
    return Feature(**j["features"][0])


@task(log_stdout=True)
def stac(
    aoi: Feature,
    start_datetime: datetime,
    end_datetime: datetime,
) -> List[Item]:

    logger = prefect.context.get("logger")

    URL = "https://earth-search.aws.element84.com/v0"
    catalog = pystac_client.Client.open(URL)

    start_datetime = dateparser.parse(start_datetime)
    end_datetime = dateparser.parse(end_datetime)
    print(start_datetime, end_datetime)

    items = catalog.search(
        intersects=geometry.mapping(
            geometry.box(*(geometry.shape(aoi.geometry.__dict__).bounds))
        ),
        collections=["sentinel-s2-l2a-cogs"],
        datetime=f"{start_datetime.isoformat()[0:10]}/{end_datetime.isoformat()[0:10]}",
    ).get_all_items()

    logger.info(f"Got {len(items)} STAC items")

    return items


@task(log_stdout=True)
def transform(aoi: Feature, item: Item) -> EventCreate:
    logger = prefect.context.get("logger")
    logger.info("NDVI transforming.")

    geom = geometry.shape(aoi.geometry.__dict__)

    xr_stack = stackstac.stack(
        item,
        resolution=10,
        bounds_latlon=geom.bounds,
        resampling=Resampling.bilinear,
        errors_as_nodata=(
            IOError(r"HTTP response code: \d\d\d"),
            RasterioIOError(".*"),
        ),
    )

    # swap geom to utm
    crs_wgs84 = CRS("EPSG:4326")
    crs_utm = CRS(xr_stack.crs)
    wgs2utm = Transformer.from_crs(crs_wgs84, crs_utm, always_xy=True).transform
    utm_geom = transform_proj(wgs2utm, geom)

    out_shp = (xr_stack.coords["y"].shape[0], xr_stack.coords["x"].shape[0])

    # burn in mask
    mask_arr = features.rasterize(
        [utm_geom],
        out_shape=out_shp,
        fill=0,
        transform=xr_stack.transform,
        all_touched=False,
        default_value=1,
        dtype=None,
    )

    # build computation graph for NDVI: (NIR-red) / (NIR+RED)
    xr_stack.coords["mask"] = (("y", "x"), mask_arr)

    xr_stack = xr_stack.where(xr_stack.mask == 1)

    xr_ndvi = (xr_stack.sel({"band": "B08"}) - xr_stack.sel({"band": "B04"})) / (
        xr_stack.sel({"band": "B08"}) + xr_stack.sel({"band": "B04"})
    )
    xr_ndvi = xr_ndvi.mean(dim=["x", "y"])

    # call the compute with the dask backend
    result = xr_ndvi.compute()

    # cast to pandas
    df = result.to_pandas()
    df.index = df.index.date

    event = EventCreate(
        labels="ndvi",
        aoi_id=aoi.id,
        datetime=df.index[0],
        keyed_values={"mean_value": df[0]},
    )

    return event


@task(log_stdout=True)
def load(events: List[EventCreate]) -> bool:
    print("Loading... (not actually)")
    print(events)
    return True


def create_flow():
    storage = GitHub(
        repo=repo_name,
        path="oxeo/flows/calc-ndvi.py",
        access_token_secret=prefect_secret_github_token,
    )
    run_config = KubernetesRun(
        image="413730540186.dkr.ecr.eu-central-1.amazonaws.com/flows:latest",
    )

    with Flow(
        "calc-ndvi",
        storage=storage,
        run_config=run_config,
    ) as flow:
        api_username = "admin@oxfordeo.com"
        api_password = PrefectSecret("API_PASSWORD")
        aoi_id = Parameter(name="aoi_id", default=1)
        start_datetime = Parameter(name="start_datetime", default="2020-01-01")
        end_datetime = Parameter(name="end_datetime", default="2020-01-08")

        aoi = extract(aoi_id, api_username, api_password)
        items = stac(aoi, start_datetime, end_datetime)
        events = transform.map(unmapped(aoi), items)
        _ = load(events)

    return flow


flow = create_flow()
