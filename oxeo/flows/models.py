from typing import List, Union

from attr import frozen
from satextractor.models import Tile
from shapely.geometry import MultiPolygon, Polygon


@frozen
class TilePath:
    tile: Tile
    constellation: str
    bucket: str = "oxeo-water"

    @property
    def path(self):
        return f"{self.bucket}/prod/{self.tile.id}/{self.constellation}"


@frozen
class WaterDict:
    area_id: int
    name: str
    geometry: Union[Polygon, MultiPolygon]
    paths: List[TilePath]
