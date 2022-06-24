from datetime import date
from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, Field, conlist

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
