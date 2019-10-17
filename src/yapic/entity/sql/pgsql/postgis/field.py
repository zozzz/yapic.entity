from typing import Any

from yapic.entity import Field

from ._impl import (
    PostGISPointImpl,
    PostGISPointType,
    PostGISLatLngType,
    PostGISLatLngImpl,
)

Point = Field[PostGISPointImpl, PostGISPointType, Any]
LatLng = Field[PostGISLatLngImpl, PostGISLatLngType, Any]
