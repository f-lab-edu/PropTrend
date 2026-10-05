from datetime import datetime

from pydantic import Field

from ..model.prop_transaction import PropertyType
from . import PropTrendCoreModel
from .prop_transaction import AddressResponse


class FavoriteComplexAddRequest(PropTrendCoreModel):
    """단지 즐겨찾기 추가 요청."""

    complex_id: int = Field(ge=1)


class FavoriteComplexAddResponse(PropTrendCoreModel):
    """단지 즐겨찾기 추가 결과."""

    complex_id: int


class FavoriteComplexResponse(AddressResponse):
    """즐겨찾기한 단지."""

    complex_id: int
    property_type: PropertyType
    building_name: str | None
    build_year: int | None
    favorited_at: datetime
