from pydantic import Field

from . import PropTrendCoreModel


class FavoriteComplexToggleRequest(PropTrendCoreModel):
    """단지 즐겨찾기 토글 요청."""

    complex_id: int = Field(ge=1)


class FavoriteComplexToggleResponse(PropTrendCoreModel):
    """단지 즐겨찾기 토글 결과."""

    complex_id: int
    # 토글한 뒤의 상태. true면 이번 요청으로 추가됐고 false면 제거됐다.
    is_favorite: bool
