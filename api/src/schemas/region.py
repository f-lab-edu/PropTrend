from . import PropTrendCoreModel


class SigunguResponse(PropTrendCoreModel):
    """시도에 속한 시군구."""

    sigungu_code: str
    sigungu_name: str


class SidoResponse(PropTrendCoreModel):
    """시도와 그에 속한 시군구 목록."""

    sido_code: str
    sido_name: str
    sigungus: list[SigunguResponse]
