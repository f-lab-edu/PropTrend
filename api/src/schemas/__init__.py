from pydantic import BaseModel, ConfigDict


class PropTrendCoreModel(BaseModel):
    # 서비스가 ORM 행을 model_validate로 바로 넘기므로 속성에서 값을 읽는다.
    model_config = ConfigDict(from_attributes=True)


class ErrorDetail(PropTrendCoreModel):
    """요청 검증에 실패한 항목 하나."""

    loc: list[str | int]
    msg: str
    type: str


class ErrorResponse(PropTrendCoreModel):
    """모든 예외 응답이 공유하는 구조."""

    message: str
    # 요청 검증 실패일 때만 채워진다. 구조를 고정하려고 다른 예외에서도 빈 목록으로 내보낸다.
    errors: list[ErrorDetail] = []
    # 서버 로그의 request_id와 같은 값. 로그 미들웨어를 거치지 않은 요청이면 비어 있다.
    trace_id: str | None = None
