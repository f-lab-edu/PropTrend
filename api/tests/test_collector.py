"""오픈API 응답의 결과코드 분기와 XML 파싱."""

from typing import Any

import httpx
import pytest
from conftest import SERVICE_KEY

from src.jobs.collector import (
    DailyLimitReachedError,
    LegalDongCodeCollector,
    OpenApiError,
    OpenApiStatusError,
    RtmsDataCollector,
)


def rtms_response_body(result_code: str = "000", items: str = "", total_count: int = 0) -> str:
    """실거래가 오픈API 응답 Body."""
    return (
        f"<response><header><resultCode>{result_code}</resultCode><resultMsg>msg</resultMsg></header>"
        f"<body><items>{items}</items><totalCount>{total_count}</totalCount></body></response>"
    )


def rtms_response_body_item(apt_nm: str) -> str:
    return f"<item><sggCd>11110</sggCd><aptNm> {apt_nm} </aptNm><dealAmount>36,900</dealAmount></item>"


def legal_dong_response_body(rows: str, total_count: int = 1) -> str:
    """법정동코드 오픈API 정상 응답 Body."""
    return (
        f"<StanReginCd><head><totalCount>{total_count}</totalCount>"
        f"<RESULT><resultCode>INFO-000</resultCode><resultMsg>정상</resultMsg></RESULT></head>{rows}</StanReginCd>"
    )


def legal_dong_response_body_row(sgg_cd: str = "110", umd_cd: str = "000", ri_cd: str = "00") -> str:
    return (
        f"<row><region_cd>1111000000</region_cd><sgg_cd>{sgg_cd}</sgg_cd>"
        f"<umd_cd>{umd_cd}</umd_cd><ri_cd>{ri_cd}</ri_cd></row>"
    )


def responder(*bodies: str, status_code: int = 200) -> Any:
    """호출 순서대로 body를 돌려주는 핸들러. 마지막 body는 그 뒤로도 계속 쓰인다."""
    remaining = list(bodies)

    def handle(_: httpx.Request) -> httpx.Response:
        body = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        return httpx.Response(status_code, text=body)

    return handle


async def test_rtms_collects_items_and_strips_text(mock_api: Any) -> None:
    requests = mock_api(responder(rtms_response_body(items=rtms_response_body_item("청운현대"), total_count=1)))

    rows = await RtmsDataCollector("https://api.test/rtms", "11110", "202602").collect()

    assert rows == [{"sggCd": "11110", "aptNm": "청운현대", "dealAmount": "36,900"}]
    assert requests[0].url.params["LAWD_CD"] == "11110"
    assert requests[0].url.params["DEAL_YMD"] == "202602"


async def test_rtms_treats_no_data_as_empty(mock_api: Any) -> None:
    # 해당 시군구·계약년월에 거래가 없는 달은 정상이다.
    mock_api(responder(rtms_response_body(result_code="03")))

    assert await RtmsDataCollector("https://api.test/rtms", "11110", "202602").collect() == []


@pytest.mark.parametrize(
    ("result_code", "expected"),
    [("22", DailyLimitReachedError), ("01", OpenApiError), ("04", OpenApiError)],
)
async def test_rtms_raises_on_error_codes(mock_api: Any, result_code: str, expected: type[Exception]) -> None:
    mock_api(responder(rtms_response_body(result_code=result_code)))

    with pytest.raises(expected):
        await RtmsDataCollector("https://api.test/rtms", "11110", "202602").collect()


async def test_rtms_follows_pagination(mock_api: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RtmsDataCollector, "MAX_ROWS_PER_PAGE", 2)
    requests = mock_api(
        responder(
            rtms_response_body(items=rtms_response_body_item("1동") + rtms_response_body_item("2동"), total_count=3),
            rtms_response_body(items=rtms_response_body_item("3동"), total_count=3),
        )
    )

    rows = await RtmsDataCollector("https://api.test/rtms", "11110", "202602").collect()

    assert [row["aptNm"] for row in rows] == ["1동", "2동", "3동"]
    assert [request.url.params["pageNo"] for request in requests] == ["1", "2"]


async def test_http_error_does_not_leak_service_key(mock_api: Any) -> None:
    mock_api(responder("", status_code=500))

    with pytest.raises(OpenApiStatusError) as error:
        await RtmsDataCollector("https://api.test/rtms", "11110", "202602").collect()

    assert error.value.status_code == 500
    # httpx의 메시지에는 인증키가 실린 요청 URL이 들어 있다. 메시지도 __cause__도 끊겨야 한다.
    assert SERVICE_KEY not in str(error.value)
    assert error.value.__cause__ is None


@pytest.mark.parametrize(
    ("sgg_cd", "umd_cd", "ri_cd"),
    [("000", "000", "00"), ("110", "101", "00"), ("110", "000", "01")],
)
async def test_legal_dong_keeps_only_sigungu_rows(mock_api: Any, sgg_cd: str, umd_cd: str, ri_cd: str) -> None:
    kept = legal_dong_response_body_row()
    dropped = legal_dong_response_body_row(sgg_cd, umd_cd, ri_cd)
    mock_api(responder(legal_dong_response_body(kept + dropped, total_count=2)))

    rows = await LegalDongCodeCollector().collect()

    assert [row["sgg_cd"] for row in rows] == ["110"]


async def test_legal_dong_rejects_empty_result(mock_api: Any) -> None:
    # 호출부가 이 목록으로 표를 통째로 갈아끼운다. 빈 목록을 통과시키면 시군구 목록이 사라진다.
    mock_api(responder(legal_dong_response_body(legal_dong_response_body_row(sgg_cd="000"), total_count=1)))

    with pytest.raises(OpenApiError, match="시군구 행이 없다"):
        await LegalDongCodeCollector().collect()


async def test_legal_dong_reads_error_without_head(mock_api: Any) -> None:
    # 에러 응답은 head 없이 최상위에 resultCode/resultMsg만 담겨 온다.
    mock_api(responder("<result><resultCode>ERROR-300</resultCode><resultMsg>필수값 누락</resultMsg></result>"))

    with pytest.raises(OpenApiError, match="ERROR-300"):
        await LegalDongCodeCollector().collect()
