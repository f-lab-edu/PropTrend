from fastapi import status


class PropTrendError(Exception):
    """애플리케이션 예외의 기반. 전역 핸들러가 status_code로 응답한다."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class InvalidQueryError(PropTrendError):
    """조회 조건의 조합이 잘못됐다."""

    status_code = status.HTTP_400_BAD_REQUEST


class InvalidApiKeyError(PropTrendError):
    """X-API-KEY 헤더가 없거나 일치하지 않는다."""

    status_code = status.HTTP_401_UNAUTHORIZED
