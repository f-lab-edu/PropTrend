from fastapi import status


class PropTrendError(Exception):
    """애플리케이션 예외의 기반. 전역 핸들러가 status_code로 응답한다."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class InvalidCredentialsError(PropTrendError):
    """로그인 이메일 또는 비밀번호가 일치하지 않는다."""

    status_code = status.HTTP_401_UNAUTHORIZED


class DuplicateEmailError(PropTrendError):
    """이미 가입된 이메일로 회원가입했다."""

    status_code = status.HTTP_409_CONFLICT


class UnauthenticatedError(PropTrendError):
    """세션 쿠키가 없거나 만료·무효한 세션이다."""

    status_code = status.HTTP_401_UNAUTHORIZED


class TransactionNotFoundError(PropTrendError):
    """요청한 id의 실거래가 없다."""

    status_code = status.HTTP_404_NOT_FOUND


class ComplexNotFoundError(PropTrendError):
    """요청한 id의 단지가 없다."""

    status_code = status.HTTP_404_NOT_FOUND
