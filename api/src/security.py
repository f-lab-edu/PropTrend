"""갱신 API를 보호하는 X-API-Key 인증."""

import hmac
import os

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

API_KEY_ENV = "REFRESH_API_KEY"

# auto_error=False로 두고 직접 401을 만든다. 헤더 누락과 불일치를 같은 응답으로 묶기 위해서다.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(api_key: str | None = Security(_api_key_header)) -> None:
    """`REFRESH_API_KEY`와 같은 `X-API-Key` 헤더가 없으면 요청을 막는다."""
    # 임포트 시점이 아니라 요청 시점에 읽는다. main.py의 load_dotenv()보다 이 모듈의
    # 임포트가 먼저 끝날 수 있고, db.py의 지연 읽기 관례와도 같다.
    expected = os.environ.get(API_KEY_ENV, "")
    if not expected:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            f"{API_KEY_ENV}가 설정되지 않아 갱신 API가 잠겨 있다",
        )
    # str끼리 비교하면 비ASCII 헤더에서 TypeError가 난다. Starlette은 헤더를 latin-1로
    # 디코드하므로 헤더 한 글자로 500을 만들 수 있다. bytes 비교에는 그 함정이 없다.
    if api_key is None or not hmac.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "유효한 X-API-Key가 필요하다")
