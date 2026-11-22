FROM python:3.14-slim

# uv는 이미지에서 그대로 복사한다. pip로 설치하면 빌드마다 인터프리터 환경을 건드린다.
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PUBLIC_DIR=/app/public

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-build

COPY src ./src
# 웹 페이지는 저장소 루트의 public/에 있어 빌드 컨텍스트(api/) 밖이다. 이름 붙인 컨텍스트로 받는다.
#   docker build --build-context public=public api
# 쿠버네티스에는 compose처럼 호스트 디렉터리를 마운트할 곳이 없어 이미지에 넣어야 한다.
# hadolint ignore=DL3022
COPY --from=public . ./public

# 컨테이너를 root로 돌리면 탈출 시 호스트 권한까지 넘어간다.
RUN useradd --system --no-create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
