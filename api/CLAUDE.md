# Project instructions

## Tech Stack

- Python 3.14
- FastAPI
- PostgreSQL + SQLAlchemy
- pytest
- ruff
- uv

## Commands

- 의존성 설치: `uv add`
- 린트: `ruff check .`
- 포맷: `ruff format .`

## Project structure

## Coding conventions

- 코드 스타일은 기본적으로 PEP 8 기준
- 모든 함수, 메서드의 인자와 반환값에 대한 타입 힌트 필수
- 코드 한 줄의 최대 길이는 120
- 커밋 전 `ruff format .`, `ruff check .` 통과 필수
