# API instructions

## Layer instructions

- 데이터베이스 세션 객체를 통한 트랜잭션 관리는 라우터 함수 계층에서 `async with session.begin():`로 관리합니다.
- 서비스 함수는 `session`을 제외한 모든 매개변수를 `*` 뒤의 키워드 인자로 받고, Pydantic·ORM 인스턴스를 통째로 받지 않습니다. 라우터는 필요한 값을 꺼내 키워드 인자로 전달합니다.
- 단순 조회·생성 서비스는 ORM 객체를 반환하고, 여러 출처를 조합하는 서비스는 응답 스키마를 만들어 반환합니다.
- 예외 처리는 FastAPI exception_handler로 전역 관리합니다. 그러므로 특별히 요구된 상황 외에는 모든 예외를 라우터 계층 바깥으로 내보냅니다.
- 예외는 `src/exceptions.py`에 `PropTrendError`를 상속하고 `status_code`를 지정해 정의합니다. `HTTPException`은 쓰지 않으며, 예외 메시지는 사용자에게 그대로 보이는 한국어로 적습니다.

### Router instructions

- 라우트 함수 이름에는 `_route` 같은 접미사를 붙이지 않고, 가져다 쓰는 서비스 함수와 겹치지 않는 이름을 짓습니다. (예: 라우트 `get_region_list` → 서비스 `get_regions`)
- 응답 타입은 `response_model=` 대신 반환 타입 어노테이션으로 지정합니다. 서비스가 ORM 객체를 반환하면 라우터에서 `XxxResponse.model_validate(...)`로 바꿔 반환합니다.
- 의존성은 `Annotated[..., Depends(...)]`, 경로의 id는 `Annotated[int, Path(ge=1)]`로 받습니다. 쿼리 파라미터는 `XxxQuery` 모델 하나로 묶어 `Annotated[XxxQuery, Query()]`로 받습니다.
- `summary`는 한국어 한 줄, `description`은 한국어 요약 뒤에 빈 줄을 두고 `- ` 목록으로 필드 의미·단위·정렬 기준·null 조건과 각 오류 상황의 상태 코드("~이면 404를 반환합니다")를 적습니다. API 동작을 바꾸면 `description`도 함께 갱신합니다.
- 서비스 기준 날짜는 `dependencies.get_today()`(KST)를 주입받아 `base_date=`로 서비스에 넘깁니다. 코드에서 만드는 시각은 `datetime.now(UTC)`를 씁니다.

### Schema·Model instructions

- 스키마는 `PropTrendCoreModel`을 상속하고 이름은 `XxxRequest`, `XxxResponse`, `XxxListResponse`, `XxxQuery`로 짓습니다.
- 목록 응답은 `ListResponse`(`items`), limit·offset 페이지 응답은 `PageResponse`(`items`·`total`·`limit`·`offset`)를 상속합니다.
- 시각 컬럼은 `DateTime(timezone=True)`로 정의합니다.
- 마이그레이션 도구 없이 기동 시 `create_all`로 없는 테이블만 만듭니다. 모델의 컬럼·인덱스를 바꾸면 기존 DB에 적용할 `ALTER` 문을 함께 안내합니다.

### Test instructions

- FastAPI의 API는 1개의 성공 케이스와 각 예외 처리 상황에 대한 테스트가 필수적으로 있어야합니다.
- API 테스트를 작성 시 API마다 클래스 기반으로 스위트를 구분합니다.
- 시드 데이터는 `api/tests/api/conftest.py`의 `seed_rows`로 스위트 내부에서 넣고 정리합니다.
- API 테스트 파일은 `pytestmark = pytest.mark.asyncio(loop_scope="session")`를 두고, 클래스 시드 픽스처는 `@pytest_asyncio.fixture(scope="class", loop_scope="session")`로 선언합니다. 세션 범위 엔진과 이벤트 루프를 맞추기 위함입니다.
- API에 대한 업데이트 진행 시 테스트 코드 변경이 필요한 경우 즉시 진행하지 않고, 어떤 부분에 어떻게 변경이 필요한지 확인을 받은 후 진행합니다.
