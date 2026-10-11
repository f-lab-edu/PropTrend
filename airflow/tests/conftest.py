"""DAG 테스트가 Airflow를 import하기 전에 갖춰야 할 환경."""

import os
import tempfile

# Airflow는 import 시점에 AIRFLOW_HOME에 설정 파일과 메타 DB를 만든다. 저장소나 홈 디렉터리에 남기지 않는다.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
# DAG가 파싱 시점에 읽는 비밀값. 실제 값이 아니어도 파싱은 된다.
os.environ.setdefault("PROP_TREND_DATABASE_URL", "postgresql+asyncpg://test:test@postgres:5432/test")
os.environ.setdefault("DATA_GO_KR_SERVICE_KEY", "test-service-key")
