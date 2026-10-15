# 공식 이미지에 DAG 태스크가 쓰는 패키지를 더한다. 버전은 pyproject.toml의 고정 버전과 맞춘다.
FROM apache/airflow:3.3.2-python3.14

# 공식 이미지의 기본 사용자와 같다. 명시해 두면 root로 패키지를 설치하거나 실행하지 않는다는 것이 드러난다.
USER airflow

# apache-airflow를 함께 적어야 추가 패키지의 의존성 해석이 이미지의 Airflow를 다른 버전으로 바꾸지 못한다.
# wheel만 받아 패키지의 설치 스크립트(setup.py)가 빌드 중에 돌지 않게 한다.
RUN pip install --no-cache-dir --only-binary :all: "apache-airflow==3.3.2" "defusedxml==0.7.1"
