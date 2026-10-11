# 공식 이미지에 DAG 태스크가 쓰는 패키지를 더한다. 버전은 pyproject.toml의 고정 버전과 맞춘다.
FROM apache/airflow:3.3.2-python3.14

# apache-airflow를 함께 적어야 추가 패키지의 의존성 해석이 이미지의 Airflow를 다른 버전으로 바꾸지 못한다.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "defusedxml==0.7.1"
