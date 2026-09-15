# Together-Stay Index 전용 Airflow 이미지.
# 기존 위스키 파이프라인의 커스텀 Airflow 이미지를 재사용하지 않고
# 공식 apache/airflow 베이스에서 완전히 새로 빌드한다 - 이미지 레이어까지
# 독립시켜야 "기존 것과 절대 충돌 없이" 라는 요구를 만족할 수 있다.
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
