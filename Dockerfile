# Together-Stay Index 전용 Airflow 이미지.
# 기존 위스키 파이프라인의 커스텀 Airflow 이미지를 재사용하지 않고
# 공식 apache/airflow 베이스에서 완전히 새로 빌드한다 - 이미지 레이어까지
# 독립시켜야 "기존 것과 절대 충돌 없이" 라는 요구를 만족할 수 있다.
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt

# Airflow 코어/provider와 얽힌 의존성 그래프를 pip 혼자 풀게 두면 버전
# 조합 폭발(ResolutionTooDeep)이 난다. Airflow가 매 릴리스마다 검증해서
# 공개하는 공식 constraints 파일을 같이 넘겨 호환 버전으로 강제한다.
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
