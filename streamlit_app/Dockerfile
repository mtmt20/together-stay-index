# Together-Stay Index 전용 Streamlit 이미지. 위스키 프로젝트의 Streamlit
# 이미지/컨테이너와 완전히 분리된 별도 빌드이다.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
