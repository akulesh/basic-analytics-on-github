FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src/dashboard/requirements.txt /app
RUN pip install -r requirements.txt

COPY src/dashboard /app/src/dashboard
COPY src/utils /app/src/utils

ENV PYTHONPATH=/app
EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "src/dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]