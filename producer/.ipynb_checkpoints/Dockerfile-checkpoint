FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt update && \
    apt install -y python-dev-is-python3 default-libmysqlclient-dev build-essential pkg-config

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src/ .

CMD bash