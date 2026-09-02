FROM python:3.12.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "feast==0.66.0" "redis==6.4.0"
WORKDIR /workspace/feature-repo

EXPOSE 6566 8000
