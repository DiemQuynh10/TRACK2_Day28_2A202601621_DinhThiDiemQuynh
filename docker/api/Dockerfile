FROM python:3.12.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[retrieval,ml]"

EXPOSE 8000
CMD ["lab28", "serve", "--host", "0.0.0.0", "--port", "8000"]
