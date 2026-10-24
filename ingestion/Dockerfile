FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SQL_DIR=/app/sql \
    PYTHONPATH=/app

WORKDIR /app

COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/polymarket_pipeline ./polymarket_pipeline
COPY ingestion/tests ./tests
COPY transformations/sql ./sql

RUN useradd --create-home --uid 10001 pipeline
USER pipeline

CMD ["python", "-m", "polymarket_pipeline", "--help"]
