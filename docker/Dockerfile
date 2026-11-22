# Pipeline image: Python 3.11 + Java 17 + PySpark + this package.
# Used by `docker compose run --rm pipeline <command>` and by Airflow (step 8).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[spark,postgres]"

ENV PYTHONUNBUFFERED=1 \
    ECOM_SPARK_DRIVER_MEMORY=2g

ENTRYPOINT ["python", "-m", "ecom_etl"]
CMD ["--help"]
