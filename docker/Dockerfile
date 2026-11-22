# Pipeline image: Python 3.11 + Java 17 + PySpark + this package.
# Used by `docker compose run --rm pipeline <command>`, the dashboard and Airflow (step 8).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[spark,postgres,dashboard,streaming]"
COPY dashboard ./dashboard

# Postgres JDBC driver baked in: runs need no internet access to Maven
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/jars/postgresql.jar

# Fetch the Spark Kafka connector once at build time (Ivy cache), so streaming
# jobs start without internet access
RUN python -c "from ecom_etl.spark.session import get_spark; from ecom_etl.spark.streaming import kafka_package; get_spark('cache', packages=[kafka_package()]).stop()"

ENV PYTHONUNBUFFERED=1 \
    ECOM_SPARK_DRIVER_MEMORY=2g \
    ECOM_SPARK_JARS=/opt/jars/postgresql.jar

ENTRYPOINT ["python", "-m", "ecom_etl"]
CMD ["--help"]
