# Airflow 3 + Java 17 + this project (PySpark runs inside the Airflow tasks).
FROM apache/airflow:3.1.7-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/jars/postgresql.jar
RUN chmod 644 /opt/jars/postgresql.jar && mkdir -p /opt/ecom && chown -R airflow:0 /opt/ecom

USER airflow
COPY --chown=airflow:0 pyproject.toml README.md /opt/ecom/
COPY --chown=airflow:0 src /opt/ecom/src
# pin Airflow so installing the project cannot upgrade or downgrade it
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -e "/opt/ecom[spark,postgres]"

ENV ECOM_SPARK_JARS=/opt/jars/postgresql.jar \
    ECOM_SPARK_DRIVER_MEMORY=2g
