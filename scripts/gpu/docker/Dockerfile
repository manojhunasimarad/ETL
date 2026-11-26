# Java 17 + Python 3.12 + PySpark for the GPU benchmark. Build context: the repo root.
FROM nvidia/cuda:12.9.1-base-ubuntu24.04

# The base image asks for all GPUs; with NVIDIA's runtime as Docker's default that runs the
# GPU hook in every build step, which fails on this server. GPUs are requested at run time.
ENV NVIDIA_VISIBLE_DEVICES=void \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless python3.12 python3.12-venv procps tini \
    && rm -rf /var/lib/apt/lists/*

RUN python3.12 -m venv /opt/ecom-venv
ENV PATH="/opt/ecom-venv/bin:${JAVA_HOME}/bin:${PATH}"

# Only what pip needs, so no data, reports or .git end up in the image.
# Non-editable install: -e would point at /tmp/build, deleted below.
# PySpark pinned to the version src/ecom_etl/spark/gpu.py pins for RAPIDS 26.10.
ARG PYSPARK_VERSION=4.1.3
COPY pyproject.toml README.md /tmp/build/
COPY src /tmp/build/src
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "/tmp/build[spark]" "pyspark==${PYSPARK_VERSION}" \
    && rm -rf /tmp/build

# The repo mounted at run time wins over the installed copy: code edits need no rebuild.
ENV PYTHONPATH=/home/manoj/ETL/src
WORKDIR /home/manoj/ETL

ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "ecom_etl"]
CMD ["--help"]
