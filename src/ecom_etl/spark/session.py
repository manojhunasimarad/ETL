"""One place to build the SparkSession, so every job uses the same settings.

Environment variables let the same code run on a laptop, in Docker, in CI
and (step 10) on Databricks, where the session already exists.
"""

from __future__ import annotations

import os
from typing import Dict, Iterable, Optional

DEFAULTS = {
    # The data has no time zone; treat every timestamp as UTC wall-clock time
    # so nothing shifts when the machine is in IST.
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": os.environ.get("ECOM_SPARK_SHUFFLE_PARTITIONS", "16"),
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.memory": os.environ.get("ECOM_SPARK_DRIVER_MEMORY", "2g"),
    "spark.ui.showConsoleProgress": "false",
    # JDBC writes convert timestamps with the JVM time zone, not the session
    # one; pin it too, or Postgres would get IST-shifted times on an IST laptop.
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
}


def get_spark(app_name: str = "ecom_etl", conf: Optional[Dict[str, str]] = None,
              packages: Iterable[str] = ()):
    """Create (or reuse) the SparkSession.

    ``conf`` overrides the defaults. ``packages`` are Maven coordinates
    downloaded at start-up (e.g. the Kafka connector in step 9).
    ``ECOM_SPARK_JARS`` lists local jar files (the Docker image bakes the
    Postgres JDBC driver in, so runs do not need internet access).
    """
    from pyspark.sql import SparkSession

    settings = dict(DEFAULTS)
    jars = os.environ.get("ECOM_SPARK_JARS")
    if jars:
        settings["spark.jars"] = jars
    packages = list(packages)
    if packages:
        settings["spark.jars.packages"] = ",".join(packages)
    settings.update(conf or {})

    builder = SparkSession.builder.appName(app_name).master(os.environ.get("ECOM_SPARK_MASTER", "local[*]"))
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("ECOM_SPARK_LOG_LEVEL", "WARN"))
    return spark
