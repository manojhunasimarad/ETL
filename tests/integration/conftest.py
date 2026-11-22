import os

import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for all Spark tests (small settings, fast start).

    The Postgres JDBC driver is fetched from Maven unless ECOM_SPARK_JARS
    already points at a local jar (as in the Docker image).
    """
    from ecom_etl.spark.session import get_spark
    from ecom_etl.spark.warehouse import PG_JDBC_PACKAGE

    packages = () if os.environ.get("ECOM_SPARK_JARS") else (PG_JDBC_PACKAGE,)
    session = get_spark("ecom-tests", conf={"spark.sql.shuffle.partitions": "4",
                                            "spark.default.parallelism": "4"}, packages=packages)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def dirty_lake(spark, dirty_source, tmp_path_factory):
    """Bronze + silver + gold for the dirty source, shared by step 4+ tests."""
    from ecom_etl.spark.bronze import run_bronze
    from ecom_etl.spark.gold import run_gold
    from ecom_etl.spark.lake import Lake
    from ecom_etl.spark.silver import run_silver

    out, manifest = dirty_source
    lake = Lake(tmp_path_factory.mktemp("lake_shared"))
    run_bronze(spark, out, lake, "test-run")
    silver = run_silver(spark, lake, manifest["extracted_at"])
    gold = run_gold(spark, lake)
    return lake, silver, gold
