import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for all Spark tests (small settings, fast start)."""
    from ecom_etl.spark.session import get_spark

    session = get_spark("ecom-tests", conf={"spark.sql.shuffle.partitions": "4",
                                            "spark.default.parallelism": "4"})
    yield session
    session.stop()
