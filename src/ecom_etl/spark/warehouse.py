"""Load the gold layer into Postgres (schema ``<source>_gold``) over JDBC.

The schema is dropped and recreated on every load (full refresh), so a
re-run gives the same result. Spark creates the tables; ``gold_constraints.sql``
then adds primary keys, foreign keys and an analyst-friendly view, and
``gold_indexes.sql`` adds the secondary indexes measured in step 7.
"""

from __future__ import annotations

from typing import Dict, Optional

from ..db import DbSettings, connect, read_sql, schema_name
from .gold import GOLD_TABLES
from .lake import Lake

PG_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def jdbc_options(settings: DbSettings) -> Dict[str, str]:
    return {
        "url": settings.jdbc_url() + "?reWriteBatchedInserts=true",
        "user": settings.user,
        "password": settings.password,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
    }


def load_gold(spark, lake: Lake, source: str, settings: Optional[DbSettings] = None) -> Dict[str, int]:
    settings = settings or DbSettings.from_env()
    schema = schema_name(source, "gold")
    with connect(settings) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        conn.commit()

    options = jdbc_options(settings)
    for table in GOLD_TABLES:
        df = spark.read.parquet(lake.path("gold", table))
        (df.write.format("jdbc").options(**options)
           .option("dbtable", f"{schema}.{table}")
           .mode("overwrite").save())

    counts = {}
    with connect(settings) as conn:
        conn.execute(read_sql("gold_constraints.sql", gold=schema))
        conn.execute(read_sql("gold_indexes.sql", gold=schema))
        for table in GOLD_TABLES:
            counts[table] = conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
        conn.commit()
    return counts
