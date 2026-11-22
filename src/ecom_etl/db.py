"""Postgres connection settings and small helpers.

Settings come from environment variables so the same code runs on your
laptop (``localhost``), inside Docker Compose (``postgres``) and in CI.
Defaults match ``docker-compose.yml``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from importlib import resources


@dataclass(frozen=True)
class DbSettings:
    host: str = "localhost"
    port: int = 5432
    user: str = "ecom"
    password: str = "ecom"
    dbname: str = "ecom"

    @classmethod
    def from_env(cls) -> "DbSettings":
        return cls(
            host=os.environ.get("ECOM_DB_HOST", cls.host),
            port=int(os.environ.get("ECOM_DB_PORT", cls.port)),
            user=os.environ.get("ECOM_DB_USER", cls.user),
            password=os.environ.get("ECOM_DB_PASSWORD", cls.password),
            dbname=os.environ.get("ECOM_DB_NAME", cls.dbname),
        )

    def conninfo(self) -> str:
        return (f"host={self.host} port={self.port} user={self.user} "
                f"password={self.password} dbname={self.dbname}")

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"


def connect(settings: DbSettings | None = None, autocommit: bool = False):
    import psycopg  # imported lazily: only steps that touch Postgres need it

    return psycopg.connect((settings or DbSettings.from_env()).conninfo(), autocommit=autocommit)


def schema_name(source: str, layer: str) -> str:
    """One schema per source and layer, e.g. ``olist_raw``, ``synthetic_gold``."""
    safe = re.sub(r"[^a-z0-9_]", "_", source.lower())
    return f"{safe}_{layer}"


def read_sql(name: str, **placeholders: str) -> str:
    """Load ``ecom_etl/sql/<name>`` and replace ``{key}`` placeholders."""
    text = resources.files("ecom_etl").joinpath("sql", name).read_text(encoding="utf-8")
    for key, value in placeholders.items():
        text = text.replace("{" + key + "}", value)
    return text
