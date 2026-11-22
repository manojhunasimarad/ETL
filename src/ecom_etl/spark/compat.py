"""Small differences between Spark engines, kept in one place.

Databricks serverless compute (the only compute in Databricks Free Edition)
refuses ``persist()``: caching is managed by the platform there. The pipeline
only caches to avoid recomputing a DataFrame it reads twice, so it can run
without it. Set ``ECOM_NO_CACHE=1`` to switch caching off; the Databricks
notebook does.
"""

from __future__ import annotations

import os


def caching_enabled() -> bool:
    return os.environ.get("ECOM_NO_CACHE", "") not in ("1", "true", "yes")


def persist(df):
    """``df.persist()`` where the engine allows it, otherwise ``df`` unchanged."""
    if not caching_enabled():
        return df
    try:
        return df.persist()
    except Exception:  # an engine that does not support caching
        return df


def unpersist(df) -> None:
    if caching_enabled():
        try:
            df.unpersist()
        except Exception:
            pass
