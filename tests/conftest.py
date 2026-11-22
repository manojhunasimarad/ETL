import importlib.util
import os
import socket

import pytest

from ecom_etl.generator import GeneratorConfig, generate

# Tests that need infrastructure are marked @pytest.mark.postgres / spark / kafka.
# Locally they are skipped when the service is not available. In CI,
# ECOM_REQUIRE_INFRA lists the services that must exist, so a missing
# service fails the run instead of silently skipping tests.


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _postgres_missing():
    if importlib.util.find_spec("psycopg") is None:
        return 'psycopg not installed (pip install -e ".[postgres]")'
    from ecom_etl.db import DbSettings

    s = DbSettings.from_env()
    if not _reachable(s.host, s.port):
        return f"Postgres not reachable at {s.host}:{s.port} (docker compose up -d postgres)"
    return None


def _spark_missing():
    if importlib.util.find_spec("pyspark") is None:
        return 'pyspark not installed (pip install -e ".[spark]")'
    return None


def _kafka_missing():
    if importlib.util.find_spec("confluent_kafka") is None:
        return 'confluent-kafka not installed (pip install -e ".[streaming]")'
    host, _, port = os.environ.get("ECOM_KAFKA_BOOTSTRAP", "localhost:9092").partition(":")
    if not _reachable(host, int(port or 9092)):
        return "Kafka not reachable (docker compose --profile streaming up -d kafka)"
    return None


INFRA = {"postgres": _postgres_missing, "spark": _spark_missing, "kafka": _kafka_missing}


def pytest_collection_modifyitems(config, items):
    required = {s.strip() for s in os.environ.get("ECOM_REQUIRE_INFRA", "").split(",") if s.strip()}
    missing = {name: check() for name, check in INFRA.items()}
    for name in required:
        if missing.get(name):
            raise pytest.UsageError(f"ECOM_REQUIRE_INFRA includes {name}, but: {missing[name]}")
    for item in items:
        for name, reason in missing.items():
            if reason and item.get_closest_marker(name):
                item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture(scope="session")
def dirty_source(tmp_path_factory):
    """3,000 orders with a 2% defect rate per defect type."""
    out = tmp_path_factory.mktemp("dirty")
    manifest = generate(GeneratorConfig(orders=3000, seed=7, defect_rate=0.02), out)
    return out, manifest


@pytest.fixture(scope="session")
def clean_source(tmp_path_factory):
    """Same seed, no defects at all."""
    out = tmp_path_factory.mktemp("clean")
    manifest = generate(GeneratorConfig(orders=3000, seed=7, defect_rate=0.0), out)
    return out, manifest
