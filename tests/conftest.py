import pytest

from ecom_etl.generator import GeneratorConfig, generate


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
