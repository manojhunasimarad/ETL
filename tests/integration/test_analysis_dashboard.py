"""Step 6: every business question runs, and the dashboard renders without errors."""

from pathlib import Path

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.postgres]

DASHBOARD = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"


@pytest.fixture(scope="module")
def loaded(spark, dirty_lake, dirty_source):
    from ecom_etl.spark.pipeline import run_quality
    from ecom_etl.spark.warehouse import load_gold

    out, manifest = dirty_source
    lake, *_ = dirty_lake
    load_gold(spark, lake, "t_dash")
    run_quality(spark, lake, "t_dash", out, manifest["extracted_at"])
    return "t_dash"


def test_every_question_returns_rows(loaded):
    from ecom_etl.analysis import render_markdown, run_analyses

    analyses = run_analyses(loaded)
    assert len(analyses) == 10
    for a in analyses:
        assert a.rows, a.name
        assert a.question.startswith("Q") and a.technique, a.name
    assert "## Q1." in render_markdown(loaded, analyses)


def test_monthly_revenue_matches_the_fact_table(loaded):
    from ecom_etl.analysis import run_analyses
    from ecom_etl.db import connect, schema_name

    q1 = next(a for a in run_analyses(loaded) if a.name.startswith("q01"))
    total = sum(row[q1.columns.index("revenue")] for row in q1.rows)
    with connect() as conn:
        fact = conn.execute(f"SELECT round(sum(line_total), 2) FROM {schema_name(loaded, 'gold')}.fact_order_line "
                            "WHERE order_status NOT IN ('canceled', 'unavailable')").fetchone()[0]
    assert abs(total - fact) <= 0.01 * len(q1.rows)  # per-month rounding


def test_dashboard_renders(loaded, monkeypatch):
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("ECOM_DASHBOARD_SOURCE", loaded)
    app = AppTest.from_file(str(DASHBOARD), default_timeout=120)
    app.run()
    assert not app.exception, app.exception
    assert len(app.metric) >= 4
    assert any(loaded in t.value for t in app.title)
