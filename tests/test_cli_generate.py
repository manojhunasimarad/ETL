"""generate refuses bad settings with one line, not a traceback (review fix F14)."""
import pytest

from ecom_etl.cli import build_parser, main


@pytest.mark.parametrize("value", ["0.2", "-0.01", "lots"])
def test_bad_defect_rate_is_one_line_error(value, capsys):
    with pytest.raises(SystemExit) as exit_info:
        build_parser().parse_args(["generate", "--defect-rate", value])
    assert exit_info.value.code == 2
    err = capsys.readouterr().err
    assert "--defect-rate" in err and "Traceback" not in err


def test_defect_rate_limits_are_accepted():
    for value in ("0", "0.01", "0.05"):
        assert build_parser().parse_args(["generate", "--defect-rate", value]).defect_rate == float(value)


def test_orders_must_be_positive_and_may_use_separators(capsys):
    assert build_parser().parse_args(["generate", "--orders", "20,000"]).orders == 20000
    with pytest.raises(SystemExit):
        build_parser().parse_args(["generate", "--orders", "0"])
    assert "at least 1" in capsys.readouterr().err


def test_generator_refusal_is_reported_without_traceback(tmp_path, capsys):
    code = main(["generate", "--orders", "50", "--start", "2018-02-01", "--end", "2017-01-01",
                 "--out", str(tmp_path / "out")])
    err = capsys.readouterr().err
    assert code == 2 and err.startswith("generate: ")
