"""Tests for the operational health CLI adapter."""

from typing import Any

from core.infrastructure.health.operational import OperationalHealthConfig, OperationalHealthResult
from scripts.health import check_operational


def test_configured_recent_domains_uses_cli_values(monkeypatch: Any) -> None:
    """CLI domains should override monitor environment defaults."""
    monkeypatch.setenv("OPERATIONAL_HEALTH_RECENT_DOMAINS", "fundamental")

    domains = check_operational.configured_recent_domains([" eod_price ", ""])

    assert domains == ["eod_price"]


def test_configured_recent_domains_uses_environment(monkeypatch: Any) -> None:
    """The deployed healthcheck can configure freshness domains through env vars."""
    monkeypatch.setenv("OPERATIONAL_HEALTH_RECENT_DOMAINS", "eod_price, fundamental exchange")

    domains = check_operational.configured_recent_domains(None)

    assert domains == ["eod_price", "fundamental", "exchange"]


def test_operational_lake_read_only_defaults_to_false_for_motherduck_token() -> None:
    """Regular MotherDuck tokens cannot be opened with DuckDB read_only=True."""
    assert check_operational.operational_lake_read_only(motherduck_token="token") is False


def test_operational_lake_read_only_auto_defaults_to_true_without_motherduck_token() -> None:
    """Local DuckDB can be opened read-only by default."""
    assert check_operational.operational_lake_read_only("auto", motherduck_token="") is True


def test_operational_lake_read_only_can_be_forced_for_read_scaling_token() -> None:
    """Operators with a read-scaling token can force a read-only MotherDuck connection."""
    assert check_operational.operational_lake_read_only("true", motherduck_token="token") is True


def test_build_parser_preserves_stale_running_default(monkeypatch: Any) -> None:
    """The CLI should keep the historical stale-running threshold by default."""
    monkeypatch.delenv("OPERATIONAL_HEALTH_STALE_RUNNING_HOURS", raising=False)

    args = check_operational.build_parser().parse_args([])

    assert args.stale_running_hours == 2.0


def test_main_builds_config_and_prints_failures(monkeypatch: Any, capsys: Any) -> None:
    """CLI parsing should feed the core runner and turn failures into exit code 1."""
    configs: list[OperationalHealthConfig] = []

    def fake_run(config: OperationalHealthConfig) -> OperationalHealthResult:
        configs.append(config)
        return OperationalHealthResult(failures=["1 stale running pipeline run(s)"])

    monkeypatch.setattr(check_operational, "run_operational_health", fake_run)

    exit_code = check_operational.main(
        [
            "--prefect-api-url",
            "http://prefect.example/api",
            "--stale-running-hours",
            "1.5",
            "--recent-domain",
            "eod_price",
            "--lake-read-only",
            "true",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "FAIL: 1 stale running pipeline run(s)" in captured.err
    assert configs == [
        OperationalHealthConfig(
            prefect_api_url="http://prefect.example/api",
            stale_running_hours=1.5,
            recent_domains=["eod_price"],
            recent_hours=36.0,
            lake_read_only=True,
        )
    ]
