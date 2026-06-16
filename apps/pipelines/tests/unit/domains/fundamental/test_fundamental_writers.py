"""Tests for fundamental Bronze writer fan-out."""

from datetime import date
from types import SimpleNamespace
from typing import cast

import pytest

from core.ingestion import BronzeParseResult, BronzeWrite
from domains.fundamental import writers
from domains.fundamental.models import FundamentalDocument


def test_write_fundamental_bronze_slices_records_one_aggregate_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fundamental slices should produce one table-group asset materialization."""
    record_calls: list[dict[str, object]] = []
    write_results = {
        "write_bronze_fundamental_document": BronzeWrite(rows_written=2),
        "write_bronze_fundamental_stock_identity": BronzeWrite(rows_written=0, reason="already_ingested"),
        "write_bronze_fundamental_statement_facts": BronzeWrite(rows_written=0, reason="no_facts"),
        "write_bronze_fundamental_stock_earnings_facts": BronzeWrite(rows_written=0, reason="no_earnings_facts"),
        "write_bronze_fundamental_stock_shares_stats": BronzeWrite(rows_written=0, reason="no_shares_stats"),
        "write_bronze_fundamental_stock_outstanding_shares": BronzeWrite(
            rows_written=0,
            reason="no_outstanding_shares",
        ),
        "write_bronze_fundamental_stock_holders": BronzeWrite(rows_written=0, reason="no_holders"),
        "write_bronze_fundamental_stock_insider_transactions": BronzeWrite(
            rows_written=0,
            reason="no_insider_transactions",
        ),
        "write_bronze_fundamental_stock_metric_facts": BronzeWrite(rows_written=0, reason="no_metric_facts"),
        "write_bronze_fundamental_etf_identity": BronzeWrite(rows_written=0, reason="not_etf"),
        "write_bronze_fundamental_mutual_fund_identity": BronzeWrite(rows_written=0, reason="not_mutual_fund"),
        "write_bronze_fundamental_index_identity": BronzeWrite(rows_written=0, reason="not_index"),
        "write_bronze_fundamental_etf_holdings": BronzeWrite(rows_written=0, reason="no_etf_holdings"),
        "write_bronze_fundamental_mutual_fund_holdings": BronzeWrite(
            rows_written=0,
            reason="no_mutual_fund_holdings",
        ),
        "write_bronze_fundamental_fund_metric_facts": BronzeWrite(
            rows_written=0,
            reason="no_fund_metric_facts",
        ),
        "write_bronze_fundamental_index_components": BronzeWrite(rows_written=0, reason="no_index_components"),
    }

    for name, result in write_results.items():
        monkeypatch.setattr(writers, name, lambda *_args, _result=result, **_kwargs: _result)
    monkeypatch.setattr(
        writers,
        "record_fundamental_bronze_materialization",
        lambda **metadata: record_calls.append(metadata),
    )

    document = cast(
        BronzeParseResult[FundamentalDocument],
        SimpleNamespace(row=SimpleNamespace(provider_exchange_code="US")),
    )

    result = writers.write_fundamental_bronze_slices(
        document=document,
        identity=None,
        statement_facts=[],
        earnings_facts=[],
        shares_stats=None,
        outstanding_shares=[],
        holders=[],
        insider_transactions=[],
        metric_facts=[],
        etf_identity=None,
        mutual_fund_identity=None,
        index_identity=None,
        etf_holdings=[],
        mutual_fund_holdings=[],
        fund_metric_facts=[],
        index_components=[],
        provider_instrument_code="AAPL",
        snapshot_date=date(2026, 6, 15),
        source_uri="s3://lake/fundamental/AAPL.json",
    )

    assert result.rows_written == 2
    assert len(record_calls) == 1
    assert record_calls[0]["rows_written"] == 2
    assert record_calls[0]["provider_exchange_code"] == "US"
    assert record_calls[0]["provider_instrument_code"] == "AAPL"
    assert record_calls[0]["slices_changed"] == ["fundamental_document"]
    assert record_calls[0]["slices_unchanged"] == ["fundamental_stock_identity"]
    slices_not_applicable = cast(list[str], record_calls[0]["slices_not_applicable"])
    assert "fundamental_statement_fact" in slices_not_applicable
