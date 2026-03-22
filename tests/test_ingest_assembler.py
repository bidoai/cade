"""Tests for cade.ingest.assembler — pure Python, no external services."""
import pytest
from datetime import date
from cade.ingest.assembler import assemble
from cade.models import MatrixRef

COB = date(2024, 3, 15)


def _agreement(cp="ACME", ns="RATES-USD"):
    return {
        "counterparty_id": cp,
        "netting_set_id": ns,
        "agreement_id": "ISDA-001",
        "threshold_amount": 5_000_000.0,
        "minimum_transfer_amount": 500_000.0,
        "independent_amount": 0.0,
        "currency": "USD",
        "eligible_collateral": ["USD_CASH"],
        "rounding_amount": 0.0,
        "valuation_agent": None,
        "extra": {},
    }


def _trade(cp="ACME", ns="RATES-USD", trade_id="T-001"):
    return {
        "counterparty_id": cp,
        "netting_set_id": ns,
        "trade_id": trade_id,
        "product_type": "IRS",
        "notional": 10_000_000.0,
        "currency": "USD",
        "direction": "PAY",
        "maturity_date": None,
        "extra": {},
    }


def test_assemble_basic():
    agreements = [_agreement()]
    trades = [_trade(), _trade(trade_id="T-002")]
    market_data = {"fx_rates": {"USD/GBP": 0.79}, "inflation_rates": {"UK_RPI": 0.031}}
    snapshots, skipped = assemble(agreements, trades, market_data, {}, COB)
    assert len(snapshots) == 1
    assert len(skipped) == 0
    snap = snapshots[0]
    assert snap.counterparty_id == "ACME"
    assert snap.netting_set_id == "RATES-USD"
    assert len(snap.trades) == 2
    assert snap.market_data.fx_rates == {"USD/GBP": 0.79}
    assert snap.agreement.threshold_amount == 5_000_000.0


def test_assemble_no_trades():
    agreements = [_agreement()]
    snapshots, skipped = assemble(agreements, [], {}, {}, COB)
    assert len(snapshots) == 1
    assert snapshots[0].trades == []


def test_assemble_multiple_counterparties():
    agreements = [_agreement("ACME", "RATES-USD"), _agreement("FOO", "FX-EUR")]
    trades = [_trade("ACME", "RATES-USD"), _trade("FOO", "FX-EUR", "T-100")]
    snapshots, _ = assemble(agreements, trades, {}, {}, COB)
    assert len(snapshots) == 2
    cp_ids = {s.counterparty_id for s in snapshots}
    assert cp_ids == {"ACME", "FOO"}


def test_assemble_with_matrix_refs():
    agreements = [_agreement()]
    ref = MatrixRef(path="matrices/2024-03-15/ACME/IR_SWAP.npy", hash="sha256-v1:abc123")
    matrix_refs = {"ACME/RATES-USD": {"IR_SWAP": ref}}
    snapshots, _ = assemble(agreements, [], {}, matrix_refs, COB)
    assert "IR_SWAP" in snapshots[0].market_data.price_matrices
    assert snapshots[0].market_data.price_matrices["IR_SWAP"].hash == "sha256-v1:abc123"


def test_assemble_empty_agreements():
    snapshots, skipped = assemble([], [], {}, {}, COB)
    assert snapshots == []
    assert skipped == []


def test_assemble_decimal_coercion():
    """Assembler should work fine with float values (Decimal→float done in staging.py)."""
    agreements = [_agreement()]
    snapshots, _ = assemble(agreements, [], {}, {}, COB)
    assert isinstance(snapshots[0].agreement.threshold_amount, float)
