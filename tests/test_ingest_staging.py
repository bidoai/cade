"""Tests for cade.ingest.staging — uses testcontainers for a real Postgres instance.

These tests require Docker. Skip with: pytest -m 'not docker'
"""
import pytest
from datetime import date
from decimal import Decimal

# Skip entire module if testcontainers not installed
testcontainers = pytest.importorskip("testcontainers")
psycopg2 = pytest.importorskip("psycopg2")

import psycopg2 as pg
from testcontainers.postgres import PostgresContainer

from cade.ingest import staging


@pytest.fixture(scope="module")
def pg_conn():
    with PostgresContainer("postgres:16-alpine") as postgres:
        conn = pg.connect(postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://"))
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE staging_agreements (
                counterparty_id TEXT, netting_set_id TEXT, cob_date DATE,
                agreement_id TEXT, threshold_amount NUMERIC,
                minimum_transfer_amount NUMERIC, independent_amount NUMERIC DEFAULT 0,
                currency TEXT, eligible_collateral TEXT[],
                rounding_amount NUMERIC DEFAULT 0, valuation_agent TEXT,
                extra JSONB DEFAULT '{}', ingested_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (counterparty_id, netting_set_id, cob_date)
            )
        """)
        cur.execute("""
            CREATE TABLE staging_trades (
                counterparty_id TEXT, netting_set_id TEXT, cob_date DATE,
                trade_id TEXT, product_type TEXT, notional NUMERIC,
                currency TEXT, direction TEXT, maturity_date DATE,
                extra JSONB DEFAULT '{}', ingested_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (counterparty_id, netting_set_id, cob_date, trade_id)
            )
        """)
        cur.execute("""
            CREATE TABLE staging_market_data (
                cob_date DATE, data_type TEXT, key TEXT,
                value NUMERIC NOT NULL, ingested_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (cob_date, data_type, key)
            )
        """)
        conn.commit()
        yield conn
        conn.close()


COB = date(2024, 3, 15)


@pytest.fixture(autouse=True)
def clean_tables(pg_conn):
    pg_conn.cursor().execute("DELETE FROM staging_agreements; DELETE FROM staging_trades; DELETE FROM staging_market_data;")
    pg_conn.commit()


def test_get_agreements_empty(pg_conn):
    rows = staging.get_agreements(pg_conn, COB)
    assert rows == []


def test_get_agreements_returns_rows(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "INSERT INTO staging_agreements (counterparty_id, netting_set_id, cob_date, agreement_id, threshold_amount, minimum_transfer_amount, currency) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        ("ACME", "RATES-USD", COB, "ISDA-001", Decimal("5000000"), Decimal("500000"), "USD"),
    )
    pg_conn.commit()
    rows = staging.get_agreements(pg_conn, COB)
    assert len(rows) == 1
    assert rows[0]["counterparty_id"] == "ACME"
    assert isinstance(rows[0]["threshold_amount"], float)


def test_get_trades_returns_rows(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "INSERT INTO staging_trades (counterparty_id, netting_set_id, cob_date, trade_id, product_type, notional, currency, direction) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        ("ACME", "RATES-USD", COB, "T-001", "IRS", Decimal("10000000"), "USD", "PAY"),
    )
    pg_conn.commit()
    rows = staging.get_trades(pg_conn, COB)
    assert len(rows) == 1
    assert isinstance(rows[0]["notional"], float)


def test_get_market_data(pg_conn):
    cur = pg_conn.cursor()
    cur.executemany(
        "INSERT INTO staging_market_data (cob_date, data_type, key, value) VALUES (%s, %s, %s, %s)",
        [(COB, "fx_rate", "USD/GBP", Decimal("0.79")), (COB, "inflation_rate", "UK_RPI", Decimal("0.031"))],
    )
    pg_conn.commit()
    md = staging.get_market_data(pg_conn, COB)
    assert md["fx_rates"]["USD/GBP"] == pytest.approx(0.79)
    assert md["inflation_rates"]["UK_RPI"] == pytest.approx(0.031)


def test_get_market_data_empty(pg_conn):
    md = staging.get_market_data(pg_conn, COB)
    assert md == {"fx_rates": {}, "inflation_rates": {}}
