"""Read raw data from the PostgreSQL staging tables.

Staging tables are populated by upstream systems (push_to_cade.sh on
each Linux dump server). This module reads the raw rows for a given
COB date and returns plain dicts — no cade model construction here.

Tables:
  staging_agreements  — one row per (counterparty_id, netting_set_id, cob_date)
  staging_trades      — one row per (counterparty_id, netting_set_id, cob_date, trade_id)
  staging_market_data — one row per (cob_date, data_type, key)
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


def _float(v: Any) -> float:
    """Explicit Decimal → float coercion. psycopg2 returns NUMERIC as Decimal."""
    if v is None:
        return 0.0
    return float(v)


def get_agreements(conn, cob_date: date) -> list[dict]:
    """Return all staging_agreements rows for cob_date as plain dicts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT counterparty_id, netting_set_id, agreement_id,
                   threshold_amount, minimum_transfer_amount, independent_amount,
                   currency, eligible_collateral, rounding_amount,
                   valuation_agent, extra
            FROM staging_agreements
            WHERE cob_date = %s
            """,
            (cob_date,),
        )
        cols = [d[0] for d in cur.description]
        rows = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            # Explicit Decimal → float conversions
            for field in ("threshold_amount", "minimum_transfer_amount",
                          "independent_amount", "rounding_amount"):
                d[field] = _float(d.get(field))
            rows.append(d)
    logger.debug("staging_agreements: %d rows for %s", len(rows), cob_date)
    return rows


def get_trades(conn, cob_date: date) -> list[dict]:
    """Return all staging_trades rows for cob_date as plain dicts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT counterparty_id, netting_set_id, trade_id,
                   product_type, notional, currency, direction,
                   maturity_date, extra
            FROM staging_trades
            WHERE cob_date = %s
            """,
            (cob_date,),
        )
        cols = [d[0] for d in cur.description]
        rows = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            d["notional"] = _float(d.get("notional"))
            rows.append(d)
    logger.debug("staging_trades: %d rows for %s", len(rows), cob_date)
    return rows


def get_market_data(conn, cob_date: date) -> dict:
    """Return market data for cob_date as {fx_rates: {...}, inflation_rates: {...}}."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type, key, value
            FROM staging_market_data
            WHERE cob_date = %s
            """,
            (cob_date,),
        )
        fx_rates: dict[str, float] = {}
        inflation_rates: dict[str, float] = {}
        for data_type, key, value in cur.fetchall():
            v = _float(value)
            if data_type == "fx_rate":
                fx_rates[key] = v
            elif data_type == "inflation_rate":
                inflation_rates[key] = v
            else:
                logger.warning("Unknown market_data type %r for %s", data_type, cob_date)
    return {"fx_rates": fx_rates, "inflation_rates": inflation_rates}
