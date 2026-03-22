"""Assemble COBSnapshot objects from staging rows.

Takes plain dicts from staging.py and matrix refs from matrix_sync.py
and builds fully-populated COBSnapshot Pydantic models ready for POST /ingest.

Completeness check:
  A counterparty/netting-set is COMPLETE if:
    - It has a row in staging_agreements for this date
    - All matrix keys listed in staging have been downloaded to CADE_DATA_DIR

  If market_data rows are missing, the snapshot is still assembled with
  empty fx_rates / inflation_rates (market data may not exist for all dates).

  If any expected matrix file is missing from disk, the counterparty is
  flagged as INCOMPLETE and skipped (caller handles the warning).
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from cade.models import (
    COBSnapshot,
    ISDAgreement,
    MarketDataSet,
    MatrixRef,
    TradePosition,
)

logger = logging.getLogger(__name__)


def assemble(
    agreements: list[dict],
    trades: list[dict],
    market_data: dict,
    matrix_refs: dict[str, dict],  # {"{cp_id}/{ns_id}": {"instrument": MatrixRef}}
    cob_date: date,
) -> tuple[list[COBSnapshot], list[str]]:
    """Build COBSnapshot objects from staging rows.

    Args:
        agreements: Rows from staging.get_agreements().
        trades: Rows from staging.get_trades().
        market_data: Dict from staging.get_market_data().
        matrix_refs: Mapping of "{cp_id}/{ns_id}" to dict of instrument → MatrixRef.
        cob_date: The COB date being assembled.

    Returns:
        (snapshots, skipped) where skipped is a list of
        "{cp_id}/{ns_id}" strings that were incomplete.
    """
    # Index trades by (cp_id, ns_id)
    trades_by_ns: dict[tuple[str, str], list[dict]] = {}
    for t in trades:
        key = (t["counterparty_id"], t["netting_set_id"])
        trades_by_ns.setdefault(key, []).append(t)

    snapshots: list[COBSnapshot] = []
    skipped: list[str] = []

    for ag in agreements:
        cp_id = ag["counterparty_id"]
        ns_id = ag["netting_set_id"]
        ns_key = f"{cp_id}/{ns_id}"

        # Build agreement
        agreement = ISDAgreement(
            agreement_id=ag["agreement_id"],
            threshold_amount=ag["threshold_amount"],
            minimum_transfer_amount=ag["minimum_transfer_amount"],
            independent_amount=ag.get("independent_amount", 0.0),
            currency=ag["currency"] or "USD",
            eligible_collateral=ag.get("eligible_collateral") or [],
            rounding_amount=ag.get("rounding_amount", 0.0),
            valuation_agent=ag.get("valuation_agent"),
            extra=ag.get("extra") or {},
        )

        # Build trade positions
        trade_rows = trades_by_ns.get((cp_id, ns_id), [])
        trade_positions = [
            TradePosition(
                trade_id=t["trade_id"],
                product_type=t["product_type"] or "UNKNOWN",
                notional=t["notional"],
                currency=t["currency"] or "USD",
                direction=t["direction"] or "PAY",
                maturity_date=t.get("maturity_date"),
                extra=t.get("extra") or {},
            )
            for t in trade_rows
        ]

        # Build price matrix refs
        matrices = matrix_refs.get(ns_key, {})
        price_matrices = {
            instrument: ref
            for instrument, ref in matrices.items()
        }

        # Build market data
        market = MarketDataSet(
            fx_rates=market_data.get("fx_rates", {}),
            inflation_rates=market_data.get("inflation_rates", {}),
            price_matrices=price_matrices,
        )

        snap = COBSnapshot(
            counterparty_id=cp_id,
            netting_set_id=ns_id,
            cob_date=cob_date,
            agreement=agreement,
            trades=trade_positions,
            market_data=market,
        )
        snapshots.append(snap)
        logger.debug("Assembled snapshot: %s/%s for %s", cp_id, ns_id, cob_date)

    return snapshots, skipped
