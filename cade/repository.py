from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING

from cade.models import COBSnapshot, ExposureSummary, SnapshotDiff

if TYPE_CHECKING:
    from cade.models import TradePosition


class AgreementRepository(ABC):
    """Abstract storage interface for cade.

    All backends implement this interface. All tests bind to it.

    Contract:
    - Snapshots are immutable. Re-writing raises DuplicateSnapshotError.
    - data_hash is set by store_snapshot(), never by the caller.
    - exposure_total is provided by the caller; cade does not compute it.
    """

    @abstractmethod
    def store_snapshot(self, snapshot: COBSnapshot, exposure_total: float) -> COBSnapshot:
        """Persist snapshot. Sets data_hash. Returns snapshot with hash populated.

        Raises: DuplicateSnapshotError, MatrixReferenceError, ValueError (NaN/Inf).
        """

    @abstractmethod
    def get_snapshot(self, counterparty_id: str, netting_set_id: str, cob_date: date) -> COBSnapshot:
        """Retrieve and hash-verify a snapshot.

        Raises: SnapshotNotFound, IntegrityError.
        """

    @abstractmethod
    def list_cob_dates(self, counterparty_id: str, netting_set_id: str) -> list[date]:
        """All COB dates with snapshots, sorted ascending."""

    @abstractmethod
    def list_netting_sets(self, counterparty_id: str) -> list[str]:
        """All netting set IDs for a counterparty."""

    @abstractmethod
    def get_portfolio(
        self,
        cob_date: date,
        threshold: float | None = None,
        top_n: int | None = None,
    ) -> tuple[list[ExposureSummary], bool]:
        """Portfolio exposure ranking. Returns (summaries, index_stale)."""

    @abstractmethod
    def list_counterparties(self) -> list[str]:
        """All counterparty IDs with at least one stored snapshot."""

    def find_by_trade(
        self,
        trade_id: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[tuple[str, str, date, "TradePosition"]]:
        """Find all (counterparty_id, netting_set_id, cob_date, trade) tuples
        containing trade_id. Default implementation scans all snapshots.
        Backends can override with an index-based approach for better performance.

        Returns list of (counterparty_id, netting_set_id, cob_date, TradePosition).
        """
        results = []
        for cp_id in self.list_counterparties():
            for ns_id in self.list_netting_sets(cp_id):
                for d in self.list_cob_dates(cp_id, ns_id):
                    if from_date is not None and d < from_date:
                        continue
                    if to_date is not None and d > to_date:
                        continue
                    snap = self.get_snapshot(cp_id, ns_id, d)
                    for trade in snap.trades:
                        if trade.trade_id == trade_id:
                            results.append((cp_id, ns_id, d, trade))
        return results

    def get_diff(
        self,
        counterparty_id: str,
        netting_set_id: str,
        from_date: date,
        to_date: date,
    ) -> SnapshotDiff:
        """Diff two snapshots. Implemented via get_snapshot() — no backend override needed.

        Raises: InvalidRangeError, SnapshotNotFound.
        """
        from cade.exceptions import InvalidRangeError
        from cade.diff import compute_diff

        if from_date >= to_date:
            raise InvalidRangeError(from_date, to_date)

        snap_from = self.get_snapshot(counterparty_id, netting_set_id, from_date)
        snap_to = self.get_snapshot(counterparty_id, netting_set_id, to_date)
        return compute_diff(snap_from, snap_to)
