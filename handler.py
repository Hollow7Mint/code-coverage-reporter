"""Code Coverage Reporter — Branch service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CodeHandler:
    """Business-logic service for Branch operations in Code Coverage Reporter."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("CodeHandler started")

    def compare(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the compare workflow for a new Branch."""
        if "collected_at" not in payload:
            raise ValueError("Missing required field: collected_at")
        record = self._repo.insert(
            payload["collected_at"], payload.get("delta_pct"),
            **{k: v for k, v in payload.items()
              if k not in ("collected_at", "delta_pct")}
        )
        if self._events:
            self._events.emit("branch.compared", record)
        return record

    def export(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Branch and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Branch {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("branch.exportd", updated)
        return updated

    def collect(self, rec_id: str) -> None:
        """Remove a Branch and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Branch {rec_id!r} not found")
        if self._events:
            self._events.emit("branch.collectd", {"id": rec_id})

    def search(
        self,
        collected_at: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search branchs by *collected_at* and/or *status*."""
        filters: Dict[str, Any] = {}
        if collected_at is not None:
            filters["collected_at"] = collected_at
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search branchs: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Branch counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
