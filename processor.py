"""Code Coverage Reporter — File service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CodeProcessor:
    """Business-logic service for File operations in Code Coverage Reporter."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("CodeProcessor started")

    def report(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the report workflow for a new File."""
        if "branch_pct" not in payload:
            raise ValueError("Missing required field: branch_pct")
        record = self._repo.insert(
            payload["branch_pct"], payload.get("delta_pct"),
            **{k: v for k, v in payload.items()
              if k not in ("branch_pct", "delta_pct")}
        )
        if self._events:
            self._events.emit("file.reportd", record)
        return record

    def collect(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a File and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"File {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("file.collectd", updated)
        return updated

    def aggregate(self, rec_id: str) -> None:
        """Remove a File and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"File {rec_id!r} not found")
        if self._events:
            self._events.emit("file.aggregated", {"id": rec_id})

    def search(
        self,
        branch_pct: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search files by *branch_pct* and/or *status*."""
        filters: Dict[str, Any] = {}
        if branch_pct is not None:
            filters["branch_pct"] = branch_pct
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search files: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of File counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
