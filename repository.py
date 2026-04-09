"""Code Coverage Reporter — Line service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CodeRepository:
    """Business-logic service for Line operations in Code Coverage Reporter."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("CodeRepository started")

    def compare(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the compare workflow for a new Line."""
        if "file_path" not in payload:
            raise ValueError("Missing required field: file_path")
        record = self._repo.insert(
            payload["file_path"], payload.get("collected_at"),
            **{k: v for k, v in payload.items()
              if k not in ("file_path", "collected_at")}
        )
        if self._events:
            self._events.emit("line.compared", record)
        return record

    def export(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Line and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Line {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("line.exportd", updated)
        return updated

    def report(self, rec_id: str) -> None:
        """Remove a Line and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Line {rec_id!r} not found")
        if self._events:
            self._events.emit("line.reportd", {"id": rec_id})

    def search(
        self,
        file_path: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search lines by *file_path* and/or *status*."""
        filters: Dict[str, Any] = {}
        if file_path is not None:
            filters["file_path"] = file_path
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search lines: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Line counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
