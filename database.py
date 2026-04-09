"""Code Coverage Reporter — Run repository."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CodeDatabase:
    """Thin repository wrapper for Run persistence in Code Coverage Reporter."""

    TABLE = "runs"

    def __init__(self, db: Any) -> None:
        self._db = db
        logger.debug("CodeDatabase bound to %s", db)

    def insert(self, line_pct: Any, branch_pct: Any, **kwargs: Any) -> str:
        """Persist a new Run row and return its generated ID."""
        rec_id = str(uuid.uuid4())
        row: Dict[str, Any] = {
            "id":         rec_id,
            "line_pct": line_pct,
            "branch_pct": branch_pct,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self._db.insert(self.TABLE, row)
        return rec_id

    def fetch(self, rec_id: str) -> Optional[Dict[str, Any]]:
        """Return the Run row for *rec_id*, or None."""
        return self._db.fetch(self.TABLE, rec_id)

    def update(self, rec_id: str, **fields: Any) -> bool:
        """Patch *fields* on an existing Run row."""
        if not self._db.exists(self.TABLE, rec_id):
            return False
        fields["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._db.update(self.TABLE, rec_id, fields)
        return True

    def delete(self, rec_id: str) -> bool:
        """Hard-delete a Run row; returns False if not found."""
        if not self._db.exists(self.TABLE, rec_id):
            return False
        self._db.delete(self.TABLE, rec_id)
        return True

    def query(
        self,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit:    int = 100,
        offset:   int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Return (rows, total_count) for the given *filters*."""
        rows  = self._db.select(self.TABLE, filters or {}, limit, offset)
        total = self._db.count(self.TABLE, filters or {})
        logger.debug("query runs: %d/%d", len(rows), total)
        return rows, total

    def report_by_run_id(
        self, value: Any, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Fetch runs filtered by *run_id*."""
        rows, _ = self.query({"run_id": value}, limit=limit)
        return rows

    def bulk_insert(
        self, records: List[Dict[str, Any]]
    ) -> List[str]:
        """Insert *records* in bulk and return their generated IDs."""
        ids: List[str] = []
        for rec in records:
            rec_id = self.insert(
                rec["line_pct"], rec.get("branch_pct"),
                **{k: v for k, v in rec.items() if k not in ("line_pct", "branch_pct")}
            )
            ids.append(rec_id)
        logger.info("bulk_insert runs: %d rows", len(ids))
        return ids
