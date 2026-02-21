"""PG-only trust account movement analysis service.

This wrapper adds controller-friendly availability checks while delegating all
analysis logic to ``TrustAccountsService`` (which is PG-only).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text

from src.services.trust_accounts import TrustAccountsService
from sunbiz.db import get_engine, resolve_pg_dsn


class PgTrustAccountsService(TrustAccountsService):
    """Trust account service with explicit PG availability gating."""

    def __init__(
        self,
        dsn: str | None = None,
        download_dir: str = "data/tmp/trust_accounts",
        request_timeout: int = 20,
    ) -> None:
        self._available = False
        self._unavailable_reason: str | None = None

        resolved = resolve_pg_dsn(dsn)
        self._engine = get_engine(resolved)
        self.pg_dsn = resolved
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.request_timeout = request_timeout

        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._available = True
            logger.info(
                "PgTrustAccountsService connected (dsn={})",
                self._dsn_tag(self.pg_dsn),
            )
        except Exception as e:
            self._unavailable_reason = str(e)
            logger.opt(exception=True).warning(
                "PgTrustAccountsService unavailable (dsn={}): {}",
                self._dsn_tag(self.pg_dsn),
                e,
            )

    @property
    def available(self) -> bool:
        return self._available

    @property
    def unavailable_reason(self) -> str | None:
        return self._unavailable_reason

    def run(self, force_reprocess: bool = False) -> dict[str, Any]:
        if not self._available:
            return {
                "skipped": True,
                "reason": "service_unavailable",
                "details": self._unavailable_reason,
            }
        return super().run(force_reprocess=force_reprocess)
