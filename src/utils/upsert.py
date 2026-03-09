"""Best-effort value-change tracking for source-mixed pipeline upserts.

This module wraps upsert call sites with read-before / read-after snapshots
to surface non-null → different-non-null column changes.  It is **observability
only** — a tracker failure must never roll back or prevent the real data write.

Design constraints (Phase 1):

- ``property_market.primary_source`` is row-level (first-writer-wins), not
  per-column.  We therefore cannot distinguish intentional priority overrides
  (Zillow refreshing zestimate on a ``primary_source='homeharvest'`` row) from
  accidental clobbers.  The tracker logs *all* value mutations and includes
  source context; it does **not** gate on cross-source vs same-source.
- Inserts are tagged ``was_insert=True`` and produce no change events.
- Only non-null → different-non-null changes are reported.
- All DB operations inside the tracker are wrapped in try/except so that a
  tracker bug can never abort the enclosing transaction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy import select
from sqlalchemy import table

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


MARKET_SOURCE_COLUMN = "primary_source"
MARKET_TRACKED_COLUMNS: list[str] = [
    "zestimate",
    "rent_zestimate",
    "list_price",
    "tax_assessed_value",
    "listing_status",
    "beds",
    "baths",
    "sqft",
    "year_built",
    "lot_size",
    "property_type",
    "detail_url",
]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(slots=True)
class OverwriteEvent:
    """A non-null → different-non-null column change detected around an upsert."""

    table: str
    row_key: str
    column: str
    old_value: Any
    new_value: Any
    stored_source: str | None
    incoming_source: str


@dataclass(slots=True)
class UpsertResult:
    """Structured result from an overwrite-tracked upsert."""

    table: str
    source: str
    row_key: str
    was_insert: bool = False  # True when no before-snapshot exists (new row OR snapshot failure)
    overwrites: list[OverwriteEvent] = field(default_factory=list)

    @property
    def has_overwrites(self) -> bool:
        return bool(self.overwrites)

    def log_overwrites(self) -> None:
        for ow in self.overwrites:
            logger.info(
                "VALUE_CHANGE {table} row_key={row_key} {column}: {old!r} -> {new!r} "
                "row_source={row_source} writer={writer}",
                table=ow.table,
                row_key=ow.row_key,
                column=ow.column,
                old=ow.old_value,
                new=ow.new_value,
                row_source=ow.stored_source,
                writer=ow.incoming_source,
            )


class OverwriteTracker:
    """Best-effort read-before/read-after comparison for tracked upserts."""

    def __init__(self, table_name: str, *, source: str) -> None:
        self._table_name = table_name
        self._source = source
        self._before: dict[str, Any] | None = None
        self._stored_source: str | None = None

    def snapshot_before(
        self,
        conn: Connection,
        row_key: str,
        tracked_columns: list[str],
        *,
        source_column: str | None = None,
    ) -> None:
        """Read current row state.  Must never raise — tracker is best-effort."""
        try:
            row = self._load_row(
                conn,
                row_key=row_key,
                tracked_columns=tracked_columns,
                source_column=source_column,
            )
            if row is None:
                self._before = None
                self._stored_source = None
                return

            self._stored_source = _clean_source(row.pop(source_column, None)) if source_column else None
            self._before = row
        except Exception:
            logger.debug("Tracker snapshot_before failed for row_key={}", row_key)
            self._before = None
            self._stored_source = None

    def compare_after(
        self,
        conn: Connection,
        row_key: str,
        tracked_columns: list[str],
        *,
        source_column: str | None = None,
    ) -> UpsertResult:
        """Compare post-write state.  Must never raise — tracker is best-effort."""
        result = UpsertResult(
            table=self._table_name,
            source=self._source,
            row_key=row_key,
        )

        try:
            row = self._load_row(
                conn,
                row_key=row_key,
                tracked_columns=tracked_columns,
                source_column=source_column,
            )
        except Exception:
            logger.debug("Tracker compare_after failed for row_key={}", row_key)
            return result

        if row is None:
            return result

        _ = _clean_source(row.pop(source_column, None)) if source_column else None

        if self._before is None:
            result.was_insert = True
            return result

        # Log ALL non-null → different-non-null changes.  We include
        # stored_source for context but do NOT gate on cross-source vs
        # same-source: primary_source is row-level (first-writer-wins)
        # and cannot reliably identify per-column ownership.
        for column_name in tracked_columns:
            old_value = self._before.get(column_name)
            new_value = row.get(column_name)
            if old_value is None or new_value is None or old_value == new_value:
                continue
            result.overwrites.append(
                OverwriteEvent(
                    table=self._table_name,
                    row_key=row_key,
                    column=column_name,
                    old_value=old_value,
                    new_value=new_value,
                    stored_source=self._stored_source,
                    incoming_source=self._source,
                )
            )

        return result

    def _load_row(
        self,
        conn: Connection,
        *,
        row_key: str,
        tracked_columns: list[str],
        source_column: str | None,
    ) -> dict[str, Any] | None:
        query_columns = list(tracked_columns)
        if source_column and source_column not in query_columns:
            query_columns.append(source_column)
        statement = _build_select_statement(
            table_name=self._table_name,
            selected_columns=query_columns,
            key_column="strap",
        )
        row = conn.execute(statement, {"row_key": row_key}).mappings().fetchone()
        return dict(row) if row is not None else None


def _build_select_statement(
    *,
    table_name: str,
    selected_columns: list[str],
    key_column: str,
):
    safe_key = _validated_identifier(key_column)
    safe_columns = [_validated_identifier(name) for name in selected_columns]
    table_columns = [safe_key, *safe_columns]
    table_clause = table(
        _validated_identifier(table_name),
        *(column(name) for name in dict.fromkeys(table_columns)),
    )
    return select(*(table_clause.c[name] for name in safe_columns)).where(
        table_clause.c[safe_key] == bindparam("row_key")
    )


def _validated_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def _clean_source(value: Any) -> str | None:
    if value is None:
        return None
    source = str(value).strip()
    return source if source else None
