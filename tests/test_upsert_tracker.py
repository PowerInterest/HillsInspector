"""Tests for src.utils.upsert — OverwriteTracker and helpers.

Covers:
  - SQL identifier validation
  - source string cleaning
  - SELECT statement construction
  - UpsertResult dataclass
  - Phase 2 change-log persistence
  - OverwriteTracker: insert detection, value-change detection (regardless of
    source), null-skip, fault isolation from the enclosing transaction
"""

import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, create_engine, text
from unittest.mock import patch

from src.utils.upsert import (
    MARKET_SOURCE_COLUMN,
    MARKET_TRACKED_COLUMNS,
    OverwriteEvent,
    OverwriteTracker,
    UpsertResult,
    _build_select_statement,
    _clean_source,
    _validated_identifier,
)


# ---------------------------------------------------------------------------
# Fixtures — disposable in-memory table via SQLite
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    meta = MetaData()
    Table(
        "property_market",
        meta,
        Column("strap", String, primary_key=True),
        Column("zestimate", Integer),
        Column("rent_zestimate", Integer),
        Column("list_price", Integer),
        Column("tax_assessed_value", Integer),
        Column("listing_status", String),
        Column("beds", Integer),
        Column("baths", Integer),
        Column("sqft", Integer),
        Column("year_built", Integer),
        Column("lot_size", String),
        Column("property_type", String),
        Column("detail_url", String),
        Column("primary_source", String),
        Column("specs_source", String),
        Column("specs_updated_at", DateTime(timezone=True)),
    )
    Table(
        "data_change_log",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("table_name", Text, nullable=False),
        Column("row_key", Text, nullable=False),
        Column("column_name", Text, nullable=False),
        Column("old_value", Text, nullable=True),
        Column("new_value", Text, nullable=True),
        Column("source", Text, nullable=False),
        Column(
            "changed_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    meta.create_all(eng)
    return eng


def _insert_row(engine, strap, source, **cols):
    with engine.begin() as conn:
        params = {"strap": strap, "primary_source": source, **cols}
        col_str = ", ".join(params.keys())
        placeholders = ", ".join(f":{k}" for k in params)
        conn.execute(text(f"INSERT INTO property_market ({col_str}) VALUES ({placeholders})"), params)


def _read_row(engine, strap):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM property_market WHERE strap = :s"), {"s": strap}
        ).mappings().fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# _validated_identifier
# ---------------------------------------------------------------------------

class TestValidatedIdentifier:
    def test_valid_names(self):
        assert _validated_identifier("strap") == "strap"
        assert _validated_identifier("primary_source") == "primary_source"
        assert _validated_identifier("PropertyMarket") == "PropertyMarket"
        assert _validated_identifier("col2") == "col2"

    def test_rejects_sql_injection(self):
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validated_identifier("strap; DROP TABLE--")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validated_identifier("my column")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validated_identifier("")

    def test_rejects_leading_digit(self):
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validated_identifier("1column")


# ---------------------------------------------------------------------------
# _clean_source
# ---------------------------------------------------------------------------

class TestCleanSource:
    def test_none_returns_none(self):
        assert _clean_source(None) is None

    def test_empty_returns_none(self):
        assert _clean_source("") is None
        assert _clean_source("   ") is None

    def test_strips_whitespace(self):
        assert _clean_source("  redfin  ") == "redfin"

    def test_non_string_coerced(self):
        assert _clean_source(42) == "42"


# ---------------------------------------------------------------------------
# _build_select_statement
# ---------------------------------------------------------------------------

class TestBuildSelectStatement:
    def test_compiles(self):
        stmt = _build_select_statement(
            table_name="property_market",
            selected_columns=["zestimate", "beds"],
            key_column="strap",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
        assert "property_market" in compiled
        assert "zestimate" in compiled
        assert "beds" in compiled

    def test_rejects_bad_table(self):
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _build_select_statement(
                table_name="DROP TABLE x",
                selected_columns=["col"],
                key_column="strap",
            )


# ---------------------------------------------------------------------------
# UpsertResult
# ---------------------------------------------------------------------------

class TestUpsertResult:
    def test_no_overwrites(self):
        r = UpsertResult(table="t", source="s", row_key="k")
        assert not r.has_overwrites
        assert r.overwrites == []

    def test_with_overwrites(self):
        r = UpsertResult(table="t", source="s", row_key="k")
        r.overwrites.append(
            OverwriteEvent(
                table="t", row_key="k", column="beds",
                old_value=3, new_value=4,
                stored_source="zillow", incoming_source="redfin",
            )
        )
        assert r.has_overwrites

    def test_was_insert(self):
        r = UpsertResult(table="t", source="s", row_key="k", was_insert=True)
        assert r.was_insert
        assert not r.has_overwrites

    def test_flush_to_log_persists_overwrite_events(self, engine):
        result = UpsertResult(table="property_market", source="redfin", row_key="TESTLOG1")
        result.overwrites.append(
            OverwriteEvent(
                table="property_market",
                row_key="TESTLOG1",
                column="beds",
                old_value=3,
                new_value=4,
                stored_source="zillow",
                incoming_source="redfin",
            )
        )
        result.overwrites.append(
            OverwriteEvent(
                table="property_market",
                row_key="TESTLOG1",
                column="sqft",
                old_value=1500,
                new_value=1800,
                stored_source="zillow",
                incoming_source="redfin",
            )
        )

        with engine.begin() as conn:
            inserted = result.flush_to_log(conn)

        assert inserted == 2
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT table_name, row_key, column_name, old_value, new_value, source "
                    "FROM data_change_log ORDER BY column_name"
                )
            ).mappings().fetchall()
        assert [dict(row) for row in rows] == [
            {
                "table_name": "property_market",
                "row_key": "TESTLOG1",
                "column_name": "beds",
                "old_value": "3",
                "new_value": "4",
                "source": "redfin",
            },
            {
                "table_name": "property_market",
                "row_key": "TESTLOG1",
                "column_name": "sqft",
                "old_value": "1500",
                "new_value": "1800",
                "source": "redfin",
            },
        ]

    def test_flush_to_log_noop_when_no_overwrites(self, engine):
        result = UpsertResult(table="property_market", source="redfin", row_key="TESTLOG2")

        with engine.begin() as conn:
            inserted = result.flush_to_log(conn)

        assert inserted == 0

    def test_flush_to_log_fault_isolation(self, engine):
        result = UpsertResult(table="property_market", source="redfin", row_key="TESTLOG3")
        result.overwrites.append(
            OverwriteEvent(
                table="property_market",
                row_key="TESTLOG3",
                column="beds",
                old_value=3,
                new_value=4,
                stored_source="zillow",
                incoming_source="redfin",
            )
        )

        with engine.begin() as conn:
            with patch.object(conn, "execute", side_effect=RuntimeError("boom")):
                inserted = result.flush_to_log(conn)

        assert inserted == 0

    def test_flush_to_log_failure_does_not_abort_outer_transaction(self, engine):
        _insert_row(engine, "TESTLOG4", "zillow", beds=3)
        result = UpsertResult(table="property_market", source="redfin", row_key="TESTLOG4")
        result.overwrites.append(
            OverwriteEvent(
                table="property_market",
                row_key="TESTLOG4",
                column="beds",
                old_value=3,
                new_value=4,
                stored_source="zillow",
                incoming_source="redfin",
            )
        )

        with engine.begin() as conn:
            original_execute = conn.execute

            def _execute(statement, params=None):
                if "INSERT INTO data_change_log" in str(statement):
                    raise RuntimeError("boom")
                if params is None:
                    return original_execute(statement)
                return original_execute(statement, params)

            with patch.object(conn, "execute", side_effect=_execute):
                inserted = result.flush_to_log(conn)
                conn.execute(
                    text("UPDATE property_market SET beds = 5 WHERE strap = :strap"),
                    {"strap": "TESTLOG4"},
                )

        assert inserted == 0
        assert _read_row(engine, "TESTLOG4")["beds"] == 5


# ---------------------------------------------------------------------------
# OverwriteTracker — insert detection
# ---------------------------------------------------------------------------

class TestOverwriteTrackerInsert:
    """When the row doesn't exist before, result.was_insert is True."""

    def test_insert_detected(self, engine):
        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST001", ["beds", "sqft"],
                source_column="primary_source",
            )
            conn.execute(text(
                "INSERT INTO property_market (strap, beds, sqft, primary_source) "
                "VALUES ('TEST001', 3, 1500, 'redfin')"
            ))
            result = tracker.compare_after(
                conn, "TEST001", ["beds", "sqft"],
                source_column="primary_source",
            )
        assert result.was_insert
        assert not result.has_overwrites


# ---------------------------------------------------------------------------
# OverwriteTracker — value change detection (source-agnostic)
# ---------------------------------------------------------------------------

class TestOverwriteTrackerValueChanges:
    """Any non-null→different-non-null change is detected, regardless of source."""

    def test_same_source_value_change_detected(self, engine):
        """Same-source refresh that changes a value IS logged (no source gate)."""
        _insert_row(engine, "TEST002", "zillow", beds=3, sqft=1500)

        tracker = OverwriteTracker("property_market", source="zillow")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST002", ["beds", "sqft"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = 4, sqft = 1800 WHERE strap = 'TEST002'"
            ))
            result = tracker.compare_after(
                conn, "TEST002", ["beds", "sqft"],
                source_column="primary_source",
            )
        assert not result.was_insert
        assert result.has_overwrites
        assert len(result.overwrites) == 2
        cols = {ow.column for ow in result.overwrites}
        assert cols == {"beds", "sqft"}

    def test_cross_source_value_change_detected(self, engine):
        """Cross-source write that changes a value IS logged."""
        _insert_row(engine, "TEST003", "zillow", beds=3, sqft=1500)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST003", ["beds", "sqft"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = 4, sqft = 1500 WHERE strap = 'TEST003'"
            ))
            result = tracker.compare_after(
                conn, "TEST003", ["beds", "sqft"],
                source_column="primary_source",
            )
        assert not result.was_insert
        assert result.has_overwrites
        assert len(result.overwrites) == 1
        ow = result.overwrites[0]
        assert ow.column == "beds"
        assert ow.old_value == 3
        assert ow.new_value == 4
        assert ow.stored_source == "zillow"
        assert ow.incoming_source == "redfin"

    def test_no_source_column_still_detects_changes(self, engine):
        """Without source_column, value changes are still tracked."""
        _insert_row(engine, "TEST006", "zillow", beds=3)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(conn, "TEST006", ["beds"])
            conn.execute(text(
                "UPDATE property_market SET beds = 99 WHERE strap = 'TEST006'"
            ))
            result = tracker.compare_after(conn, "TEST006", ["beds"])
        assert result.has_overwrites
        assert result.overwrites[0].old_value == 3
        assert result.overwrites[0].new_value == 99
        assert result.overwrites[0].stored_source is None

    def test_null_stored_source_still_detects_changes(self, engine):
        """Null primary_source doesn't suppress change detection."""
        _insert_row(engine, "TEST007", None, beds=3)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST007", ["beds"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = 99 WHERE strap = 'TEST007'"
            ))
            result = tracker.compare_after(
                conn, "TEST007", ["beds"],
                source_column="primary_source",
            )
        assert result.has_overwrites
        assert result.overwrites[0].stored_source is None

    def test_unchanged_value_not_flagged(self, engine):
        """No change event when old == new."""
        _insert_row(engine, "TEST008", "zillow", beds=3)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST008", ["beds"],
                source_column="primary_source",
            )
            # No actual change to beds
            conn.execute(text(
                "UPDATE property_market SET primary_source = 'redfin' WHERE strap = 'TEST008'"
            ))
            result = tracker.compare_after(
                conn, "TEST008", ["beds"],
                source_column="primary_source",
            )
        assert not result.has_overwrites


# ---------------------------------------------------------------------------
# OverwriteTracker — null-skip behaviour
# ---------------------------------------------------------------------------

class TestOverwriteTrackerNullSkip:
    """Null old or null new values should NOT count as changes."""

    def test_null_old_not_flagged(self, engine):
        _insert_row(engine, "TEST004", "zillow", beds=None, sqft=1500)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST004", ["beds", "sqft"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = 4 WHERE strap = 'TEST004'"
            ))
            result = tracker.compare_after(
                conn, "TEST004", ["beds", "sqft"],
                source_column="primary_source",
            )
        assert not result.has_overwrites

    def test_null_new_not_flagged(self, engine):
        _insert_row(engine, "TEST005", "zillow", beds=3, sqft=1500)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST005", ["beds", "sqft"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = NULL WHERE strap = 'TEST005'"
            ))
            result = tracker.compare_after(
                conn, "TEST005", ["beds", "sqft"],
                source_column="primary_source",
            )
        assert not result.has_overwrites


# ---------------------------------------------------------------------------
# OverwriteTracker — fault isolation (HIGH severity fix)
# ---------------------------------------------------------------------------

class TestOverwriteTrackerFaultIsolation:
    """Tracker failures must never abort the enclosing transaction."""

    def test_snapshot_before_failure_does_not_propagate(self, engine):
        """If snapshot_before raises, it swallows the error and the
        subsequent write + compare_after still succeed."""
        _insert_row(engine, "TEST_FAULT1", "zillow", beds=3)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            with patch.object(tracker, "_load_row", side_effect=RuntimeError("boom")):
                tracker.snapshot_before(
                    conn, "TEST_FAULT1", ["beds"],
                    source_column="primary_source",
                )
            # snapshot_before failed, but the write must still succeed
            conn.execute(text(
                "UPDATE property_market SET beds = 99 WHERE strap = 'TEST_FAULT1'"
            ))
            result = tracker.compare_after(
                conn, "TEST_FAULT1", ["beds"],
                source_column="primary_source",
            )
        # Because snapshot_before failed, _before is None → was_insert=True
        assert result.was_insert
        # The actual write committed
        assert _read_row(engine, "TEST_FAULT1")["beds"] == 99

    def test_compare_after_failure_does_not_rollback_write(self, engine):
        """If compare_after raises, the real data write must survive."""
        _insert_row(engine, "TEST_FAULT2", "zillow", beds=3)

        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            tracker.snapshot_before(
                conn, "TEST_FAULT2", ["beds"],
                source_column="primary_source",
            )
            conn.execute(text(
                "UPDATE property_market SET beds = 99 WHERE strap = 'TEST_FAULT2'"
            ))
            with patch.object(tracker, "_load_row", side_effect=RuntimeError("boom")):
                result = tracker.compare_after(
                    conn, "TEST_FAULT2", ["beds"],
                    source_column="primary_source",
                )
        # compare_after returned empty result, did not raise
        assert not result.has_overwrites
        # The actual write committed despite tracker failure
        assert _read_row(engine, "TEST_FAULT2")["beds"] == 99

    def test_tracker_error_never_surfaces_to_caller(self, engine):
        """Even if _load_row raises in both methods, no exception escapes."""
        tracker = OverwriteTracker("property_market", source="redfin")
        with engine.begin() as conn:
            with patch.object(tracker, "_load_row", side_effect=RuntimeError("boom")):
                # Neither call should raise
                tracker.snapshot_before(conn, "NOPE", ["beds"])
                result = tracker.compare_after(conn, "NOPE", ["beds"])
        assert not result.has_overwrites
        assert not result.was_insert


# ---------------------------------------------------------------------------
# Market constants
# ---------------------------------------------------------------------------

class TestMarketConstants:
    def test_source_column(self):
        assert MARKET_SOURCE_COLUMN == "primary_source"

    def test_tracked_columns_no_duplicates(self):
        assert len(MARKET_TRACKED_COLUMNS) == len(set(MARKET_TRACKED_COLUMNS))

    def test_tracked_columns_all_valid_identifiers(self):
        for col in MARKET_TRACKED_COLUMNS:
            assert _validated_identifier(col) == col
