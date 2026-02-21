"""
SQLAlchemy ORM models for Hillsborough County Clerk of Court civil bulk data.

Tables:
- clerk_civil_cases: Monthly bulk case data
- clerk_civil_events: Monthly bulk event/docket data
- clerk_civil_parties: Monthly bulk party data (plaintiff, defendant, attorney)
- clerk_disposed_cases: Monthly disposed cases report
- clerk_garnishment_cases: Weekly return-of-service and garnishment report
- clerk_name_index: Complete alphabetical party index (20+ years, Circuit + County)
- official_records_daily_instruments: Daily Official Records D/P/M instrument feed

Data sources:
- https://publicrec.hillsclerk.com/Civil/bulkdata/
- https://publicrec.hillsclerk.com/Civil/CircuitCivilDisposedCases/
- https://publicrec.hillsclerk.com/Civil/Circuit%20and%20County%20Civil%20with%20Return%20of%20Service%20and%20Garnishment%20Data/
- https://publicrec.hillsclerk.com/Civil/alpha_index/Circuit/
- https://publicrec.hillsclerk.com/Civil/alpha_index/County/
- https://publicrec.hillsclerk.com/OfficialRecords/DailyIndexes/
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import Time
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from sunbiz.models import Base


class ClerkCivilCase(Base):
    __tablename__ = "clerk_civil_cases"

    case_number: Mapped[str] = mapped_column(String(32), primary_key=True)
    ucn: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    style: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    division: Mapped[str | None] = mapped_column(String(16), nullable=True)
    judge: Mapped[str | None] = mapped_column(Text, nullable=True)
    cause_of_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    cause_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    filing_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    judgment_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    judgment_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    judgment_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    is_foreclosure: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        Index("idx_clerk_cases_case_type", "case_type"),
        Index("idx_clerk_cases_filing_date", "filing_date"),
        Index("idx_clerk_cases_case_status", "case_status"),
        Index("idx_clerk_cases_judgment_date", "judgment_date"),
        Index(
            "idx_clerk_cases_foreclosure",
            "case_number",
            postgresql_where="is_foreclosure = true",
        ),
    )


class ClerkCivilEvent(Base):
    __tablename__ = "clerk_civil_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    case_number: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    party_first_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    party_middle_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    party_last_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "case_number",
            "event_code",
            "event_date",
            "party_last_name",
            name="uq_clerk_events_case_code_date_party",
        ),
        Index("idx_clerk_events_code", "event_code"),
        Index("idx_clerk_events_date", "event_date"),
        Index("idx_clerk_events_case_number", "case_number"),
    )


class ClerkCivilParty(Base):
    __tablename__ = "clerk_civil_parties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    case_number: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    party_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    middle_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    zip: Mapped[str | None] = mapped_column(Text, nullable=True)
    bar_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    phone: Mapped[str | None] = mapped_column(Text, nullable=True)
    email: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "case_number",
            "party_type",
            "name",
            name="uq_clerk_parties_case_type_name",
        ),
        Index("idx_clerk_parties_case_number", "case_number"),
        Index("idx_clerk_parties_party_type", "party_type"),
        # GIN trigram index on name for fuzzy search (requires pg_trgm extension)
        Index(
            "idx_clerk_parties_name_trgm",
            "name",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        ),
    )


class ClerkDisposedCase(Base):
    __tablename__ = "clerk_disposed_cases"

    case_number: Mapped[str] = mapped_column(String(32), primary_key=True)
    style: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_subtype: Mapped[str | None] = mapped_column(Text, nullable=True)
    closure_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    statistical_closure: Mapped[str | None] = mapped_column(Text, nullable=True)
    closure_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    current_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        Index("idx_clerk_disposed_closure_date", "closure_date"),
        Index("idx_clerk_disposed_case_type", "case_type"),
        Index("idx_clerk_disposed_status", "current_status"),
    )


class ClerkGarnishmentCase(Base):
    __tablename__ = "clerk_garnishment_cases"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    row_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    case_number: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    filing_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    plaintiff_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    garnishee_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    defendant_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    address3: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    zip: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_type_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    case_status_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    pre_trial_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    service_return_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    service_return_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    non_service_return_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    writ_filed_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    writ_issued_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    snapshot_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        Index("idx_clerk_garnishment_case_number", "case_number"),
        Index("idx_clerk_garnishment_snapshot_date", "snapshot_date"),
        Index("idx_clerk_garnishment_writ_issued_date", "writ_issued_date"),
        Index("idx_clerk_garnishment_defendant_name", "defendant_name"),
    )


class ClerkNameIndex(Base):
    """Complete alphabetical party index — all civil cases, 20+ years of history.

    Source: https://publicrec.hillsclerk.com/Civil/alpha_index/{Circuit,County}/
    Format: Pipe-delimited TXT, 27 files per court type (A-Z + NonAlpha).
    Updated weekly by the Clerk.

    The UCN (Uniform Case Number) encodes: county(29) + year + court_type + sequence + party_designator + location(HC).
    Example: 292019CA123456A001HC
    """

    __tablename__ = "clerk_name_index"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    court_type: Mapped[str] = mapped_column(Text, nullable=False)
    business_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    middle_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    suffix: Mapped[str | None] = mapped_column(Text, nullable=True)
    party_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    ucn: Mapped[str] = mapped_column(String(64), nullable=False)
    case_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    case_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    division: Mapped[str | None] = mapped_column(Text, nullable=True)
    judge_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_filed: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    current_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    zip_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    disposition_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    disposition_desc: Mapped[str | None] = mapped_column(Text, nullable=True)
    disposition_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    amount_paid: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_paid: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    akas: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_foreclosure: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint("ucn", "disposition_code", name="uq_clerk_name_index_ucn_disp"),
        Index("idx_clerk_ni_case_number", "case_number"),
        Index("idx_clerk_ni_case_type", "case_type"),
        Index("idx_clerk_ni_date_filed", "date_filed"),
        Index("idx_clerk_ni_party_type", "party_type"),
        Index("idx_clerk_ni_court_type", "court_type"),
        Index("idx_clerk_ni_status", "current_status"),
        Index("idx_clerk_ni_disposition_code", "disposition_code"),
        Index("idx_clerk_ni_foreclosure", "case_number", postgresql_where="is_foreclosure = true"),
        Index(
            "idx_clerk_ni_last_name_trgm",
            "last_name",
            postgresql_using="gin",
            postgresql_ops={"last_name": "gin_trgm_ops"},
        ),
        Index(
            "idx_clerk_ni_business_name_trgm",
            "business_name",
            postgresql_using="gin",
            postgresql_ops={"business_name": "gin_trgm_ops"},
        ),
    )


class OfficialRecordsDailyInstrument(Base):
    """Daily Official Records index row (D + P + M merged by instrument)."""

    __tablename__ = "official_records_daily_instruments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    snapshot_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)

    action: Mapped[str | None] = mapped_column(String(8), nullable=True)
    county_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instrument_number: Mapped[str] = mapped_column(String(64), nullable=False)

    doc_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    doc_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    facc_doc_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    legal_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    book_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    book_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    page_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    recording_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    recording_time: Mapped[dt.time | None] = mapped_column(Time, nullable=True)
    consideration_amount: Mapped[float | None] = mapped_column(Numeric(19, 4), nullable=True)

    parties_from_json: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    parties_to_json: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    parties_from_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    parties_to_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_d_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_p_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_m_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "instrument_number",
            name="uq_official_records_daily_instruments_instrument",
        ),
        Index("idx_ori_daily_snapshot_date", "snapshot_date"),
        Index("idx_ori_daily_recording_date", "recording_date"),
        Index("idx_ori_daily_doc_type", "doc_type"),
        Index("idx_ori_daily_facc_doc_type", "facc_doc_type"),
    )
