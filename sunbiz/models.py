from __future__ import annotations

import datetime as dt

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class IngestFile(Base):
    __tablename__ = "ingest_files"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_system: Mapped[str] = mapped_column(String(32), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    relative_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_modified_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    discovered_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )
    loaded_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    loader_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "source_system", "relative_path", name="uq_ingest_files_source_path"
        ),
        Index("idx_ingest_files_source_category", "source_system", "category"),
    )


class SunbizRawRecord(Base):
    __tablename__ = "sunbiz_raw_records"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    record_type: Mapped[str | None] = mapped_column(String(8), nullable=True)
    doc_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    raw_line: Mapped[str] = mapped_column(Text, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "file_id",
            "source_member",
            "line_number",
            name="uq_sunbiz_raw_records_file_member_line",
        ),
        Index("idx_sunbiz_raw_records_doc", "doc_number"),
        Index("idx_sunbiz_raw_records_type", "record_type"),
    )


class SunbizFlrFiling(Base):
    __tablename__ = "sunbiz_flr_filings"

    doc_number: Mapped[str] = mapped_column(String(32), primary_key=True)
    filing_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    filing_status: Mapped[str | None] = mapped_column(String(8), nullable=True)
    filing_type: Mapped[str | None] = mapped_column(String(8), nullable=True)
    assessment_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    cancellation_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    trans_utility: Mapped[bool | None] = mapped_column(nullable=True)
    filing_event_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_debtor_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_secured_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_debtor_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_secured_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (Index("idx_sunbiz_flr_filings_status", "filing_status"),)


class SunbizFlrParty(Base):
    __tablename__ = "sunbiz_flr_parties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    doc_number: Mapped[str] = mapped_column(String(32), nullable=False)
    party_role: Mapped[str] = mapped_column(String(8), nullable=False)  # debtor|secured
    filing_type: Mapped[str | None] = mapped_column(String(8), nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    name_format: Mapped[str | None] = mapped_column(String(8), nullable=True)
    address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    sequence_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    relation_to_filing: Mapped[str | None] = mapped_column(String(8), nullable=True)
    original_party: Mapped[str | None] = mapped_column(String(8), nullable=True)
    filing_status: Mapped[str | None] = mapped_column(String(8), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "doc_number",
            "party_role",
            "sequence_number",
            "relation_to_filing",
            "name",
            "filing_status",
            name="uq_sunbiz_flr_parties_doc_role_seq_name",
        ),
        Index("idx_sunbiz_flr_parties_doc", "doc_number"),
        Index("idx_sunbiz_flr_parties_name", "name"),
    )


class SunbizFlrEvent(Base):
    __tablename__ = "sunbiz_flr_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_doc_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_orig_doc_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_action_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_sequence_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    action_sequence_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    action_verbage: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    action_zip: Mapped[str | None] = mapped_column(String(16), nullable=True)
    action_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    action_old_name_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_new_name_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_name_type: Mapped[str | None] = mapped_column(String(8), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "event_doc_number",
            "event_sequence_number",
            "action_sequence_number",
            "action_code",
            "action_name",
            name="uq_sunbiz_flr_events_identity",
        ),
        Index("idx_sunbiz_flr_events_doc", "event_doc_number"),
    )


class SunbizEntityFiling(Base):
    __tablename__ = "sunbiz_entity_filings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dataset_type: Mapped[str] = mapped_column(String(16), nullable=False)  # cor|gen
    doc_number: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    filing_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    filed_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    effective_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    cancellation_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    fei_number: Mapped[str | None] = mapped_column(String(16), nullable=True)
    state_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    principal_address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    principal_address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    principal_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    principal_state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    principal_zip: Mapped[str | None] = mapped_column(String(16), nullable=True)
    principal_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    mailing_address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    mailing_address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    mailing_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    mailing_state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    mailing_zip: Mapped[str | None] = mapped_column(String(16), nullable=True)
    mailing_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )
    raw_fields: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "dataset_type",
            "doc_number",
            name="uq_sunbiz_entity_filings_dataset_doc",
        ),
        Index("idx_sunbiz_entity_filings_name", "entity_name"),
        Index("idx_sunbiz_entity_filings_type", "filing_type"),
        Index("idx_sunbiz_entity_filings_status", "status"),
    )


class SunbizEntityParty(Base):
    __tablename__ = "sunbiz_entity_parties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dataset_type: Mapped[str] = mapped_column(String(16), nullable=False)  # cor|gen
    doc_number: Mapped[str] = mapped_column(String(32), nullable=False)
    party_role: Mapped[str | None] = mapped_column(String(32), nullable=True)
    party_title: Mapped[str | None] = mapped_column(String(32), nullable=True)
    party_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    party_name_format: Mapped[str | None] = mapped_column(String(8), nullable=True)
    party_corp_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    party_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_type",
            "doc_number",
            "party_role",
            "party_sequence",
            "party_name",
            name="uq_sunbiz_entity_parties_identity",
        ),
        Index("idx_sunbiz_entity_parties_doc", "doc_number"),
        Index("idx_sunbiz_entity_parties_name", "party_name"),
    )


class SunbizEntityEvent(Base):
    __tablename__ = "sunbiz_entity_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dataset_type: Mapped[str] = mapped_column(String(16), nullable=False)  # cor|gen
    event_doc_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_orig_doc_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_sequence_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_effective_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    event_filing_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    event_cancellation_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    event_expiration_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    event_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_member: Mapped[str] = mapped_column(Text, nullable=False)
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_type",
            "event_doc_number",
            "event_sequence_number",
            "event_code",
            "event_filing_date",
            name="uq_sunbiz_entity_events_identity",
        ),
        Index("idx_sunbiz_entity_events_doc", "event_doc_number"),
    )


class HcpaLatLon(Base):
    __tablename__ = "hcpa_latlon"

    folio: Mapped[str] = mapped_column(String(32), primary_key=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )


class HcpaBulkParcel(Base):
    __tablename__ = "hcpa_bulk_parcels"

    folio: Mapped[str] = mapped_column(String(32), primary_key=True)
    pin: Mapped[str | None] = mapped_column(String(64), nullable=True)
    strap: Mapped[str | None] = mapped_column(String(64), nullable=True)
    owner_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    property_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    land_use: Mapped[str | None] = mapped_column(String(64), nullable=True)
    land_use_desc: Mapped[str | None] = mapped_column(Text, nullable=True)
    year_built: Mapped[int | None] = mapped_column(Integer, nullable=True)
    beds: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    baths: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    stories: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    units: Mapped[int | None] = mapped_column(Integer, nullable=True)
    buildings: Mapped[int | None] = mapped_column(Integer, nullable=True)
    heated_area: Mapped[float | None] = mapped_column(Numeric(18, 3), nullable=True)
    lot_size: Mapped[float | None] = mapped_column(Numeric(18, 3), nullable=True)
    assessed_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    market_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    just_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    land_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    building_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    extra_features_value: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    taxable_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    last_sale_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    last_sale_price: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    raw_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    raw_sub: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_taxdist: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_muni: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_legal1: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_legal2: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_legal3: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_legal4: Mapped[str | None] = mapped_column(Text, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        Index("idx_hcpa_bulk_parcels_strap", "strap"),
        Index("idx_hcpa_bulk_parcels_owner", "owner_name"),
        Index("idx_hcpa_bulk_parcels_address", "property_address"),
    )


class HcpaParcelDorName(Base):
    __tablename__ = "hcpa_parcel_dor_names"

    dor_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )


class HcpaParcelSubName(Base):
    __tablename__ = "hcpa_parcel_sub_names"

    sub_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    sub_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    plat_bk: Mapped[str | None] = mapped_column(String(32), nullable=True)
    page: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (Index("idx_hcpa_parcel_sub_names_name", "sub_name"),)


class HcpaAllSale(Base):
    __tablename__ = "hcpa_allsales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pin: Mapped[str | None] = mapped_column(String(64), nullable=True)
    folio: Mapped[str | None] = mapped_column(String(32), nullable=True)
    dor_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    nbhc: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sale_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    vacant_improved: Mapped[str | None] = mapped_column(String(16), nullable=True)
    qualification_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    reason_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    sale_amount: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    sub_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    street_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sale_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    or_book: Mapped[str | None] = mapped_column(String(32), nullable=True)
    or_page: Mapped[str | None] = mapped_column(String(32), nullable=True)
    grantor: Mapped[str | None] = mapped_column(Text, nullable=True)
    grantee: Mapped[str | None] = mapped_column(Text, nullable=True)
    doc_num: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_allsales_file_line",
        ),
        Index("idx_hcpa_allsales_folio", "folio"),
        Index("idx_hcpa_allsales_doc_num", "doc_num"),
        Index("idx_hcpa_allsales_sale_date", "sale_date"),
        Index("idx_hcpa_allsales_pin", "pin"),
    )


class HcpaSubdivision(Base):
    __tablename__ = "hcpa_subdivisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    object_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    legal1: Mapped[str | None] = mapped_column(Text, nullable=True)
    sub_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    plat_bk: Mapped[str | None] = mapped_column(String(32), nullable=True)
    page: Mapped[str | None] = mapped_column(String(32), nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    shape_star: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    shape_stle: Mapped[float | None] = mapped_column(Numeric(20, 6), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_subdivisions_file_line",
        ),
        Index("idx_hcpa_subdivisions_sub_code", "sub_code"),
        Index("idx_hcpa_subdivisions_legal1", "legal1"),
    )


class HcpaSpecialDistrictTif(Base):
    __tablename__ = "hcpa_special_district_tifs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tif_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    perimeter: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_special_tifs_file_line",
        ),
        Index("idx_hcpa_special_tifs_code", "tif_code"),
    )


class HcpaSpecialDistrictCdd(Base):
    __tablename__ = "hcpa_special_district_cdds"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cdd_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    perimeter: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_special_cdds_file_line",
        ),
        Index("idx_hcpa_special_cdds_code", "cdd_code"),
    )


class HcpaSpecialDistrictSd(Base):
    __tablename__ = "hcpa_special_district_sd"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sp_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    ord_value: Mapped[str | None] = mapped_column(String(32), nullable=True)
    dist_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    dist_num: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dist_tp: Mapped[str | None] = mapped_column(String(16), nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    perimeter: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_special_sd_file_line",
        ),
        Index("idx_hcpa_special_sd_num", "dist_num"),
    )


class HcpaSpecialDistrictSd2(Base):
    __tablename__ = "hcpa_special_district_sd2"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sd_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    sp_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    perimeter: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_special_sd2_file_line",
        ),
        Index("idx_hcpa_special_sd2_code", "sd_code"),
    )


class HcpaSpecialDistrictLd(Base):
    __tablename__ = "hcpa_special_district_lds"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ld_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    area: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    perimeter: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    source_line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_file_id",
            "source_line_number",
            name="uq_hcpa_special_lds_file_line",
        ),
        Index("idx_hcpa_special_lds_code", "ld_code"),
    )


class DorNalParcel(Base):
    """Florida Department of Revenue NAL (Name-Address-Legal) parcel data.

    Key fields for foreclosure analysis: homestead exemption status,
    assessed/taxable values, millage rates, and exemption flags.
    Source: https://floridarevenue.com/property/dataportal/
    """

    __tablename__ = "dor_nal_parcels"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    county_code: Mapped[str] = mapped_column(String(4), nullable=False)
    parcel_id: Mapped[str] = mapped_column(String(40), nullable=False)
    folio: Mapped[str | None] = mapped_column(String(32), nullable=True)
    strap: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)

    # Owner info
    owner_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_address1: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_address2: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_state: Mapped[str | None] = mapped_column(String(32), nullable=True)
    owner_zip: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Property situs
    property_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Classification
    property_use_code: Mapped[str | None] = mapped_column(String(8), nullable=True)

    # Valuation
    just_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    just_value_homestead: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    assessed_value_school: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    assessed_value_nonschool: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    assessed_value_homestead: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    taxable_value_school: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    taxable_value_nonschool: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )

    # Exemptions
    homestead_exempt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    homestead_exempt_value: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    widow_exempt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    widow_exempt_value: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    disability_exempt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    disability_exempt_value: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    veteran_exempt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    veteran_exempt_value: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )
    ag_exempt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    ag_exempt_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    soh_differential: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)

    # Millage rates
    total_millage: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    county_millage: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    school_millage: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    city_millage: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)

    # Computed
    estimated_annual_tax: Mapped[float | None] = mapped_column(
        Numeric(18, 2), nullable=True
    )

    # Legal description
    legal_description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Source tracking
    source_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_file_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_files.id", ondelete="CASCADE"), nullable=False
    )
    loaded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "county_code", "parcel_id", "tax_year",
            name="uq_dor_nal_parcels_county_parcel_year",
        ),
        Index("idx_dor_nal_parcels_folio", "folio"),
        Index("idx_dor_nal_parcels_strap", "strap"),
        Index("idx_dor_nal_parcels_parcel_id", "parcel_id"),
        Index("idx_dor_nal_parcels_zip_code", "zip_code"),
        Index(
            "idx_dor_nal_parcels_homestead",
            "homestead_exempt",
            postgresql_where=text("homestead_exempt = true"),
        ),
        Index("idx_dor_nal_parcels_tax_year", "tax_year"),
    )


class PropertyMarket(Base):
    """Consolidated market snapshot — one row per property (strap).

    Merges best-of data from Zillow, Redfin, and HomeHarvest into a single
    row for fast dashboard reads.  Locally downloaded photo paths stored in
    ``photo_local_paths`` JSONB array.
    """

    __tablename__ = "property_market"

    strap: Mapped[str] = mapped_column(String(64), primary_key=True)
    folio: Mapped[str | None] = mapped_column(String(32), nullable=True)
    case_number: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Valuation
    zestimate: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    rent_zestimate: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    list_price: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    tax_assessed_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)

    # Specs
    beds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    baths: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    sqft: Mapped[int | None] = mapped_column(Integer, nullable=True)
    year_built: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_size: Mapped[str | None] = mapped_column(Text, nullable=True)
    property_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Listing
    listing_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    detail_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Photos
    photo_local_paths: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, server_default=text("'[]'::jsonb")
    )
    photo_cdn_urls: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, server_default=text("'[]'::jsonb")
    )

    # Per-source raw JSON
    zillow_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    redfin_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    homeharvest_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    primary_source: Mapped[str | None] = mapped_column(String(16), nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        Index("idx_property_market_folio", "folio"),
        Index("idx_property_market_case", "case_number"),
    )


class CountyPermit(Base):
    """Hillsborough county permit records sourced from ArcGIS layer 0."""

    __tablename__ = "county_permits"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    permit_number: Mapped[str] = mapped_column(String(64), nullable=False)

    # Source identity
    source_layer_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Location / linkage
    folio_raw: Mapped[str | None] = mapped_column(String(32), nullable=True)
    folio_clean: Mapped[str | None] = mapped_column(String(16), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Permit details
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    category: Mapped[str | None] = mapped_column(String(32), nullable=True)
    permit_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    type2: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    occupancy_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    occupancy_category: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Sizes / counts
    bedrooms: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    bathrooms: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    house_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sf_living: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    sf_cover: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    sf_total: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    permit_value: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)

    # Timeline
    issue_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    complete_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    combined_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)

    # Source deep-link
    aca_link: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_ingested_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint(
            "source_layer_id",
            "source_object_id",
            name="uq_county_permits_layer_object",
        ),
        Index("idx_county_permits_permit_number", "permit_number"),
        Index("idx_county_permits_folio_clean", "folio_clean"),
        Index("idx_county_permits_issue_date", "issue_date"),
        Index("idx_county_permits_complete_date", "complete_date"),
        Index("idx_county_permits_combined_date", "combined_date"),
        Index("idx_county_permits_category", "category"),
        Index("idx_county_permits_status", "status"),
    )


class TampaAccelaRecord(Base):
    """Tampa Accela records captured from Building module exports."""

    __tablename__ = "tampa_accela_records"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Source identity
    record_number: Mapped[str] = mapped_column(String(64), nullable=False)
    record_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    record_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    module: Mapped[str | None] = mapped_column(String(32), nullable=True)
    short_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    project_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Address normalization
    address_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    address_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state: Mapped[str | None] = mapped_column(String(8), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Key analysis flags
    is_violation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_closeout: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_fix_record: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Estimated work cost and enrichment metadata
    estimated_work_cost: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    estimated_cost_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    detail_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    expiration_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)

    # Source capture metadata
    source_start_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    source_end_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    source_query_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_csv_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_export_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    source_ingested_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: dt.datetime.now(dt.UTC)
    )

    __table_args__ = (
        UniqueConstraint("record_number", name="uq_tampa_accela_records_record_number"),
        Index("idx_tampa_accela_records_record_date", "record_date"),
        Index("idx_tampa_accela_records_module", "module"),
        Index("idx_tampa_accela_records_status", "status"),
        Index("idx_tampa_accela_records_zip_code", "zip_code"),
        Index("idx_tampa_accela_records_is_violation", "is_violation"),
        Index("idx_tampa_accela_records_needs_closeout", "needs_closeout"),
        Index("idx_tampa_accela_records_is_fix_record", "is_fix_record"),
        Index("idx_tampa_accela_records_estimated_work_cost", "estimated_work_cost"),
        Index("idx_tampa_accela_records_source_window", "source_start_date", "source_end_date"),
    )
