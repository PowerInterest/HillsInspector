from __future__ import annotations

import datetime as dt

from sqlalchemy import BigInteger
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
