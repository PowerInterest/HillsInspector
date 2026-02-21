"""Trust account movement analysis service.

This service:
1. Discovers published trust/escrow PDF reports from Hills Clerk Civil endpoints.
2. Downloads any new reports.
3. Parses case-level balances from:
   - real_auction_balances (money in escrow)
   - registry_trust_balances (registry/trust balances)
4. Computes movement against prior report snapshots (entered/changed/stable/dropped).
5. Aligns real-auction entry amounts to prior-business-day winning bids from PG datasets.
6. Classifies counterparties (bank / third_party_bidder / unknown).
7. Upserts analysis rows into `TrustAccount` and aggregates into
   `TrustAccountSummary`.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import fitz
import requests
from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sunbiz.db import get_engine, resolve_pg_dsn

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


CASE_NUMBER_RE = re.compile(r"^\d{2}-[A-Z]{2}-\d{6}$")
REPORT_DATE_RE = re.compile(r"as_of_[A-Za-z]{3}_(\d{2})_(\d{2})_(\d{4})", re.IGNORECASE)
INDEX_LINK_RE = re.compile(r'<A HREF="([^"]+)"', re.IGNORECASE)
REAL_PDF_PREFIX = "Realauction_Mortgage_Foreclosure_Balances_as_of_"
REGISTRY_PDF_PREFIX = "Registry_and_TrustAccounts_Balances_as_of_"

REAL_INDEX_URL = "https://publicrec.hillsclerk.com/Civil/real_auction_balances/"
REGISTRY_INDEX_URL = "https://publicrec.hillsclerk.com/Civil/registry_trust_balances/"

AMOUNT_LINE_RE = re.compile(r"^-?\d{1,3}(?:,\d{3})*\.\d{2}$")
AMOUNT_ANY_RE = re.compile(r"\$?(-?\d{1,3}(?:,\d{3})*\.\d{2})")
DATE_LINE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")

BANK_KEYWORDS = (
    "BANK",
    "MORTGAGE",
    "CREDIT UNION",
    "FEDERAL NATIONAL",
    "FANNIE MAE",
    "FREDDIE MAC",
    "NATIONAL ASSOCIATION",
    "U.S. BANK",
    "US BANK",
    "WELLS FARGO",
    "JPMORGAN",
    "CHASE",
    "NATIONSTAR",
    "NEWREZ",
    "PENNYMAC",
    "TRUIST",
    "REGIONS",
    "CITIBANK",
    "DEUTSCHE",
    "LOAN",
)

LEGAL_SUFFIXES = {
    "LLC",
    "INC",
    "CORP",
    "CORPORATION",
    "LTD",
    "LP",
    "LLP",
    "LLLP",
    "CO",
    "COMPANY",
    "TRUST",
    "HOLDINGS",
    "HOLDING",
    "PROPERTIES",
    "PROPERTY",
    "INVESTMENTS",
    "INVESTMENT",
    "GROUP",
    "FUND",
}

REAL_NOISE_EXACT = {
    "Case Number",
    "Case Style",
    "Recipient",
    "Hold",
    "Amt As of",
    "Amt to Disburse",
    "Wells Fargo",
    "In Escrow Since",
    "2408 MORTGAGE FORECLOSURE",
    "Confidential address/phone displayed",
    "Money In Escrow",
    "FLHILLSBPROD",
    "Include:",
    "ALL",
    "Page",
    "of",
}
REAL_NOISE_PREFIX = (
    "Fee Codes:",
    "Printed on",
)


@dataclass(slots=True)
class Snapshot:
    source: str
    report_date: str
    case_number: str
    amount: float | None
    in_escrow_since: str | None
    multiple_recipients: bool
    has_negative: bool
    has_offset_pair: bool
    max_abs_amount: float | None
    division_codes: list[str]
    registry_net_sum: float | None
    plaintiff_name: str | None
    raw_payload: dict[str, Any]


class TrustAccountsService:
    def __init__(
        self,
        pg_dsn: str | None = None,
        download_dir: str = "data/tmp/trust_accounts",
        request_timeout: int = 20,
    ):
        self.pg_dsn = resolve_pg_dsn(pg_dsn)
        self._engine = get_engine(self.pg_dsn)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.request_timeout = request_timeout

    def run(self, force_reprocess: bool = False) -> dict[str, Any]:
        logger.info("TrustAccounts: starting analysis run")

        history_bids_by_date = self._load_history_winning_bids()
        third_party_exact, third_party_core = self._load_known_third_party_bidders()
        source_to_reports = {
            "real": self._discover_reports(REAL_INDEX_URL, REAL_PDF_PREFIX),
            "registry": self._discover_reports(REGISTRY_INDEX_URL, REGISTRY_PDF_PREFIX),
        }

        summary: dict[str, Any]
        with self._engine.begin() as conn:
            self._ensure_schema(conn)
            upcoming_context = self._load_upcoming_auction_context(conn)

            summary = {
                "processed": {"real": 0, "registry": 0},
                "available_report_dates": {
                    "real": sorted(source_to_reports["real"]),
                    "registry": sorted(source_to_reports["registry"]),
                },
                "new_report_dates": {"real": [], "registry": []},
                "rows_upserted": 0,
                "rows_deleted": 0,
                "summary_rows_written": 0,
                "known_third_party_entities": len(third_party_exact),
                "upcoming_case_links": len(upcoming_context),
            }

            for source in ("real", "registry"):
                available_dates = sorted(source_to_reports[source])
                if force_reprocess:
                    target_dates = available_dates
                else:
                    processed_dates = self._get_processed_dates(conn, source)
                    target_dates = [d for d in available_dates if d not in processed_dates]

                summary["new_report_dates"][source] = target_dates

                for report_date in target_dates:
                    url = source_to_reports[source][report_date]
                    pdf_path = self._download_report(source, report_date, url)

                    snapshots = (
                        self._parse_real_report(report_date, pdf_path)
                        if source == "real"
                        else self._parse_registry_report(report_date, pdf_path)
                    )

                    rows_deleted = conn.execute(
                        text(
                            """
                            DELETE FROM "TrustAccount"
                            WHERE source = :source
                              AND report_date = :report_date
                            """
                        ),
                        {"source": source, "report_date": report_date},
                    ).rowcount
                    summary["rows_deleted"] += int(rows_deleted or 0)

                    previous = self._get_previous_snapshot_amounts(conn, source, report_date)
                    current_cases = set(snapshots)
                    previous_cases = set(previous)

                    upsert_rows = 0
                    for case_number, snap in snapshots.items():
                        previous_amount = previous.get(case_number)
                        movement_type, delta_amount = self._movement_for_case(
                            previous_amount,
                            snap.amount,
                        )

                        upcoming = upcoming_context.get(case_number)
                        plaintiff_name = None
                        upcoming_auction_date = None
                        match_upcoming_auction = 0
                        if upcoming:
                            plaintiff_name = upcoming.get("plaintiff")
                            upcoming_auction_date = upcoming.get("auction_date")
                            match_upcoming_auction = 1
                        elif snap.plaintiff_name:
                            plaintiff_name = snap.plaintiff_name

                        counterparty_type = self._classify_counterparty(
                            plaintiff_name,
                            third_party_exact,
                            third_party_core,
                        )

                        winning_bid_date = None
                        winning_bid_match_count = None
                        winning_bid_amount = None
                        days_before_winning_auction = None
                        is_pre_auction_signal = None

                        if source == "real":
                            prior_biz = self._prior_business_day(report_date)
                            match_count = self._winning_bid_match_count(
                                history_bids_by_date,
                                prior_biz,
                                snap.amount,
                            )
                            if match_count > 0:
                                winning_bid_date = prior_biz
                                winning_bid_match_count = match_count
                                winning_bid_amount = snap.amount

                                if snap.in_escrow_since:
                                    escrow_date = self._parse_iso_date(snap.in_escrow_since)
                                    auction_date = self._parse_iso_date(prior_biz)
                                    if escrow_date and auction_date:
                                        days_before_winning_auction = (
                                            auction_date - escrow_date
                                        ).days
                                        is_pre_auction_signal = int(
                                            days_before_winning_auction > 0
                                        )

                        upsert_rows += self._upsert_row(
                            conn=conn,
                            snapshot=snap,
                            movement_type=movement_type,
                            previous_amount=previous_amount,
                            delta_amount=delta_amount,
                            plaintiff_name=plaintiff_name,
                            counterparty_type=counterparty_type,
                            match_upcoming_auction=match_upcoming_auction,
                            upcoming_auction_date=upcoming_auction_date,
                            winning_bid_date=winning_bid_date,
                            winning_bid_match_count=winning_bid_match_count,
                            winning_bid_amount=winning_bid_amount,
                            days_before_winning_auction=days_before_winning_auction,
                            is_pre_auction_signal=is_pre_auction_signal,
                        )

                    dropped_cases = previous_cases - current_cases
                    for case_number in sorted(dropped_cases):
                        upsert_rows += self._upsert_dropped_row(
                            conn=conn,
                            source=source,
                            report_date=report_date,
                            case_number=case_number,
                            previous_amount=previous[case_number],
                        )

                    summary_rows = self._refresh_summary_for_date(conn, source, report_date)

                    summary["summary_rows_written"] += summary_rows
                    summary["rows_upserted"] += upsert_rows
                    summary["processed"][source] += 1

        logger.info(
            "TrustAccounts: complete (processed real={}, registry={}, rows_upserted={})",
            summary["processed"]["real"],
            summary["processed"]["registry"],
            summary["rows_upserted"],
        )
        return summary

    def _ensure_schema(self, conn: Connection) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "TrustAccount" (
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    case_number TEXT NOT NULL,
                    movement_type TEXT NOT NULL,
                    amount DOUBLE PRECISION,
                    previous_amount DOUBLE PRECISION,
                    delta_amount DOUBLE PRECISION,
                    in_escrow_since DATE,
                    multiple_recipients INTEGER,
                    has_negative INTEGER,
                    has_offset_pair INTEGER,
                    max_abs_amount DOUBLE PRECISION,
                    division_codes TEXT,
                    registry_net_sum DOUBLE PRECISION,
                    plaintiff_name TEXT,
                    counterparty_type TEXT,
                    match_upcoming_auction INTEGER,
                    upcoming_auction_date DATE,
                    winning_bid_date DATE,
                    winning_bid_match_count INTEGER,
                    winning_bid_amount DOUBLE PRECISION,
                    days_before_winning_auction INTEGER,
                    is_pre_auction_signal INTEGER,
                    raw_payload TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(source, report_date, case_number, movement_type)
                )
                """
            )
        )

        self._ensure_column(conn, "TrustAccount", "plaintiff_name", "TEXT")
        self._ensure_column(conn, "TrustAccount", "counterparty_type", "TEXT")
        self._ensure_column(conn, "TrustAccount", "match_upcoming_auction", "INTEGER")
        self._ensure_column(conn, "TrustAccount", "upcoming_auction_date", "DATE")

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "TrustAccountSummary" (
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    scope TEXT NOT NULL,
                    counterparty_type TEXT NOT NULL,
                    case_count INTEGER NOT NULL,
                    total_amount DOUBLE PRECISION NOT NULL,
                    avg_amount DOUBLE PRECISION,
                    max_amount DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(source, report_date, scope, counterparty_type)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trust_account_source_date
                ON "TrustAccount"(source, report_date)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trust_account_case
                ON "TrustAccount"(case_number)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_trust_account_summary_date
                ON "TrustAccountSummary"(source, report_date)
                """
            )
        )

    def _ensure_column(
        self,
        conn: Connection,
        table_name: str,
        column_name: str,
        column_type: str,
    ) -> None:
        exists = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                  AND column_name = :column_name
                LIMIT 1
                """
            ),
            {
                "table_name": table_name,
                "column_name": column_name,
            },
        ).first()
        if exists:
            return
        conn.execute(
            text(
                f'ALTER TABLE "{table_name}" ADD COLUMN {column_name} {column_type}'
            )
        )

    def _discover_reports(self, index_url: str, prefix: str) -> dict[str, str]:
        logger.info("TrustAccounts: discovering reports at {}", index_url)
        response = requests.get(index_url, timeout=self.request_timeout)
        response.raise_for_status()
        html = response.text

        reports: dict[str, str] = {}
        for href in INDEX_LINK_RE.findall(html):
            if not href.lower().endswith(".pdf"):
                continue
            file_name = unquote(href.rsplit("/", 1)[-1])
            if not file_name.startswith(prefix):
                continue

            report_date = self._report_date_from_filename(file_name)
            if not report_date:
                continue

            absolute_url = (
                f"https://publicrec.hillsclerk.com{href}"
                if href.startswith("/")
                else f"{index_url.rstrip('/')}/{href}"
            )
            reports[report_date] = absolute_url

        return reports

    def _report_date_from_filename(self, file_name: str) -> str | None:
        match = REPORT_DATE_RE.search(file_name)
        if not match:
            return None
        month, day, year = match.groups()
        try:
            parsed = date(int(year), int(month), int(day))
        except ValueError:
            return None
        return parsed.isoformat()

    def _download_report(self, source: str, report_date: str, url: str) -> Path:
        file_name = unquote(url.rsplit("/", 1)[-1])
        out_path = self.download_dir / f"{source}_{report_date}_{file_name}"
        if out_path.exists():
            return out_path

        logger.info(
            "TrustAccounts: downloading report source={} report_date={} url={} path={}",
            source,
            report_date,
            url,
            out_path,
        )
        response = requests.get(url, timeout=self.request_timeout)
        response.raise_for_status()
        out_path.write_bytes(response.content)
        return out_path

    def _parse_real_report(self, report_date: str, pdf_path: Path) -> dict[str, Snapshot]:
        doc = fitz.open(pdf_path)
        case_lines: dict[str, list[str]] = defaultdict(list)
        current_case: str | None = None

        for page in doc:
            for raw in page.get_text("text").splitlines():
                line = raw.strip()
                if not line:
                    continue

                if CASE_NUMBER_RE.match(line):
                    current_case = line
                    continue

                if self._is_real_noise(line):
                    continue

                if current_case is not None:
                    case_lines[current_case].append(line)

        doc.close()

        snapshots: dict[str, Snapshot] = {}
        for case_number, lines in case_lines.items():
            amounts = [
                float(token.replace(",", ""))
                for token in lines
                if AMOUNT_LINE_RE.match(token)
            ]
            disburse_values: list[float] = []
            for idx in range(0, len(amounts) - 1, 2):
                disburse_values.append(amounts[idx + 1])
            if not disburse_values and amounts:
                disburse_values = [amounts[0]]

            amount = disburse_values[0] if disburse_values else None
            in_escrow_since = self._extract_min_iso_date(lines)

            has_negative = any(val < 0 for val in disburse_values)
            rounded_values = {round(val, 2) for val in disburse_values}
            has_offset_pair = any(
                (-val) in rounded_values for val in rounded_values if val != 0
            )
            max_abs_amount = max((abs(val) for val in disburse_values), default=None)
            plaintiff_name = self._extract_plaintiff_name(lines)

            snapshots[case_number] = Snapshot(
                source="real",
                report_date=report_date,
                case_number=case_number,
                amount=amount,
                in_escrow_since=in_escrow_since,
                multiple_recipients=any("Multiple Recipients" in line for line in lines),
                has_negative=has_negative,
                has_offset_pair=has_offset_pair,
                max_abs_amount=max_abs_amount,
                division_codes=[],
                registry_net_sum=None,
                plaintiff_name=plaintiff_name,
                raw_payload={"lines": lines[:120], "line_count": len(lines)},
            )

        return snapshots

    def _parse_registry_report(
        self,
        report_date: str,
        pdf_path: Path,
    ) -> dict[str, Snapshot]:
        doc = fitz.open(pdf_path)

        case_to_net_sum: dict[str, float] = defaultdict(float)
        case_to_divisions: dict[str, set[str]] = defaultdict(set)
        case_to_rows: dict[str, int] = defaultdict(int)
        current_division: str | None = None

        for page in doc:
            lines = self._extract_lines_from_words(page)
            for line in lines:
                if not line:
                    continue
                if line.startswith("Printed on"):
                    continue
                if line.startswith("As of Date:"):
                    continue
                if line.startswith("Registry & Trust Accounts With Balances"):
                    continue
                if line.startswith("Case Number Party Name Increases Decreases Net Credit Balance"):
                    continue

                division_match = re.match(r"^(\d{4})\s+(.+)$", line)
                if division_match and not CASE_NUMBER_RE.match(line.split()[0]):
                    current_division = division_match.group(1)
                    continue

                case_match = re.match(r"^(\d{2}-[A-Z]{2}-\d{6})\s+", line)
                if not case_match:
                    continue

                case_number = case_match.group(1)
                amount_tokens = [
                    float(token.replace(",", ""))
                    for token in AMOUNT_ANY_RE.findall(line)
                ]
                if not amount_tokens:
                    continue

                net_credit = amount_tokens[-1]
                case_to_net_sum[case_number] += net_credit
                case_to_rows[case_number] += 1
                if current_division:
                    case_to_divisions[case_number].add(current_division)

        doc.close()

        snapshots: dict[str, Snapshot] = {}
        for case_number, net_sum in case_to_net_sum.items():
            divisions = sorted(case_to_divisions.get(case_number, set()))
            snapshots[case_number] = Snapshot(
                source="registry",
                report_date=report_date,
                case_number=case_number,
                amount=round(net_sum, 2),
                in_escrow_since=None,
                multiple_recipients=False,
                has_negative=net_sum < 0,
                has_offset_pair=False,
                max_abs_amount=abs(net_sum),
                division_codes=divisions,
                registry_net_sum=round(net_sum, 2),
                plaintiff_name=None,
                raw_payload={
                    "row_count": case_to_rows.get(case_number, 0),
                    "divisions": divisions,
                },
            )

        return snapshots

    def _extract_plaintiff_name(self, lines: list[str]) -> str | None:
        style_parts: list[str] = []
        for line in lines:
            if line in {"CLERK", "Multiple Recipients"}:
                break
            if DATE_LINE_RE.match(line):
                continue
            if AMOUNT_LINE_RE.match(line):
                continue
            style_parts.append(line)

        if not style_parts:
            return None

        style_text = " ".join(style_parts)
        if not style_text.strip():
            return None

        parts = re.split(r"\bvs\.?\b", style_text, maxsplit=1, flags=re.IGNORECASE)
        if not parts:
            return None

        plaintiff = parts[0].strip(" ,")
        return plaintiff or None

    def _extract_lines_from_words(self, page: fitz.Page) -> list[str]:
        words = page.get_text("words")
        by_y: dict[float, list[tuple[float, str]]] = defaultdict(list)
        for word in words:
            by_y[round(word[1], 1)].append((word[0], word[4]))

        lines: list[str] = []
        for y in sorted(by_y):
            tokens = [token for _, token in sorted(by_y[y], key=lambda item: item[0])]
            line = " ".join(tokens).strip()
            if line:
                lines.append(line)
        return lines

    def _is_real_noise(self, line: str) -> bool:
        if line in REAL_NOISE_EXACT:
            return True
        if any(line.startswith(prefix) for prefix in REAL_NOISE_PREFIX):
            return True
        return bool(line.isdigit())

    def _extract_min_iso_date(self, lines: list[str]) -> str | None:
        parsed: list[date] = []
        for line in lines:
            if not DATE_LINE_RE.match(line):
                continue
            try:
                parsed.append(datetime.strptime(line, "%m/%d/%Y").date())
            except ValueError:
                continue
        if not parsed:
            return None
        return min(parsed).isoformat()

    def _get_processed_dates(self, conn: Connection, source: str) -> set[str]:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT report_date
                FROM "TrustAccount"
                WHERE source = :source
                """
            ),
            {"source": source},
        ).fetchall()
        return {str(row[0]) for row in rows if row[0] is not None}

    def _get_previous_snapshot_amounts(
        self,
        conn: Connection,
        source: str,
        report_date: str,
    ) -> dict[str, float | None]:
        prior_row = conn.execute(
            text(
                """
                SELECT MAX(report_date)
                FROM "TrustAccount"
                WHERE source = :source
                  AND report_date < :report_date
                """
            ),
            {"source": source, "report_date": report_date},
        ).fetchone()

        prior_date = prior_row[0] if prior_row else None
        if not prior_date:
            return {}

        rows = conn.execute(
            text(
                """
                SELECT case_number, amount
                FROM "TrustAccount"
                WHERE source = :source
                  AND report_date = :prior_date
                  AND movement_type != 'dropped'
                """
            ),
            {"source": source, "prior_date": prior_date},
        ).mappings().all()

        result: dict[str, float | None] = {}
        for row in rows:
            result[str(row["case_number"])] = (
                float(row["amount"]) if row["amount"] is not None else None
            )
        return result

    def _movement_for_case(
        self,
        previous_amount: float | None,
        current_amount: float | None,
    ) -> tuple[str, float | None]:
        if previous_amount is None:
            return "entered", None
        if current_amount is None:
            return "stable", None

        delta = round(current_amount - previous_amount, 2)
        if delta == 0:
            return "stable", 0.0
        return "changed", delta

    def _upsert_row(
        self,
        conn: Connection,
        snapshot: Snapshot,
        movement_type: str,
        previous_amount: float | None,
        delta_amount: float | None,
        plaintiff_name: str | None,
        counterparty_type: str,
        match_upcoming_auction: int,
        upcoming_auction_date: str | None,
        winning_bid_date: str | None,
        winning_bid_match_count: int | None,
        winning_bid_amount: float | None,
        days_before_winning_auction: int | None,
        is_pre_auction_signal: int | None,
    ) -> int:
        conn.execute(
            text(
                """
                INSERT INTO "TrustAccount" (
                    source,
                    report_date,
                    case_number,
                    movement_type,
                    amount,
                    previous_amount,
                    delta_amount,
                    in_escrow_since,
                    multiple_recipients,
                    has_negative,
                    has_offset_pair,
                    max_abs_amount,
                    division_codes,
                    registry_net_sum,
                    plaintiff_name,
                    counterparty_type,
                    match_upcoming_auction,
                    upcoming_auction_date,
                    winning_bid_date,
                    winning_bid_match_count,
                    winning_bid_amount,
                    days_before_winning_auction,
                    is_pre_auction_signal,
                    raw_payload,
                    updated_at
                ) VALUES (
                    :source,
                    :report_date,
                    :case_number,
                    :movement_type,
                    :amount,
                    :previous_amount,
                    :delta_amount,
                    :in_escrow_since,
                    :multiple_recipients,
                    :has_negative,
                    :has_offset_pair,
                    :max_abs_amount,
                    :division_codes,
                    :registry_net_sum,
                    :plaintiff_name,
                    :counterparty_type,
                    :match_upcoming_auction,
                    :upcoming_auction_date,
                    :winning_bid_date,
                    :winning_bid_match_count,
                    :winning_bid_amount,
                    :days_before_winning_auction,
                    :is_pre_auction_signal,
                    :raw_payload,
                    NOW()
                )
                ON CONFLICT(source, report_date, case_number, movement_type)
                DO UPDATE SET
                    amount = EXCLUDED.amount,
                    previous_amount = EXCLUDED.previous_amount,
                    delta_amount = EXCLUDED.delta_amount,
                    in_escrow_since = EXCLUDED.in_escrow_since,
                    multiple_recipients = EXCLUDED.multiple_recipients,
                    has_negative = EXCLUDED.has_negative,
                    has_offset_pair = EXCLUDED.has_offset_pair,
                    max_abs_amount = EXCLUDED.max_abs_amount,
                    division_codes = EXCLUDED.division_codes,
                    registry_net_sum = EXCLUDED.registry_net_sum,
                    plaintiff_name = EXCLUDED.plaintiff_name,
                    counterparty_type = EXCLUDED.counterparty_type,
                    match_upcoming_auction = EXCLUDED.match_upcoming_auction,
                    upcoming_auction_date = EXCLUDED.upcoming_auction_date,
                    winning_bid_date = EXCLUDED.winning_bid_date,
                    winning_bid_match_count = EXCLUDED.winning_bid_match_count,
                    winning_bid_amount = EXCLUDED.winning_bid_amount,
                    days_before_winning_auction = EXCLUDED.days_before_winning_auction,
                    is_pre_auction_signal = EXCLUDED.is_pre_auction_signal,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()
                """
            ),
            {
                "source": snapshot.source,
                "report_date": snapshot.report_date,
                "case_number": snapshot.case_number,
                "movement_type": movement_type,
                "amount": snapshot.amount,
                "previous_amount": previous_amount,
                "delta_amount": delta_amount,
                "in_escrow_since": snapshot.in_escrow_since,
                "multiple_recipients": int(snapshot.multiple_recipients),
                "has_negative": int(snapshot.has_negative),
                "has_offset_pair": int(snapshot.has_offset_pair),
                "max_abs_amount": snapshot.max_abs_amount,
                "division_codes": json.dumps(snapshot.division_codes),
                "registry_net_sum": snapshot.registry_net_sum,
                "plaintiff_name": plaintiff_name,
                "counterparty_type": counterparty_type,
                "match_upcoming_auction": match_upcoming_auction,
                "upcoming_auction_date": upcoming_auction_date,
                "winning_bid_date": winning_bid_date,
                "winning_bid_match_count": winning_bid_match_count,
                "winning_bid_amount": winning_bid_amount,
                "days_before_winning_auction": days_before_winning_auction,
                "is_pre_auction_signal": is_pre_auction_signal,
                "raw_payload": json.dumps(snapshot.raw_payload),
            },
        )
        return 1

    def _upsert_dropped_row(
        self,
        conn: Connection,
        source: str,
        report_date: str,
        case_number: str,
        previous_amount: float | None,
    ) -> int:
        conn.execute(
            text(
                """
                INSERT INTO "TrustAccount" (
                    source,
                    report_date,
                    case_number,
                    movement_type,
                    amount,
                    previous_amount,
                    delta_amount,
                    multiple_recipients,
                    has_negative,
                    has_offset_pair,
                    counterparty_type,
                    match_upcoming_auction,
                    raw_payload,
                    updated_at
                ) VALUES (
                    :source,
                    :report_date,
                    :case_number,
                    'dropped',
                    NULL,
                    :previous_amount,
                    NULL,
                    0,
                    0,
                    0,
                    'unknown',
                    0,
                    :raw_payload,
                    NOW()
                )
                ON CONFLICT(source, report_date, case_number, movement_type)
                DO UPDATE SET
                    previous_amount = EXCLUDED.previous_amount,
                    counterparty_type = EXCLUDED.counterparty_type,
                    match_upcoming_auction = EXCLUDED.match_upcoming_auction,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()
                """
            ),
            {
                "source": source,
                "report_date": report_date,
                "case_number": case_number,
                "previous_amount": previous_amount,
                "raw_payload": json.dumps(
                    {"note": "Case present in previous report, absent in current"}
                ),
            },
        )
        return 1

    def _refresh_summary_for_date(
        self,
        conn: Connection,
        source: str,
        report_date: str,
    ) -> int:
        conn.execute(
            text(
                """
                DELETE FROM "TrustAccountSummary"
                WHERE source = :source
                  AND report_date = :report_date
                """
            ),
            {"source": source, "report_date": report_date},
        )

        written = 0
        scopes = {
            "all_cases": "",
            "upcoming_auctions": " AND match_upcoming_auction = 1",
        }

        for scope, where_clause in scopes.items():
            rows = conn.execute(
                text(
                    f"""
                    SELECT
                        COALESCE(counterparty_type, 'unknown') AS counterparty_type,
                        COUNT(*) AS case_count,
                        COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS total_amount,
                        AVG(CASE WHEN amount > 0 THEN amount END) AS avg_amount,
                        MAX(CASE WHEN amount > 0 THEN amount END) AS max_amount
                    FROM "TrustAccount"
                    WHERE source = :source
                      AND report_date = :report_date
                      AND movement_type != 'dropped'
                      {where_clause}
                    GROUP BY COALESCE(counterparty_type, 'unknown')
                    """
                ),
                {"source": source, "report_date": report_date},
            ).mappings().all()

            for row in rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO "TrustAccountSummary" (
                            source,
                            report_date,
                            scope,
                            counterparty_type,
                            case_count,
                            total_amount,
                            avg_amount,
                            max_amount,
                            updated_at
                        ) VALUES (
                            :source,
                            :report_date,
                            :scope,
                            :counterparty_type,
                            :case_count,
                            :total_amount,
                            :avg_amount,
                            :max_amount,
                            NOW()
                        )
                        ON CONFLICT(source, report_date, scope, counterparty_type)
                        DO UPDATE SET
                            case_count = EXCLUDED.case_count,
                            total_amount = EXCLUDED.total_amount,
                            avg_amount = EXCLUDED.avg_amount,
                            max_amount = EXCLUDED.max_amount,
                            updated_at = NOW()
                        """
                    ),
                    {
                        "source": source,
                        "report_date": report_date,
                        "scope": scope,
                        "counterparty_type": row["counterparty_type"],
                        "case_count": row["case_count"],
                        "total_amount": row["total_amount"],
                        "avg_amount": row["avg_amount"],
                        "max_amount": row["max_amount"],
                    },
                )
                written += 1

        return written

    def _load_history_winning_bids(self) -> dict[str, dict[float, int]]:
        try:
            with self._engine.connect() as conn:
                sources: list[str] = []
                for table in ("historical_auctions", "foreclosures_history", "foreclosures"):
                    if not self._table_exists(conn, table):
                        continue
                    if self._table_has_columns(
                        conn,
                        table,
                        {"auction_date", "auction_status", "winning_bid"},
                    ):
                        sources.append(table)
                if not sources:
                    logger.warning(
                        "TrustAccounts winning-bid context unavailable: no source tables present"
                    )
                    return {}

                union_sql = "\nUNION ALL\n".join(
                    [
                        f"""
                        SELECT auction_date::text AS auction_date, winning_bid
                        FROM {table}
                        WHERE LOWER(auction_status) = 'foreclosure sold'
                          AND winning_bid IS NOT NULL
                        """
                        for table in sources
                    ]
                )
                rows = conn.execute(text(union_sql)).mappings().all()
        except SQLAlchemyError as exc:
            logger.opt(exception=True).warning(
                "TrustAccounts winning-bid context query failed (dsn={}): {}",
                self._dsn_tag(self.pg_dsn),
                exc,
            )
            raise

        result: dict[str, dict[float, int]] = defaultdict(dict)
        for row in rows:
            auction_date = str(row["auction_date"])
            amount = round(float(row["winning_bid"]), 2)
            result[auction_date][amount] = result[auction_date].get(amount, 0) + 1

        return result

    def _load_known_third_party_bidders(self) -> tuple[set[str], set[str]]:
        try:
            with self._engine.connect() as conn:
                sources: list[str] = []
                for table in ("historical_auctions", "foreclosures_history", "foreclosures"):
                    if not self._table_exists(conn, table):
                        continue
                    if self._table_has_columns(
                        conn,
                        table,
                        {"buyer_type", "sold_to"},
                    ):
                        sources.append(table)
                if not sources:
                    logger.warning(
                        "TrustAccounts bidder context unavailable: no source tables present"
                    )
                    return set(), set()

                union_sql = "\nUNION\n".join(
                    [
                        f"""
                        SELECT DISTINCT sold_to
                        FROM {table}
                        WHERE buyer_type = 'Third Party'
                          AND sold_to IS NOT NULL
                          AND sold_to != ''
                        """
                        for table in sources
                    ]
                )
                rows = conn.execute(text(union_sql)).mappings().all()
        except SQLAlchemyError as exc:
            logger.opt(exception=True).warning(
                "TrustAccounts bidder context query failed (dsn={}): {}",
                self._dsn_tag(self.pg_dsn),
                exc,
            )
            raise

        exact: set[str] = set()
        core: set[str] = set()
        for row in rows:
            name = str(row["sold_to"])
            normalized = self._normalize_party_name(name)
            if not normalized:
                continue
            exact.add(normalized)
            core_name = self._core_party_name(normalized)
            if core_name:
                core.add(core_name)

        return exact, core

    def _load_upcoming_auction_context(self, conn: Connection) -> dict[str, dict[str, str]]:
        today_iso = datetime.now(tz=UTC).date().isoformat()
        try:
            if not self._table_exists(conn, "foreclosures"):
                logger.warning(
                    "TrustAccounts PG context unavailable: missing table 'foreclosures'"
                )
                return {}

            columns = self._table_columns(conn, "foreclosures")
            required = {"case_number_raw", "auction_date"}
            missing = sorted(required - columns)
            if missing:
                logger.warning(
                    "TrustAccounts PG context unavailable: foreclosures missing columns {}",
                    ",".join(missing),
                )
                return {}

            plaintiff_expr = (
                "judgment_data->>'plaintiff_name'" if "judgment_data" in columns else "''"
            )
            archived_clause = "AND archived_at IS NULL" if "archived_at" in columns else ""
            rows = conn.execute(
                text(
                    f"""
                    SELECT
                        case_number_raw AS case_number,
                        auction_date::text AS auction_date,
                        {plaintiff_expr} AS plaintiff
                    FROM foreclosures
                    WHERE case_number_raw IS NOT NULL
                      AND case_number_raw != ''
                      AND auction_date IS NOT NULL
                      AND auction_date >= CAST(:today_iso AS date)
                      {archived_clause}
                    """
                ),
                {"today_iso": today_iso},
            ).mappings().all()
        except SQLAlchemyError as exc:
            logger.opt(exception=True).warning(
                "TrustAccounts PG context query failed (dsn={}): {}",
                self._dsn_tag(self.pg_dsn),
                exc,
            )
            raise

        result: dict[str, dict[str, str]] = {}
        for row in rows:
            short_case = self._normalize_case_number(str(row["case_number"]))
            if not short_case:
                continue
            record = {
                "auction_date": str(row["auction_date"]),
                "plaintiff": str(row.get("plaintiff") or ""),
                "source_case_number": str(row["case_number"]),
            }
            current = result.get(short_case)
            if not current:
                result[short_case] = record
                continue
            # Keep the earliest upcoming auction date when duplicates exist.
            if record["auction_date"] < current["auction_date"]:
                result[short_case] = record

        if not result:
            logger.warning(
                "TrustAccounts PG context returned no upcoming auctions (today={}, source=foreclosures)",
                today_iso,
            )

        return result

    def _dsn_tag(self, dsn: str) -> str:
        if "@" not in dsn:
            return dsn
        return dsn.rsplit("@", 1)[-1]

    def _table_exists(self, conn: Connection, table_name: str) -> bool:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                LIMIT 1
                """
            ),
            {"table_name": table_name},
        ).first()
        return row is not None

    def _table_columns(self, conn: Connection, table_name: str) -> set[str]:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).mappings().all()
        return {str(r["column_name"]) for r in rows}

    def _table_has_columns(
        self,
        conn: Connection,
        table_name: str,
        required_columns: set[str],
    ) -> bool:
        columns = self._table_columns(conn, table_name)
        missing = sorted(required_columns - columns)
        if missing:
            logger.warning(
                "TrustAccounts context skip: table={} missing columns={}",
                table_name,
                ",".join(missing),
            )
            return False
        return True

    def _normalize_case_number(self, value: str | None) -> str | None:
        if not value:
            return None
        case_number = str(value).strip().upper()
        if CASE_NUMBER_RE.match(case_number):
            return case_number
        if (
            len(case_number) >= 14
            and case_number[:2].isdigit()
            and case_number[2:6].isdigit()
            and case_number[6:8].isalpha()
            and case_number[8:14].isdigit()
        ):
            return f"{case_number[2:4]}-{case_number[6:8]}-{case_number[8:14]}"
        return case_number

    def _normalize_party_name(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = re.sub(r"[^A-Z0-9 ]+", " ", str(value).upper())
        return re.sub(r"\s+", " ", normalized).strip()

    def _core_party_name(self, normalized_name: str) -> str:
        if not normalized_name:
            return ""
        tokens = normalized_name.split()
        while tokens and tokens[-1] in LEGAL_SUFFIXES:
            tokens.pop()
        return " ".join(tokens).strip()

    def _classify_counterparty(
        self,
        plaintiff_name: str | None,
        third_party_exact: set[str],
        third_party_core: set[str],
    ) -> str:
        normalized = self._normalize_party_name(plaintiff_name)
        if not normalized:
            return "unknown"

        if any(keyword in normalized for keyword in BANK_KEYWORDS):
            return "bank"

        core_name = self._core_party_name(normalized)
        if normalized in third_party_exact or (core_name and core_name in third_party_core):
            return "third_party_bidder"

        return "unknown"

    def _prior_business_day(self, iso_day: str) -> str:
        current = datetime.strptime(iso_day, "%Y-%m-%d").date() - timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current.isoformat()

    def _winning_bid_match_count(
        self,
        history_bids_by_date: dict[str, dict[float, int]],
        bid_date: str,
        amount: float | None,
    ) -> int:
        if amount is None:
            return 0
        rounded = round(amount, 2)
        return history_bids_by_date.get(bid_date, {}).get(rounded, 0)

    def _parse_iso_date(self, value: str) -> date | None:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None


def run_trust_accounts_update(force_reprocess: bool = False) -> dict[str, Any]:
    service = TrustAccountsService()
    return service.run(force_reprocess=force_reprocess)


if __name__ == "__main__":
    output = run_trust_accounts_update(force_reprocess=False)
    print(json.dumps(output, indent=2))
