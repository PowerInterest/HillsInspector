"""
Single-PIN permit collector for PostgreSQL-era HillsInspector workflows.

Architectural purpose:
- Provide an on-demand, property-scoped permit collector keyed by HCPA PIN.
- Use the same parcel endpoint that powers the public HCPA parcel page:
  `CommonServices/property/search//ParcelData?pin=...`.
- Enrich permit rows with additional detail fields when available from:
  1) permit URL targets (typically Accela GlobalSearch/CapDetail pages), and
  2) Hillsborough county ArcGIS permit layer (when permit number matches).

How it fits in the broader system:
- This is a diagnostic/forensic utility in `src/tools` for one-property deep dives.
- It complements bulk loaders (`CountyPermitService`, `TampaPermitService`) by
  prioritizing completeness for a single PIN instead of high-volume ingestion.
- Output is JSON-first so downstream scripts or analysts can diff this payload
  against `county_permits`, `tampa_accela_records`, and title-event outputs.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from loguru import logger
import requests

# Add project root to sys.path to allow running as a script.
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

HCPA_PARCEL_DATA_URL = "https://gis.hcpafl.org/CommonServices/property/search//ParcelData"
COUNTY_ARCGIS_QUERY_URL = (
    "https://services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/"
    "AccelaDashBoard_MapService20211019/FeatureServer/0/query"
)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _safe_pin(value: str) -> str:
    return "".join(ch for ch in value.strip().upper() if ch.isalnum())


def _money_to_float(value: Any) -> float | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    text_value = text_value.replace("$", "").replace(",", "")
    try:
        return float(text_value)
    except ValueError:
        return None


def _parse_mmddyyyy_to_iso(value: str | None) -> str | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    try:
        return datetime.strptime(text_value, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _extract_cap_detail_url(page_html: str, fallback_url: str | None = None) -> str | None:
    detail_pattern = re.compile(r"CapDetail\.aspx\?[^\"']+", re.IGNORECASE)
    match = detail_pattern.search(page_html)
    if match:
        href = match.group(0).replace("&amp;", "&")
        if href.lower().startswith("http"):
            return href
        return f"https://aca-prod.accela.com/TAMPA/Cap/{href}"
    if fallback_url and "CapDetail.aspx" in fallback_url:
        return fallback_url
    return None


def _extract_accela_detail_fields(page_html: str) -> dict[str, Any]:
    """
    Parse common fields from Accela GlobalSearch/CapDetail HTML bodies.

    The source pages vary between jurisdictions/modules, so this parser uses
    conservative regex extraction and may return null fields when unavailable.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    text_value = soup.get_text(" ", strip=True)

    status_match = re.search(
        r"Record\s+Status:\s*([A-Za-z][A-Za-z /&()\-]{0,100})",
        text_value,
    )
    expiration_match = re.search(r"Expiration\s+Date:\s*(\d{2}/\d{2}/\d{4})", text_value)
    job_value_match = re.search(
        r"Job\s+Value:\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,})",
        text_value,
    )
    valuation_match = re.search(
        r"(Valuation|Estimated\s+Value|Estimated\s+Cost):\s*\$?\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,})",
        text_value,
        re.IGNORECASE,
    )

    status = _clean_text(status_match.group(1) if status_match else None)
    expiration_date = _parse_mmddyyyy_to_iso(
        expiration_match.group(1) if expiration_match else None
    )
    job_value = _money_to_float(job_value_match.group(1) if job_value_match else None)
    alt_valuation = _money_to_float(valuation_match.group(2) if valuation_match else None)

    return {
        "status": status,
        "expiration_date": expiration_date,
        "job_value": job_value,
        "alt_valuation": alt_valuation,
    }


def _guess_source(permit_url: str | None) -> str:
    url = (permit_url or "").lower()
    if "accela.com/tampa" in url:
        return "tampa"
    if "accela.com/hcfl" in url:
        return "hcfl"
    if "accela.com" in url:
        return "accela_other"
    if not url:
        return "unknown"
    return "non_accela"


class PermitSinglePinFetcher:
    def __init__(
        self,
        *,
        timeout_seconds: int = 45,
        include_accela: bool = True,
        include_arcgis: bool = True,
        max_retries: int = 3,
        request_pause_seconds: float = 0.15,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.include_accela = include_accela
        self.include_arcgis = include_arcgis
        self.max_retries = max_retries
        self.request_pause_seconds = request_pause_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "HillsInspector/pg_permit_single_pin/1.0"})

    def _request_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                return payload
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(0.35 * attempt)
        if last_error is None:
            raise RuntimeError("request failed with unknown error")
        raise RuntimeError(f"request failed: {last_error}") from last_error

    def _request_text(self, url: str) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout_seconds)
                response.raise_for_status()
                return response.text
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(0.35 * attempt)
        if last_error is None:
            raise RuntimeError("request failed with unknown error")
        raise RuntimeError(f"request failed: {last_error}") from last_error

    def fetch_parcel_data(self, pin: str) -> dict[str, Any]:
        payload = self._request_json(HCPA_PARCEL_DATA_URL, params={"pin": pin})
        if payload.get("Message"):
            raise RuntimeError(f"HCPA parcel lookup failed: {payload.get('Message')}")
        if not payload.get("pin"):
            raise RuntimeError("HCPA parcel lookup returned no parcel payload")
        return payload

    def _query_arcgis_by_permit(self, permit_number: str) -> list[dict[str, Any]]:
        if not self.include_arcgis:
            return []
        safe = permit_number.replace("'", "''")
        where_exact = f"PERMIT__ = '{safe}'"
        params = {
            "where": where_exact,
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
        }
        payload = self._request_json(COUNTY_ARCGIS_QUERY_URL, params=params)
        features = payload.get("features") or []
        if features:
            return [feature.get("attributes") or {} for feature in features]

        # Fallback for inconsistent casing/trailing spaces in source fields.
        where_upper = f"UPPER(PERMIT__) = '{safe.upper()}'"
        params["where"] = where_upper
        payload = self._request_json(COUNTY_ARCGIS_QUERY_URL, params=params)
        features = payload.get("features") or []
        return [feature.get("attributes") or {} for feature in features]

    def _enrich_with_accela(self, permit_url: str | None) -> dict[str, Any]:
        if not self.include_accela:
            return {"enabled": False}
        url = _clean_text(permit_url)
        if not url:
            return {"enabled": True, "skipped": "missing_url"}
        if "accela.com" not in url.lower():
            return {"enabled": True, "skipped": "not_accela_url", "queried_url": url}

        result: dict[str, Any] = {
            "enabled": True,
            "queried_url": url,
            "detail_url": None,
            "search_extract": None,
            "detail_extract": None,
            "error": None,
        }
        try:
            search_html = self._request_text(url)
            search_extract = _extract_accela_detail_fields(search_html)
            detail_url = _extract_cap_detail_url(search_html, url)
            result["search_extract"] = search_extract
            result["detail_url"] = detail_url

            if detail_url and detail_url != url:
                time.sleep(self.request_pause_seconds)
                detail_html = self._request_text(detail_url)
                detail_extract = _extract_accela_detail_fields(detail_html)
                result["detail_extract"] = detail_extract
        except Exception as exc:
            result["error"] = str(exc)
        return result

    @staticmethod
    def _normalize_permit_row(row: dict[str, Any]) -> dict[str, Any]:
        permit_number = _clean_text(row.get("permitNum"))
        issue_date_raw = _clean_text(row.get("issueDate"))
        est_value_raw = _clean_text(row.get("estValue"))

        return {
            "permit_number": permit_number,
            "description": _clean_text(row.get("descr")),
            "issue_date_raw": issue_date_raw,
            "issue_date": _parse_mmddyyyy_to_iso(issue_date_raw),
            "estimated_value_raw": est_value_raw,
            "estimated_value": _money_to_float(est_value_raw),
            "permit_url": _clean_text(row.get("permitUrl")),
            "permit_type_code": _clean_text(row.get("permitType")),
            "property_type_code": _clean_text(row.get("propertyType")),
            "source_guess": _guess_source(_clean_text(row.get("permitUrl"))),
            "source_row_id": row.get("id"),
            "raw_row": row,
        }

    def fetch_pin_permits(
        self,
        pin: str,
        *,
        max_permits: int | None = None,
    ) -> dict[str, Any]:
        pin = _safe_pin(pin)
        if not pin:
            raise ValueError("pin must be non-empty")

        parcel = self.fetch_parcel_data(pin)
        permit_rows = parcel.get("permitInfo") or []
        if not isinstance(permit_rows, list):
            permit_rows = []
        if max_permits is not None and max_permits > 0:
            permit_rows = permit_rows[: max_permits]

        permits: list[dict[str, Any]] = []
        arcgis_match_count = 0
        accela_detail_count = 0
        accela_error_count = 0

        for row in permit_rows:
            normalized = self._normalize_permit_row(row)
            permit_number = normalized.get("permit_number")

            arcgis_matches: list[dict[str, Any]] = []
            arcgis_error: str | None = None
            if self.include_arcgis and permit_number:
                try:
                    arcgis_matches = self._query_arcgis_by_permit(permit_number)
                    if arcgis_matches:
                        arcgis_match_count += 1
                except Exception as exc:
                    arcgis_error = str(exc)

            accela_data = self._enrich_with_accela(normalized.get("permit_url"))
            if accela_data.get("error"):
                accela_error_count += 1
            if accela_data.get("detail_extract"):
                accela_detail_count += 1

            normalized["arcgis"] = {
                "match_count": len(arcgis_matches),
                "matches": arcgis_matches,
                "error": arcgis_error,
            }
            normalized["accela"] = accela_data
            permits.append(normalized)

            time.sleep(self.request_pause_seconds)

        property_card = parcel.get("propertyCard") if isinstance(parcel.get("propertyCard"), dict) else {}
        return {
            "pin": pin,
            "fetched_at_utc": datetime.now(tz=UTC).isoformat(),
            "parcel_context": {
                "folio": _clean_text(property_card.get("folio")),
                "owner_name": _clean_text(parcel.get("owner")),
                "site_address": _clean_text(parcel.get("siteAddress")),
                "mailing_address": (
                    parcel.get("mailingAddress")
                    if isinstance(parcel.get("mailingAddress"), dict)
                    else None
                ),
                "subdivision": parcel.get("subdivision"),
                "tax_dist": parcel.get("taxDist"),
            },
            "permit_count": len(permits),
            "permits": permits,
            "summary": {
                "permits_with_estimated_value": sum(
                    1 for p in permits if p.get("estimated_value") is not None
                ),
                "permits_with_arcgis_match": arcgis_match_count,
                "permits_with_accela_detail_extract": accela_detail_count,
                "accela_errors": accela_error_count,
            },
            "source": {
                "hcpa_parcel_data_url": HCPA_PARCEL_DATA_URL,
                "county_arcgis_query_url": COUNTY_ARCGIS_QUERY_URL if self.include_arcgis else None,
            },
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch and enrich permit data for a single HCPA property PIN"
    )
    parser.add_argument("pin", help="HCPA property PIN (strap), e.g. 1829134XZ000012000103A")
    parser.add_argument(
        "--max-permits",
        type=int,
        default=None,
        help="Optional cap for number of permits to enrich",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=45,
        help="HTTP timeout for each request",
    )
    parser.add_argument(
        "--no-accela",
        action="store_true",
        help="Skip Accela page enrichment",
    )
    parser.add_argument(
        "--no-arcgis",
        action="store_true",
        help="Skip county ArcGIS permit lookups",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional output file path for JSON payload",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON (no indentation)",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    fetcher = PermitSinglePinFetcher(
        timeout_seconds=max(5, args.timeout_seconds),
        include_accela=not args.no_accela,
        include_arcgis=not args.no_arcgis,
    )

    result = fetcher.fetch_pin_permits(args.pin, max_permits=args.max_permits)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, indent=None if args.compact else 2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(f"Wrote permit payload to {output_path}")

    print(json.dumps(result, indent=None if args.compact else 2, ensure_ascii=False))


if __name__ == "__main__":
    main()
