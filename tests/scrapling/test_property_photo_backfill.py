"""
Integration smoke test for downloading property photos from PG CDN URLs.

This test:
1) reads properties from ``property_market`` that have remote photos but no local photos,
2) fetches a bounded sample of those CDN URLs,
3) writes photos to a persistent on-disk run folder and verifies results.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests
from loguru import logger
from requests import Response
from sqlalchemy import text

from src.utils.logging_config import configure_logger
from sunbiz.db import get_engine, resolve_pg_dsn


def _integration_enabled() -> bool:
    """Gate integration run to avoid accidental live network/DB execution."""
    return os.getenv("PG_PHOTO_BACKFILL_TEST", "").strip().lower() in {"1", "true", "yes", "on"}


def _to_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _resolve_output_root(run_id: str) -> Path:
    """Return a persistent root directory for this test run."""
    return Path("data") / "realtor_photo_backfill" / "foreclosure" / run_id


def _query_candidates(engine, limit: int) -> list[dict[str, object]]:
    """Return properties that still need local photo cache population."""
    query = """
        SELECT
            pm.strap,
            pm.case_number,
            pm.photo_cdn_urls
        FROM property_market pm
        WHERE pm.photo_cdn_urls IS NOT NULL
          AND jsonb_typeof(pm.photo_cdn_urls) = 'array'
          AND jsonb_array_length(pm.photo_cdn_urls) > 0
          AND (
              pm.photo_local_paths IS NULL
              OR jsonb_typeof(pm.photo_local_paths) != 'array'
              OR jsonb_array_length(pm.photo_local_paths) = 0
              OR (
                  jsonb_array_length(pm.photo_local_paths) < 15
                  AND jsonb_array_length(pm.photo_local_paths) < jsonb_array_length(pm.photo_cdn_urls)
              )
          )
        ORDER BY pm.updated_at DESC NULLS LAST
        LIMIT :limit
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query), {"limit": limit}).mappings().all()
    except Exception:
        logger.exception("PG photo backfill candidate query failed")
        raise

    logger.info("Found {} candidate properties for photo backfill.", len(rows))
    return [dict(row) for row in rows]


def _coerce_urls(raw_urls: object, *, case_number: str | None = None) -> list[str]:
    """Return a clean list of candidate image URLs from a JSONB row value."""
    if isinstance(raw_urls, str):
        try:
            raw_urls = json.loads(raw_urls)
        except json.JSONDecodeError as exc:
            logger.warning("photo_cdn_urls decode failed for case {}: {}", case_number or "<unknown>", exc)
            return []
    if not isinstance(raw_urls, list):
        if raw_urls is not None:
            logger.warning(
                "photo_cdn_urls expected JSON array for case {}, got {}.",
                case_number or "<unknown>",
                type(raw_urls).__name__,
            )
        return []
    return [url for url in raw_urls if isinstance(url, str) and url.strip()]


def _safe_case_folder(case_number: object) -> str:
    """Create filesystem-safe folder segment for case number."""
    raw = str(case_number).strip() if case_number else "unknown"
    return raw.replace("/", "_").replace("\\", "_").replace(" ", "_")


def _image_extension(response: Response) -> str:
    content_type = (response.headers.get("Content-Type", "") or "").split(";", maxsplit=1)[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }.get(content_type, ".jpg")


def _download_property_photos(
    session: requests.Session,
    photo_urls: list[str],
    out_dir: Path,
    *,
    case_number: str | None,
    max_photos: int,
) -> list[str]:
    """Download a bounded list of photos to ``out_dir`` and return saved file paths."""
    saved_paths: list[str] = []
    timeout_seconds = 15
    out_dir.mkdir(parents=True, exist_ok=True)

    for idx, photo_url in enumerate(photo_urls[:max_photos]):
        if not isinstance(photo_url, str) or not photo_url:
            logger.warning(
                "Encountered empty/invalid photo URL for case {} at index {}.",
                case_number or "<unknown>",
                idx,
            )
            continue

        try:
            response = session.get(photo_url, timeout=timeout_seconds)
            response.raise_for_status()
        except requests.RequestException:
            logger.warning(
                "Request failed for case {} URL {}",
                case_number or "<unknown>",
                photo_url,
                exc_info=True,
            )
            continue

        content_type = (response.headers.get("Content-Type", "") or "").split(";", maxsplit=1)[0].strip().lower()
        if not content_type.startswith("image/"):
            logger.warning(
                "Non-image response for case {} URL {}: status={}, content-type={}",
                case_number or "<unknown>",
                photo_url,
                response.status_code,
                content_type,
            )
            continue

        if not response.content:
            logger.warning("Empty image payload for case {} URL {}", case_number or "<unknown>", photo_url)
            continue

        url_hash = hashlib.sha1(photo_url.encode()).hexdigest()[:10]
        filename = f"{idx:03d}_{url_hash}{_image_extension(response)}"
        path = out_dir / filename

        if path.exists():
            logger.debug(
                "Photo file already exists for case {} at {}.", case_number or "<unknown>", path
            )
            if path.stat().st_size > 0:
                saved_paths.append(str(path))
            else:
                logger.warning("Existing photo file is empty for case {} at {}.", case_number or "<unknown>", path)
            continue

        try:
            path.write_bytes(response.content)
        except OSError as exc:
            logger.warning(
                "Failed writing photo for case {} to {}: {}",
                case_number or "<unknown>",
                path,
                exc,
            )
            continue

        try:
            file_size = path.stat().st_size
        except OSError as exc:
            logger.warning(
                "Unable to validate saved photo for case {} at {}: {}",
                case_number or "<unknown>",
                path,
                exc,
            )
            continue

        if file_size <= 0:
            logger.warning("Saved image is empty for case {} at {}.", case_number or "<unknown>", path)
            continue

        saved_paths.append(str(path))

    return saved_paths


def _configure_test_logger() -> str:
    """Configure Loguru for this integration test and return a unique run id."""
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    configure_logger(log_file=f"scrapling/realtor_photo_backfill_{run_id}.log", level="INFO")
    return run_id


def _print_summary(
    *,
    run_id: str,
    candidate_count: int,
    max_per_property: int,
    min_expected_saved: int,
    saved_paths: list[str],
    output_root: Path,
    log_file: str,
) -> None:
    """Emit a concise console summary for quick operator consumption."""
    header = [
        "\n=== Realtor Photo Backfill Summary ===",
        f"run_id={run_id}",
        f"candidates={candidate_count}",
        f"max_per_property={max_per_property}",
        f"min_expected={min_expected_saved}",
        f"saved_count={len(saved_paths)}",
        f"output_root={output_root}",
        f"log_file=logs/{log_file}",
    ]
    print("\n".join(header))

    if not saved_paths:
        print("saved_samples=none")
        return

    print("saved_samples=" + ", ".join(saved_paths[:10]))

    if len(saved_paths) > 10:
        print(f"... and {len(saved_paths)-10} more saved files")


@pytest.mark.integration
def test_pg_properties_missing_local_photos_download_to_disk() -> None:
    """Smoke test: can we write remote property photos to disk from PG candidates."""
    if not _integration_enabled():
        print("[realtor-photo] SKIPPED: set PG_PHOTO_BACKFILL_TEST=1 to run live PG + network test.")
        pytest.skip("Set PG_PHOTO_BACKFILL_TEST=1 to run live PG + network test.")

    run_id = _configure_test_logger()
    total_saved: list[str] = []

    with logger.contextualize(run_id=run_id):
        logger.info(
            "Starting property photo backfill integration test | run_id={}",
            run_id,
        )
        log_file = f"scrapling/realtor_photo_backfill_{run_id}.log"

        dsn = resolve_pg_dsn()
        logger.info("Resolved PostgreSQL DSN source: {}", "env" if os.getenv("SUNBIZ_PG_DSN") else "default")

        try:
            engine = get_engine(dsn)
            candidates = _query_candidates(
                engine,
                limit=_to_int(os.getenv("PG_PHOTO_TEST_LIMIT", "10"), default=10),
            )
        except Exception as exc:  # pragma: no cover - environment dependent
            print(f"[realtor-photo] SKIPPED: PostgreSQL unavailable: {exc}")
            logger.exception("PostgreSQL unavailable for integration test")
            pytest.skip(f"PostgreSQL unavailable for integration test: {exc}")

        if not candidates:
            print("[realtor-photo] SKIPPED: no PG rows matched photo backfill criteria.")
            logger.warning("No PG rows matched photo backfill selection criteria.")
            pytest.skip("No PG properties found with CDN photos and no local photos.")

        max_per_property = _to_int(os.getenv("PG_PHOTO_PER_PROPERTY", "10"), default=10)
        min_expected_saved = min(
            _to_int(os.getenv("PG_PHOTO_MIN_SAVED", "1"), default=1),
            len(candidates) * max_per_property,
        )
        logger.info(
            "Configured test: candidate_limit={}, per_property_limit={}, min_expected_saved={}",
            len(candidates),
            max_per_property,
            min_expected_saved,
        )

        output_root = _resolve_output_root(run_id) / "Foreclosure"
        logger.info("Persisting photos under: {}", output_root)
        with requests.Session() as session:
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                }
            )

            for candidate in candidates:
                case_number = candidate.get("case_number")
                urls = _coerce_urls(
                    candidate.get("photo_cdn_urls"),
                    case_number=str(case_number) if case_number else None,
                )
                if not urls:
                    logger.warning(
                        "No usable CDN URLs after coercion for case {}.",
                        case_number or "<unknown>",
                    )
                    continue

                case_dir = output_root / _safe_case_folder(case_number) / "photos"
                downloaded = _download_property_photos(
                    session=session,
                    photo_urls=urls,
                    out_dir=case_dir,
                    case_number=str(case_number) if case_number else None,
                    max_photos=max_per_property,
                )
                total_saved.extend(downloaded)

                if downloaded:
                    logger.info(
                        "Downloaded {} photos for case {} to {}.",
                        len(downloaded),
                        case_number or "<unknown>",
                        case_dir,
                    )
                else:
                    logger.warning("No photos downloaded for case {}.", case_number or "<unknown>")

        logger.info(
            "Photo backfill test complete | candidates={} | total_saved={}",
            len(candidates),
            len(total_saved),
        )
        _print_summary(
            run_id=run_id,
            candidate_count=len(candidates),
            max_per_property=max_per_property,
            min_expected_saved=min_expected_saved,
            saved_paths=total_saved,
            output_root=output_root,
            log_file=log_file,
        )

    assert len(total_saved) >= min_expected_saved, (
        f"Expected at least {min_expected_saved} images, wrote {len(total_saved)}."
    )
