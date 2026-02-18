#!/usr/bin/env python3
"""
Download Sunbiz bulk data from Florida DOS SFTP into a local mirror.

Usage examples:
  uv run python sunbiz/sync.py list --mode all
  uv run python sunbiz/sync.py sync --mode quarterly
  uv run python sunbiz/sync.py sync --mode daily --modified-since 2026-01-01
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import posixpath
import re
import stat
import sys
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    import paramiko
except ModuleNotFoundError:
    print(
        "Missing dependency: paramiko. Install with `uv add paramiko`.",
        file=sys.stderr,
    )
    raise


DEFAULT_HOST = os.getenv("SUNBIZ_SFTP_HOST", "sftp.floridados.gov")
DEFAULT_PORT = int(os.getenv("SUNBIZ_SFTP_PORT", "22"))
DEFAULT_USER = os.getenv("SUNBIZ_SFTP_USER", "Public")
DEFAULT_PASSWORD = os.getenv("SUNBIZ_SFTP_PASSWORD", "PubAccess1845!")
DEFAULT_DAILY_DIR = os.getenv("SUNBIZ_SFTP_DAILY_DIR", "/public/doc")
DEFAULT_QUARTERLY_DIR = os.getenv("SUNBIZ_SFTP_QUARTERLY_DIR", "/public/doc/quarterly")
DEFAULT_DATA_DIR = Path(os.getenv("SUNBIZ_DATA_DIR", "data/sunbiz"))
DEFAULT_MANIFEST = Path(os.getenv("SUNBIZ_MANIFEST", "data/sunbiz/manifest.json"))


@dataclass
class RemoteFile:
    path: str
    size: int
    mtime: int


def _utc_ts_to_iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).isoformat()


def _parse_date(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc
    return parsed.replace(tzinfo=dt.UTC)


def _print(msg: str) -> None:
    print(msg, flush=True)


class SunbizMirror:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        data_dir: Path,
        manifest_path: Path,
        recursive: bool = True,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.manifest_path = manifest_path
        self.recursive = recursive

    def _connect(self) -> tuple[paramiko.Transport, paramiko.SFTPClient]:
        transport = paramiko.Transport((self.host, self.port))
        transport.connect(username=self.username, password=self.password)
        return transport, paramiko.SFTPClient.from_transport(transport)

    @staticmethod
    def _norm_remote(remote_path: str) -> str:
        path = posixpath.normpath(remote_path.strip())
        if not path.startswith("/"):
            path = f"/{path}"
        return path

    def _listdir_attr_safe(
        self, sftp: paramiko.SFTPClient, remote_dir: str
    ) -> list[paramiko.SFTPAttributes] | None:
        try:
            return list(sftp.listdir_attr(remote_dir))
        except Exception:
            return None

    def _first_existing_dir(
        self,
        sftp: paramiko.SFTPClient,
        candidates: list[str],
    ) -> str | None:
        for candidate in candidates:
            normalized = self._norm_remote(candidate)
            if self._listdir_attr_safe(sftp, normalized) is not None:
                return normalized
        return None

    def _discover_named_directory(
        self,
        sftp: paramiko.SFTPClient,
        target_name: str,
        max_depth: int = 2,
    ) -> str | None:
        target = target_name.lower()
        queue: list[tuple[str, int]] = [("/", 0)]
        seen: set[str] = set()

        while queue:
            current, depth = queue.pop(0)
            if current in seen or depth > max_depth:
                continue
            seen.add(current)

            entries = self._listdir_attr_safe(sftp, current)
            if entries is None:
                continue

            for entry in entries:
                if not stat.S_ISDIR(entry.st_mode):
                    continue
                child = posixpath.join(current, entry.filename)
                if entry.filename.lower() == target:
                    return self._norm_remote(child)
                queue.append((child, depth + 1))
        return None

    def resolve_mode_dirs(
        self,
        sftp: paramiko.SFTPClient,
        mode: str,
        explicit_dirs: list[str] | None = None,
    ) -> list[str]:
        if explicit_dirs:
            return [self._norm_remote(d) for d in explicit_dirs]

        resolved: list[str] = []
        if mode in {"daily", "all"}:
            daily = self._first_existing_dir(
                sftp,
                [
                    DEFAULT_DAILY_DIR,
                    "/Public/doc",
                    "/public/doc",
                    "/doc",
                ],
            )
            if daily is None:
                daily = self._discover_named_directory(sftp, "doc")
            if daily is None:
                raise RuntimeError(
                    "Could not locate daily directory on SFTP."
                    " Set SUNBIZ_SFTP_DAILY_DIR or use --remote-dir."
                )
            resolved.append(daily)

        if mode in {"quarterly", "all"}:
            quarterly = self._first_existing_dir(
                sftp,
                [
                    DEFAULT_QUARTERLY_DIR,
                    "/Public/doc/quarterly",
                    "/public/doc/quarterly",
                    "/doc/quarterly",
                    "/quarterly",
                ],
            )
            if quarterly is None:
                quarterly = self._discover_named_directory(sftp, "quarterly")
            if quarterly is None:
                raise RuntimeError(
                    "Could not locate quarterly directory on SFTP."
                    " Set SUNBIZ_SFTP_QUARTERLY_DIR or use --remote-dir."
                )
            resolved.append(quarterly)

        if not resolved:
            raise RuntimeError("No remote directories resolved.")
        return resolved

    def list_remote_files(
        self,
        sftp: paramiko.SFTPClient,
        remote_dirs: Iterable[str],
        limit: int | None = None,
    ) -> list[RemoteFile]:
        files: list[RemoteFile] = []
        queue = [self._norm_remote(d) for d in remote_dirs]
        seen_dirs: set[str] = set()

        while queue:
            current = queue.pop(0)
            if current in seen_dirs:
                continue
            seen_dirs.add(current)

            entries = self._listdir_attr_safe(sftp, current)
            if entries is None:
                continue

            for entry in entries:
                child = posixpath.join(current, entry.filename)
                if stat.S_ISDIR(entry.st_mode):
                    if self.recursive:
                        queue.append(self._norm_remote(child))
                    continue
                files.append(
                    RemoteFile(
                        path=self._norm_remote(child),
                        size=int(entry.st_size),
                        mtime=int(entry.st_mtime),
                    )
                )
                if limit is not None and len(files) >= limit:
                    return files
        return files

    def load_manifest(self) -> dict[str, dict]:
        if not self.manifest_path.exists():
            return {}
        with self.manifest_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data

    def save_manifest(self, manifest: dict[str, dict]) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        tmp.replace(self.manifest_path)

    @staticmethod
    def _matches_patterns(
        remote_path: str,
        include: str | None,
        exclude: str | None,
    ) -> bool:
        if include and include not in remote_path and not re.search(include, remote_path):
            return False
        return not (exclude and re.search(exclude, remote_path))

    def sync(
        self,
        mode: str,
        remote_dirs: list[str] | None,
        include: str | None,
        exclude: str | None,
        modified_since: dt.datetime | None,
        max_files: int | None,
        dry_run: bool,
        force: bool,
    ) -> None:
        transport, sftp = self._connect()
        manifest = self.load_manifest()
        try:
            dirs = self.resolve_mode_dirs(sftp, mode=mode, explicit_dirs=remote_dirs)
            _print(f"Resolved remote dirs: {dirs}")
            pre_limit = (
                max_files
                if max_files is not None
                and include is None
                and exclude is None
                and modified_since is None
                else None
            )
            files = self.list_remote_files(sftp, dirs, limit=pre_limit)
            files.sort(key=lambda x: (x.mtime, x.path), reverse=True)

            if modified_since:
                threshold = int(modified_since.timestamp())
                files = [f for f in files if f.mtime >= threshold]
            files = [
                f
                for f in files
                if self._matches_patterns(f.path, include=include, exclude=exclude)
            ]
            if max_files is not None:
                files = files[:max_files]

            _print(f"Candidate files: {len(files)}")
            downloaded = 0
            skipped = 0

            for item in files:
                rel = item.path.lstrip("/")
                local_path = self.data_dir / rel
                local_path.parent.mkdir(parents=True, exist_ok=True)

                prior = manifest.get(item.path)
                unchanged = (
                    prior
                    and int(prior.get("size", -1)) == item.size
                    and int(prior.get("mtime", -1)) == item.mtime
                    and Path(prior.get("local_path", "")).exists()
                )
                if unchanged and not force:
                    skipped += 1
                    continue

                if dry_run:
                    _print(f"[DRY RUN] download {item.path} -> {local_path}")
                    downloaded += 1
                    continue

                tmp_path = local_path.with_suffix(local_path.suffix + ".part")
                _print(f"Downloading {item.path} ({item.size:,} bytes)")
                sftp.get(item.path, str(tmp_path))
                tmp_path.replace(local_path)
                downloaded += 1

                manifest[item.path] = {
                    **asdict(item),
                    "mtime_iso": _utc_ts_to_iso(item.mtime),
                    "local_path": str(local_path),
                    "downloaded_at_utc": dt.datetime.now(tz=dt.UTC).isoformat(),
                }

            if not dry_run:
                self.save_manifest(manifest)

            _print(
                f"Sync finished: downloaded={downloaded}, skipped={skipped}, "
                f"manifest={self.manifest_path}"
            )
        finally:
            sftp.close()
            transport.close()

    def list(
        self,
        mode: str,
        remote_dirs: list[str] | None,
        include: str | None,
        exclude: str | None,
        modified_since: dt.datetime | None,
        max_files: int | None,
    ) -> None:
        transport, sftp = self._connect()
        try:
            dirs = self.resolve_mode_dirs(sftp, mode=mode, explicit_dirs=remote_dirs)
            _print(f"Resolved remote dirs: {dirs}")
            pre_limit = (
                max_files
                if max_files is not None
                and include is None
                and exclude is None
                and modified_since is None
                else None
            )
            files = self.list_remote_files(sftp, dirs, limit=pre_limit)
            files.sort(key=lambda x: (x.mtime, x.path), reverse=True)

            if modified_since:
                threshold = int(modified_since.timestamp())
                files = [f for f in files if f.mtime >= threshold]
            files = [
                f
                for f in files
                if self._matches_patterns(f.path, include=include, exclude=exclude)
            ]
            if max_files is not None:
                files = files[:max_files]

            _print(f"Remote file count: {len(files)}")
            for item in files:
                _print(f"{_utc_ts_to_iso(item.mtime)}  {item.size:>12}  {item.path}")
        finally:
            sftp.close()
            transport.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mirror Sunbiz bulk SFTP data (daily + quarterly)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=DEFAULT_HOST)
        p.add_argument("--port", type=int, default=DEFAULT_PORT)
        p.add_argument("--username", default=DEFAULT_USER)
        p.add_argument(
            "--password",
            default=DEFAULT_PASSWORD,
            help="SFTP password (default from SUNBIZ_SFTP_PASSWORD or public default).",
        )
        p.add_argument(
            "--mode",
            choices=("daily", "quarterly", "all"),
            default="all",
            help="Which Sunbiz dataset branch to use.",
        )
        p.add_argument(
            "--remote-dir",
            action="append",
            default=None,
            help="Override remote directory (repeat for multiple).",
        )
        p.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
        p.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
        p.add_argument(
            "--pattern",
            default=None,
            help="Include regex pattern for remote path filtering.",
        )
        p.add_argument(
            "--exclude-pattern",
            default=None,
            help="Exclude regex pattern for remote path filtering.",
        )
        p.add_argument(
            "--modified-since",
            type=_parse_date,
            default=None,
            help="Only include files modified on/after YYYY-MM-DD (UTC).",
        )
        p.add_argument(
            "--max-files",
            type=int,
            default=None,
            help="Limit number of files after filtering.",
        )
        p.add_argument(
            "--no-recursive",
            action="store_true",
            help="Do not recurse into nested directories.",
        )

    list_cmd = sub.add_parser("list", help="List remote Sunbiz files.")
    add_common_args(list_cmd)

    sync_cmd = sub.add_parser("sync", help="Download remote files into local mirror.")
    add_common_args(sync_cmd)
    sync_cmd.add_argument("--dry-run", action="store_true")
    sync_cmd.add_argument("--force", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    mirror = SunbizMirror(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        data_dir=args.data_dir,
        manifest_path=args.manifest,
        recursive=not args.no_recursive,
    )

    if args.command == "list":
        mirror.list(
            mode=args.mode,
            remote_dirs=args.remote_dir,
            include=args.pattern,
            exclude=args.exclude_pattern,
            modified_since=args.modified_since,
            max_files=args.max_files,
        )
        return 0

    if args.command == "sync":
        mirror.sync(
            mode=args.mode,
            remote_dirs=args.remote_dir,
            include=args.pattern,
            exclude=args.exclude_pattern,
            modified_since=args.modified_since,
            max_files=args.max_files,
            dry_run=args.dry_run,
            force=args.force,
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
