from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import sys
import tarfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, cast

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
EXPORT_SCHEMA_VERSION = 1
INVENTORY_SCHEMA_VERSION = 1
MAX_INPUT_FILE_BYTES = 4 * 1024 * 1024
MAX_LEDGER_BYTES = 8 * 1024 * 1024
MAX_EXPORT_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 40 * 1024 * 1024
MAX_MANIFEST_CHAIN = 64
ARCHIVE_NAME = "receipt-auth-forensic.tar"
INVENTORY_NAME = "forensic-inventory.json"
SCOPE_NAME = "forensic-scope.json"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_FORENSIC_SLUG = re.compile(
    r"^receipt-auth-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$"
)
_SHARD_LEAF = re.compile(r"^shard-([0-9]{4})-([0-9]{8}T[0-9]{6}Z)$")


class ForensicError(RuntimeError):
    """Fail-closed forensic export or inspection error."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_object(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ForensicError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ForensicError(f"{label} root is not an object")
    return value


def _real_directory(path: Path, *, label: str) -> Path:
    absolute = path.absolute()
    try:
        metadata = absolute.lstat()
        resolved = absolute.resolve(strict=True)
    except OSError as error:
        raise ForensicError(f"{label} is unavailable: {absolute}") from error
    if not stat.S_ISDIR(metadata.st_mode) or absolute.is_symlink() or resolved != absolute:
        raise ForensicError(f"{label} is not an exact real directory: {absolute}")
    return absolute


def _secure_read(path: Path, *, maximum_bytes: int) -> tuple[bytes, os.stat_result]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ForensicError(f"required file is unreadable without symlink following: {path}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
            raise ForensicError(f"required file is special or exceeds its bound: {path}")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ForensicError(f"short read: {path}")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ForensicError(f"file grew during read: {path}")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise ForensicError(f"file identity changed during read: {path}")
    raw = b"".join(chunks)
    if len(raw) != before.st_size:
        raise ForensicError(f"file length changed during read: {path}")
    return raw, before


def _safe_relative(value: str) -> PurePosixPath:
    relative = PurePosixPath(value)
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ForensicError(f"unsafe forensic relative path: {value}")
    return relative


def _write_new(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if path.exists():
            path.unlink()
        raise


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _Exporter:
    def __init__(
        self,
        *,
        campaign_root: Path,
        incoming_root: Path,
        source_root: Path,
        output_root: Path,
        expected_source_commit: str,
    ) -> None:
        self.campaign_root = _real_directory(campaign_root, label="failed campaign root")
        self.incoming_root = _real_directory(incoming_root, label="failed incoming root")
        self.source_root = _real_directory(source_root, label="failed source root")
        self.output_root = output_root.absolute()
        self.expected_source_commit = expected_source_commit
        self.entries: list[dict[str, object]] = []
        self.captured: dict[str, bytes] = {}
        self.absent: list[str] = []
        self.shards: dict[str, str] = {}
        self.raw_segment_metadata: list[dict[str, object]] = []

    def _validate_roots(self) -> None:
        run_slug = self.campaign_root.name
        if (
            _RUN_SLUG.fullmatch(run_slug) is None
            or self.incoming_root.name != run_slug
            or self.source_root.name != run_slug
            or self.campaign_root.parent.name != "campaigns"
            or self.source_root.parent.name != "sources"
            or self.campaign_root.parent.parent != self.source_root.parent.parent
            or self.incoming_root.parent.name != "incoming"
            or self.incoming_root.parent.parent.name != "hyperlab-prediction-markets"
            or _COMMIT.fullmatch(self.expected_source_commit) is None
        ):
            raise ForensicError("failed campaign/source/incoming identities are inconsistent")
        expected_parent = self.incoming_root / "forensics"
        if expected_parent.exists():
            _real_directory(expected_parent, label="forensics parent")
        else:
            os.mkdir(expected_parent, 0o700)
            _fsync_directory(self.incoming_root)
        if (
            self.output_root.parent != expected_parent
            or _FORENSIC_SLUG.fullmatch(self.output_root.name) is None
            or self.output_root.exists()
        ):
            raise ForensicError("forensic output root must be a new canonical child of incoming/forensics")
        os.mkdir(self.output_root, 0o700)
        _fsync_directory(expected_parent)

    def _capture(self, source: Path, relative: str, *, maximum_bytes: int) -> bytes:
        rel = _safe_relative(relative)
        raw, metadata = _secure_read(source.absolute(), maximum_bytes=maximum_bytes)
        destination = self.output_root.joinpath(*rel.parts)
        _write_new(destination, raw)
        copied, _copied_metadata = _secure_read(destination, maximum_bytes=maximum_bytes)
        if copied != raw:
            raise ForensicError(f"forensic copy diverged: {relative}")
        entry = {
            "origin": "FAILED_CAMPAIGN_READ_ONLY_COPY",
            "path": rel.as_posix(),
            "sha256": sha256_bytes(raw),
            "size": len(raw),
            "source": str(source.absolute()),
            "source_stat": {
                "ctime_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "identity_stable_before_after": True,
                "inode": metadata.st_ino,
                "mtime_ns": metadata.st_mtime_ns,
                "size": metadata.st_size,
            },
        }
        self.entries.append(entry)
        self.captured[rel.as_posix()] = raw
        return raw

    def _capture_optional(self, source: Path, relative: str, *, maximum_bytes: int) -> None:
        try:
            metadata = source.lstat()
        except FileNotFoundError:
            self.absent.append(relative)
            return
        if not stat.S_ISREG(metadata.st_mode) or source.is_symlink():
            raise ForensicError(f"optional forensic input is special: {source}")
        self._capture(source, relative, maximum_bytes=maximum_bytes)

    def _verify_pinned_json(self, json_relative: str, pin_relative: str) -> dict[str, Any]:
        raw = self.captured[json_relative]
        pin = self.captured[pin_relative]
        try:
            fields = pin.decode("ascii").strip().split()
        except UnicodeDecodeError as error:
            raise ForensicError(f"pin is not ASCII: {pin_relative}") from error
        if (
            len(fields) != 2
            or fields[1] != PurePosixPath(json_relative).name
            or fields[0] != sha256_bytes(raw)
        ):
            raise ForensicError(f"physical pin diverged: {json_relative}")
        return _strict_object(raw, label=json_relative)

    def _capture_roots(self) -> tuple[dict[str, Any], dict[str, Any]]:
        self._capture(
            self.campaign_root / "campaign-manifest.json",
            "payload/campaign/campaign-manifest.json",
            maximum_bytes=MAX_INPUT_FILE_BYTES,
        )
        self._capture(
            self.campaign_root / "campaign-manifest.sha256",
            "payload/campaign/campaign-manifest.sha256",
            maximum_bytes=256,
        )
        campaign = self._verify_pinned_json(
            "payload/campaign/campaign-manifest.json",
            "payload/campaign/campaign-manifest.sha256",
        )
        for name, maximum in (
            ("handoff.json", MAX_INPUT_FILE_BYTES),
            ("handoff.sha256", 256),
            ("source-inventory.json", MAX_INPUT_FILE_BYTES),
        ):
            self._capture(
                self.incoming_root / name,
                f"payload/incoming/{name}",
                maximum_bytes=maximum,
            )
        handoff = self._verify_pinned_json(
            "payload/incoming/handoff.json",
            "payload/incoming/handoff.sha256",
        )
        if (
            handoff.get("source_commit") != self.expected_source_commit
            or handoff.get("campaign_root") != str(self.campaign_root)
            or handoff.get("incoming_root") != str(self.incoming_root)
            or handoff.get("source_root") != str(self.source_root)
            or handoff.get("boundary") != BOUNDARY
        ):
            raise ForensicError("handoff does not bind the failed roots and source commit")
        source_inventory = _strict_object(
            self.captured["payload/incoming/source-inventory.json"],
            label="source inventory",
        )
        source_inventory_body = {
            key: value
            for key, value in source_inventory.items()
            if key != "inventory_sha256"
        }
        if (
            source_inventory.get("commit") != self.expected_source_commit
            or source_inventory.get("inventory_sha256")
            != sha256_bytes(canonical_json_bytes(source_inventory_body))
        ):
            raise ForensicError("source inventory commit or logical hash diverged")
        source_rows = source_inventory.get("files")
        if not isinstance(source_rows, list):
            raise ForensicError("source inventory file list is absent")
        for name in (
            "prediction-markets-candidate-v1.json",
            "polymarket-public-contract-v1.json",
            "kalshi-public-contract-v1.json",
        ):
            raw = self._capture(
                self.source_root / "config" / "research" / name,
                f"payload/source/config/research/{name}",
                maximum_bytes=MAX_INPUT_FILE_BYTES,
            )
            relative = f"config/research/{name}"
            matches = [
                row
                for row in source_rows
                if isinstance(row, dict) and row.get("path") == relative
            ]
            git_blob_sha1 = hashlib.sha1(
                f"blob {len(raw)}\0".encode("ascii") + raw,
                usedforsecurity=False,
            ).hexdigest()
            if (
                len(matches) != 1
                or matches[0].get("size") != len(raw)
                or matches[0].get("blob_sha1") != git_blob_sha1
            ):
                raise ForensicError(f"source inventory does not authenticate {relative}")
        return campaign, handoff

    def _one_shard(self, venue: str) -> Path:
        runs = _real_directory(
            self.campaign_root / venue / "runs",
            label=f"{venue} failed runs root",
        )
        children = list(runs.iterdir())
        if len(children) != 1:
            raise ForensicError(f"{venue} failed runs root must contain exactly one shard")
        shard = children[0]
        if _SHARD_LEAF.fullmatch(shard.name) is None:
            raise ForensicError(f"{venue} shard name is not canonical")
        return _real_directory(shard, label=f"{venue} failed shard")

    def _manifest_chain(self, venue: str, shard: Path, result: Mapping[str, object]) -> None:
        current = result.get("manifest_sha256")
        if not isinstance(current, str) or _SHA256.fullmatch(current) is None:
            raise ForensicError(f"{venue} result lacks a pinned raw manifest hash")
        manifests_root = _real_directory(
            shard / "raw" / "manifests",
            label=f"{venue} raw manifests root",
        )
        segments_root = _real_directory(
            shard / "raw" / "segments",
            label=f"{venue} raw segments root",
        )
        seen: set[str] = set()
        head: dict[str, Any] | None = None
        while current is not None:
            if current in seen or len(seen) >= MAX_MANIFEST_CHAIN:
                raise ForensicError(f"{venue} raw manifest chain loops or exceeds its bound")
            seen.add(current)
            relative = f"payload/venues/{venue}/raw/manifests/{current}.manifest.json"
            raw = self._capture(
                manifests_root / f"{current}.manifest.json",
                relative,
                maximum_bytes=MAX_INPUT_FILE_BYTES,
            )
            if sha256_bytes(raw) != current:
                raise ForensicError(f"{venue} raw manifest filename hash diverged")
            manifest = _strict_object(raw, label=f"{venue} raw manifest")
            if head is None:
                head = manifest
            previous = manifest.get("previous_manifest_sha256")
            if previous is not None and (
                not isinstance(previous, str) or _SHA256.fullmatch(previous) is None
            ):
                raise ForensicError(f"{venue} raw manifest predecessor is invalid")
            current = previous
        assert head is not None
        descriptors = head.get("segments")
        if not isinstance(descriptors, list):
            raise ForensicError(f"{venue} raw head manifest has no segment descriptors")
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                raise ForensicError(f"{venue} raw segment descriptor is not an object")
            identity = descriptor.get("physical_sha256")
            stored_bytes = descriptor.get("stored_bytes")
            if (
                not isinstance(identity, str)
                or _SHA256.fullmatch(identity) is None
                or type(stored_bytes) is not int
                or stored_bytes <= 0
            ):
                raise ForensicError(f"{venue} raw segment descriptor identity is invalid")
            segment = segments_root / f"{identity}.rdpseg"
            before = segment.lstat()
            if segment.is_symlink() or not stat.S_ISREG(before.st_mode):
                raise ForensicError(f"{venue} raw segment is missing or special")
            after = segment.lstat()
            identity_before = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            identity_after = (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            if identity_before != identity_after:
                raise ForensicError(f"{venue} raw segment metadata changed during inspection")
            if before.st_size != stored_bytes:
                raise ForensicError(f"{venue} raw segment size diverges from its manifest")
            self.raw_segment_metadata.append(
                {
                    "content_exported": False,
                    "declared_physical_sha256": identity,
                    "declared_stored_bytes": stored_bytes,
                    "path": str(segment),
                    "reason": "RAW_SEGMENT_EXCLUDED_FAILURE_PRECEDES_REPLAY",
                    "source_stat": {
                        "ctime_ns": before.st_ctime_ns,
                        "device": before.st_dev,
                        "identity_stable_before_after": True,
                        "inode": before.st_ino,
                        "mtime_ns": before.st_mtime_ns,
                        "size": before.st_size,
                    },
                    "venue": venue,
                }
            )

    def _capture_venue(self, venue: str) -> None:
        venue_root = _real_directory(
            self.campaign_root / venue,
            label=f"{venue} failed venue root",
        )
        self._capture(
            venue_root / "state.json",
            f"payload/venues/{venue}/state.json",
            maximum_bytes=MAX_INPUT_FILE_BYTES,
        )
        self._capture_optional(
            venue_root / "ledger.jsonl",
            f"payload/venues/{venue}/ledger.jsonl",
            maximum_bytes=MAX_LEDGER_BYTES,
        )
        shard = self._one_shard(venue)
        self.shards[venue] = shard.name
        reports = _real_directory(shard / "reports", label=f"{venue} reports root")
        for name in ("probe-config.json", "result.json", "health.json"):
            self._capture(
                reports / name,
                f"payload/venues/{venue}/reports/{name}",
                maximum_bytes=MAX_INPUT_FILE_BYTES,
            )
        result = _strict_object(
            self.captured[f"payload/venues/{venue}/reports/result.json"],
            label=f"{venue} result",
        )
        self._manifest_chain(venue, shard, result)

    def _generated_entry(self, name: str, raw: bytes) -> None:
        _write_new(self.output_root / name, raw)
        self.entries.append(
            {
                "origin": "FORENSIC_DERIVED_METADATA",
                "path": name,
                "sha256": sha256_bytes(raw),
                "size": len(raw),
            }
        )
        self.captured[name] = raw

    def _build_archive(self, paths: Sequence[str]) -> str:
        archive_path = self.output_root / ARCHIVE_NAME
        directories = sorted(
            {
                parent.as_posix()
                for name in paths
                for parent in PurePosixPath(name).parents
                if parent.as_posix() not in {".", ""}
            }
        )
        with archive_path.open("xb") as raw_handle:
            with tarfile.open(
                fileobj=raw_handle,
                mode="w",
                format=tarfile.USTAR_FORMAT,
            ) as archive:
                for directory in directories:
                    info = tarfile.TarInfo(directory + "/")
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o700
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    archive.addfile(info)
                for name in sorted(paths):
                    raw = (self.output_root / Path(*PurePosixPath(name).parts)).read_bytes()
                    info = tarfile.TarInfo(name)
                    info.size = len(raw)
                    info.mode = 0o600
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    archive.addfile(info, io.BytesIO(raw))
            raw_handle.flush()
            os.fsync(raw_handle.fileno())
        if archive_path.stat().st_size > MAX_ARCHIVE_BYTES:
            raise ForensicError("forensic archive exceeds its transfer bound")
        archive_hash = sha256_bytes(archive_path.read_bytes())
        _write_new(
            self.output_root / f"{ARCHIVE_NAME}.sha256",
            f"{archive_hash}  {ARCHIVE_NAME}\n".encode("ascii"),
        )
        return archive_hash

    def run(self) -> dict[str, object]:
        self._validate_roots()
        campaign, _handoff = self._capture_roots()
        for venue in ("polymarket", "kalshi"):
            self._capture_venue(venue)
        scope = {
            "absent_optional_files": sorted(self.absent),
            "boundary": BOUNDARY,
            "campaign_id": campaign.get("campaign_id"),
            "excluded": [
                "raw/segments/*.rdpseg:CONTENTS_NOT_READ_OR_EXPORTED",
                "systemd_journal:NOT_QUERIED",
                "H1:OUT_OF_SCOPE",
                "secrets_or_private_data:NOT_REQUESTED",
            ],
            "failed_campaign_root": str(self.campaign_root),
            "failed_incoming_root": str(self.incoming_root),
            "failed_source_root": str(self.source_root),
            "raw_segment_metadata": self.raw_segment_metadata,
            "read_only_source_policy": "O_RDONLY_O_NOFOLLOW_FSTAT_BEFORE_AFTER",
            "run_slug": self.campaign_root.name,
            "schema_version": EXPORT_SCHEMA_VERSION,
            "shards": self.shards,
            "source_commit": self.expected_source_commit,
        }
        scope_raw = canonical_json_bytes(scope) + b"\n"
        self._generated_entry(SCOPE_NAME, scope_raw)
        if sum(cast(int, item["size"]) for item in self.entries) > MAX_EXPORT_BYTES:
            raise ForensicError("forensic export exceeds its total byte bound")
        inventory_body = {
            "boundary": BOUNDARY,
            "failed_campaign_root": str(self.campaign_root),
            "files": sorted(self.entries, key=lambda item: cast(str, item["path"])),
            "run_slug": self.campaign_root.name,
            "schema_version": INVENTORY_SCHEMA_VERSION,
            "source_commit": self.expected_source_commit,
        }
        inventory = {
            **inventory_body,
            "inventory_sha256": sha256_bytes(canonical_json_bytes(inventory_body)),
        }
        inventory_raw = canonical_json_bytes(inventory) + b"\n"
        _write_new(self.output_root / INVENTORY_NAME, inventory_raw)
        inventory_file_hash = sha256_bytes(inventory_raw)
        _write_new(
            self.output_root / f"{INVENTORY_NAME}.sha256",
            f"{inventory_file_hash}  {INVENTORY_NAME}\n".encode("ascii"),
        )
        archive_paths = [
            cast(str, item["path"])
            for item in self.entries
        ] + [INVENTORY_NAME, f"{INVENTORY_NAME}.sha256"]
        archive_hash = self._build_archive(archive_paths)
        for directory in sorted(
            {path.parent for path in self.output_root.rglob("*") if path.is_file()},
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            _fsync_directory(directory)
        _fsync_directory(self.output_root)
        _fsync_directory(self.output_root.parent)
        return {
            "archive": str(self.output_root / ARCHIVE_NAME),
            "archive_sha256": archive_hash,
            "boundary": BOUNDARY,
            "export_root": str(self.output_root),
            "file_count": len(self.entries),
            "inventory_file_sha256": inventory_file_hash,
            "inventory_sha256": inventory["inventory_sha256"],
            "raw_segments_exported": 0,
            "source_commit": self.expected_source_commit,
            "status": "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_GREEN",
        }


def export_forensics(
    *,
    campaign_root: Path,
    incoming_root: Path,
    source_root: Path,
    output_root: Path,
    expected_source_commit: str,
) -> dict[str, object]:
    return _Exporter(
        campaign_root=campaign_root,
        incoming_root=incoming_root,
        source_root=source_root,
        output_root=output_root,
        expected_source_commit=expected_source_commit,
    ).run()


def _pin_fields(raw: bytes, *, expected_name: str) -> str:
    try:
        fields = raw.decode("ascii").strip().split()
    except UnicodeDecodeError as error:
        raise ForensicError(f"{expected_name} pin is not ASCII") from error
    if len(fields) != 2 or fields[1] != expected_name or _SHA256.fullmatch(fields[0]) is None:
        raise ForensicError(f"{expected_name} pin is malformed")
    return fields[0]


def _read_local_regular(path: Path, *, maximum_bytes: int) -> bytes:
    raw, _metadata = _secure_read(path.absolute(), maximum_bytes=maximum_bytes)
    return raw


def _archive_members(bundle_root: Path) -> tuple[dict[str, bytes], dict[str, Any]]:
    root = _real_directory(bundle_root, label="local forensic bundle root")
    archive_raw = _read_local_regular(root / ARCHIVE_NAME, maximum_bytes=MAX_ARCHIVE_BYTES)
    archive_pin = _read_local_regular(root / f"{ARCHIVE_NAME}.sha256", maximum_bytes=256)
    if sha256_bytes(archive_raw) != _pin_fields(archive_pin, expected_name=ARCHIVE_NAME):
        raise ForensicError("forensic archive SHA-256 diverged")
    external_inventory = _read_local_regular(
        root / INVENTORY_NAME,
        maximum_bytes=MAX_INPUT_FILE_BYTES,
    )
    external_inventory_pin = _read_local_regular(
        root / f"{INVENTORY_NAME}.sha256",
        maximum_bytes=256,
    )
    if sha256_bytes(external_inventory) != _pin_fields(
        external_inventory_pin,
        expected_name=INVENTORY_NAME,
    ):
        raise ForensicError("forensic inventory physical SHA-256 diverged")
    members: dict[str, bytes] = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode="r:") as archive:
        for member in archive.getmembers():
            relative = _safe_relative(member.name.rstrip("/"))
            if member.isdir():
                continue
            if not member.isreg() or member.size > MAX_INPUT_FILE_BYTES:
                raise ForensicError(f"archive member is special or oversized: {member.name}")
            name = relative.as_posix()
            if name in members:
                raise ForensicError(f"duplicate archive member: {name}")
            handle = archive.extractfile(member)
            if handle is None:
                raise ForensicError(f"archive member is unreadable: {name}")
            raw = handle.read(MAX_INPUT_FILE_BYTES + 1)
            if len(raw) != member.size or len(raw) > MAX_INPUT_FILE_BYTES:
                raise ForensicError(f"archive member short read or overflow: {name}")
            members[name] = raw
            total += len(raw)
            if total > MAX_EXPORT_BYTES:
                raise ForensicError("archive expanded bytes exceed the forensic bound")
    if members.get(INVENTORY_NAME) != external_inventory:
        raise ForensicError("archive and external inventory differ")
    if members.get(f"{INVENTORY_NAME}.sha256") != external_inventory_pin:
        raise ForensicError("archive and external inventory pin differ")
    inventory = _strict_object(external_inventory, label="forensic inventory")
    body = {key: value for key, value in inventory.items() if key != "inventory_sha256"}
    if (
        inventory.get("boundary") != BOUNDARY
        or inventory.get("schema_version") != INVENTORY_SCHEMA_VERSION
        or inventory.get("inventory_sha256") != sha256_bytes(canonical_json_bytes(body))
    ):
        raise ForensicError("forensic inventory logical identity diverged")
    files = inventory.get("files")
    if not isinstance(files, list):
        raise ForensicError("forensic inventory file list is absent")
    expected_members = {INVENTORY_NAME, f"{INVENTORY_NAME}.sha256"}
    for item in files:
        if not isinstance(item, dict):
            raise ForensicError("forensic inventory entry is not an object")
        path = item.get("path")
        digest = item.get("sha256")
        size = item.get("size")
        if (
            not isinstance(path, str)
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
            or type(size) is not int
            or size < 0
            or path not in members
            or len(members[path]) != size
            or sha256_bytes(members[path]) != digest
        ):
            raise ForensicError(f"forensic inventory entry diverged: {path}")
        expected_members.add(path)
    if set(members) != expected_members:
        raise ForensicError("forensic archive contains undeclared files")
    return members, inventory


def _brief(value: object) -> object:
    if isinstance(value, str):
        return value if len(value) <= 256 else value[:253] + "..."
    if isinstance(value, list):
        return {"length": len(value), "type": "array"}
    if isinstance(value, dict):
        return {"keys": sorted(value), "type": "object"}
    return value


@dataclass(frozen=True, slots=True)
class _BindingView:
    venue: str
    collection_id: str
    campaign_manifest_sha256: str
    candidate_config_sha256: str
    official_contract_sha256: str
    probe_binding_sha256: str
    payload: Mapping[str, object]


def _nonempty_text(value: object, *, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ForensicError(f"{label} must be nonempty text")
    return value


def _binding_view(raw: bytes) -> _BindingView:
    root = _strict_object(raw, label="prediction collection binding")
    expected = {
        "boundary", "campaign_manifest_sha256", "candidate_config_sha256",
        "census_limit", "collection_id", "collection_cutoff_utc_ns_exclusive",
        "duration_seconds", "feeds", "instruments", "max_bytes", "max_frames",
        "max_network_calls", "max_segment_bytes", "max_segments",
        "official_contract_sha256", "probe_binding_sha256",
        "progress_interval_seconds", "proxy_policy", "rotation_seconds",
        "schema_version", "venue",
    }
    if set(root) != expected:
        raise ForensicError("prediction collection binding fields differ from probe schema v1")
    if (
        root.get("boundary") != BOUNDARY
        or root.get("schema_version") != 1
        or root.get("proxy_policy") != "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED"
        or type(root.get("collection_cutoff_utc_ns_exclusive")) is not int
        or cast(int, root.get("collection_cutoff_utc_ns_exclusive")) <= 0
    ):
        raise ForensicError("prediction collection binding boundary, schema, or cutoff is invalid")
    claimed = _nonempty_text(root.get("probe_binding_sha256"), label="probe binding hash")
    payload = {key: value for key, value in root.items() if key != "probe_binding_sha256"}
    if sha256_bytes(canonical_json_bytes(payload)) != claimed:
        raise ForensicError("prediction collection binding self-hash diverged")
    hashes = [
        _nonempty_text(root.get(field), label=field)
        for field in (
            "campaign_manifest_sha256",
            "candidate_config_sha256",
            "official_contract_sha256",
        )
    ]
    if any(len(value) != 64 for value in (*hashes, claimed)):
        raise ForensicError("prediction collection binding requires SHA-256 identities")
    venue = _nonempty_text(root.get("venue"), label="binding venue")
    if venue not in {"polymarket", "kalshi"}:
        raise ForensicError("prediction collection binding venue is unsupported")
    return _BindingView(
        venue=venue,
        collection_id=_nonempty_text(root.get("collection_id"), label="binding collection id"),
        campaign_manifest_sha256=hashes[0],
        candidate_config_sha256=hashes[1],
        official_contract_sha256=hashes[2],
        probe_binding_sha256=claimed,
        payload=payload,
    )


def _verify_binding_plan(
    binding: _BindingView,
    *,
    venue: str,
    plan: Mapping[str, object],
) -> None:
    expected = {
        "census_limit": plan.get("census_limit"),
        "duration_seconds": plan.get("duration_seconds"),
        "feeds": plan.get("feeds"),
        "instruments": [],
        "max_bytes": plan.get("max_bytes"),
        "max_frames": plan.get("max_frames"),
        "max_network_calls": plan.get("max_network_calls"),
        "max_segment_bytes": plan.get("max_segment_bytes"),
        "max_segments": plan.get("max_segments"),
        "progress_interval_seconds": str(plan.get("progress_interval_seconds")),
        "rotation_seconds": str(plan.get("rotation_seconds")),
        "venue": venue,
    }
    if binding.venue != venue or any(
        binding.payload.get(key) != value for key, value in expected.items()
    ):
        raise ForensicError("prediction collection binding diverged from the frozen plan")


def _manifest_metadata(raw: bytes, *, expected_sha256: str) -> dict[str, object]:
    if sha256_bytes(raw) != expected_sha256:
        raise ForensicError("raw manifest content SHA-256 diverged")
    manifest = _strict_object(raw, label="raw manifest")
    expected_fields = {
        "collection_id", "frame_count", "generation", "manifest_type",
        "previous_manifest_sha256", "root_sha256", "schema_version",
        "segment_count", "segments", "stored_segment_bytes",
    }
    if (
        set(manifest) != expected_fields
        or raw != canonical_json_bytes(manifest)
        or manifest.get("schema_version") != 1
        or manifest.get("manifest_type") != "HYPERLAB_PUBLIC_RESEARCH_RAW_MANIFEST"
    ):
        raise ForensicError("raw manifest schema or canonical encoding diverged")
    segments = manifest.get("segments")
    if not isinstance(segments, list) or not segments:
        raise ForensicError("raw manifest segment descriptors are absent")
    physical_hashes: list[str] = []
    frame_count = 0
    stored_bytes = 0
    for descriptor in segments:
        if not isinstance(descriptor, dict):
            raise ForensicError("raw manifest segment descriptor is not an object")
        physical = descriptor.get("physical_sha256")
        frames = descriptor.get("frame_count")
        stored = descriptor.get("stored_bytes")
        if (
            not isinstance(physical, str)
            or _SHA256.fullmatch(physical) is None
            or type(frames) is not int
            or frames <= 0
            or type(stored) is not int
            or stored <= 0
        ):
            raise ForensicError("raw manifest segment descriptor metrics are invalid")
        physical_hashes.append(physical)
        frame_count += frames
        stored_bytes += stored
    root_hasher = hashlib.sha256(b"HYPERLAB_RESEARCH_DATA_ROOT_V1")
    for physical in physical_hashes:
        root_hasher.update(bytes.fromhex(physical))
    derived = {
        "frame_count": frame_count,
        "root_sha256": root_hasher.hexdigest(),
        "segment_count": len(segments),
        "stored_segment_bytes": stored_bytes,
    }
    if any(manifest.get(field) != value for field, value in derived.items()):
        raise ForensicError("raw manifest derived metrics diverged")
    return derived


def _result_checks(
    *,
    result: Mapping[str, object],
    binding: _BindingView,
) -> list[dict[str, object]]:
    checks: list[dict[str, object]] = []

    def add(field: str, ok: bool, observed: object, expected: object) -> None:
        checks.append(
            {
                "expected": _brief(expected),
                "field": field,
                "observed": _brief(observed),
                "ok": ok,
            }
        )

    frame_count = result.get("frames")
    byte_count = result.get("bytes")
    segment_count = result.get("segments")
    elapsed_ms = result.get("elapsed_ms")
    network_calls = result.get("network_calls")
    terminal = result.get("terminal_health")
    accepted = {
        "COMPLETE",
        "MAX_BYTES_REACHED",
        "MAX_DURATION_REACHED",
        "MAX_FRAMES_REACHED",
        "MAX_NETWORK_CALLS_REACHED",
        "MAX_SEGMENTS_REACHED",
        "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
    }
    add("frames.positive_int", type(frame_count) is int and frame_count > 0, frame_count, ">0 int")
    add("bytes.positive_int", type(byte_count) is int and byte_count > 0, byte_count, ">0 int")
    add("segments.positive_int", type(segment_count) is int and segment_count > 0, segment_count, ">0 int")
    add("elapsed_ms.nonnegative_int", type(elapsed_ms) is int and elapsed_ms >= 0, elapsed_ms, ">=0 int")
    add("network_calls.nonnegative_int", type(network_calls) is int and network_calls >= 0, network_calls, ">=0 int")
    for field in (
        "duration_seconds",
        "max_bytes",
        "max_frames",
        "max_network_calls",
        "max_segment_bytes",
        "max_segments",
    ):
        value = binding.payload.get(field)
        add(f"binding.{field}.positive_int", type(value) is int and value > 0, value, ">0 int")
    duration = binding.payload.get("duration_seconds")
    add("boundary", result.get("boundary") == BOUNDARY, result.get("boundary"), BOUNDARY)
    add("schema_version", result.get("schema_version") == 1, result.get("schema_version"), 1)
    add(
        "requested_duration_seconds",
        result.get("requested_duration_seconds") == duration,
        result.get("requested_duration_seconds"),
        duration,
    )
    add(
        "elapsed_ms.within_duration",
        type(elapsed_ms) is int and type(duration) is int and elapsed_ms <= duration * 1000,
        elapsed_ms,
        None if type(duration) is not int else f"<= {duration * 1000}",
    )
    for result_field, limit_field in (
        ("frames", "max_frames"),
        ("bytes", "max_bytes"),
        ("network_calls", "max_network_calls"),
        ("segments", "max_segments"),
    ):
        observed = result.get(result_field)
        limit = binding.payload.get(limit_field)
        add(
            f"{result_field}.within_{limit_field}",
            type(observed) is int and type(limit) is int and observed <= limit,
            observed,
            None if type(limit) is not int else f"<= {limit}",
        )
    for field in ("manifest_sha256", "root_sha256"):
        value = result.get(field)
        add(field, isinstance(value, str) and _SHA256.fullmatch(value) is not None, value, "lowercase SHA-256")
    add("terminal_health.accepted", terminal in accepted, terminal, sorted(accepted))
    error_required = terminal in {"MAX_BYTES_REACHED", "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED"}
    error = result.get("error")
    error_ok = (
        isinstance(error, str) and bool(error.strip())
        if error_required
        else error is None
    )
    add(
        "error.contract",
        error_ok,
        error,
        "nonempty string" if error_required else None,
    )
    for field, expected in (
        ("probe_binding_sha256", binding.probe_binding_sha256),
        ("campaign_manifest_sha256", binding.campaign_manifest_sha256),
        ("candidate_config_sha256", binding.candidate_config_sha256),
        ("official_contract_sha256", binding.official_contract_sha256),
        ("collection_id", binding.collection_id),
        ("venue", binding.venue),
    ):
        add(field, result.get(field) == expected, result.get(field), expected)
    for field in ("connection_attempts", "limitations"):
        add(f"{field}.array", isinstance(result.get(field), list), result.get(field), "array")
    for field in ("duplicates", "gaps", "queue_high_water", "reconnects"):
        value = result.get(field)
        add(f"{field}.nonnegative_int", type(value) is int and value >= 0, value, ">=0 int")
    for field in ("source_timestamp_max_ns", "source_timestamp_min_ns"):
        value = result.get(field)
        add(f"{field}.nullable_int", value is None or type(value) is int, value, "null or int")
    return checks


def diagnose_forensics(
    bundle_root: Path,
    *,
    expected_source_commit: str,
) -> dict[str, object]:
    members, inventory = _archive_members(bundle_root)
    if inventory.get("source_commit") != expected_source_commit:
        raise ForensicError("forensic bundle source commit diverged")
    candidate = _strict_object(
        members["payload/source/config/research/prediction-markets-candidate-v1.json"],
        label="candidate config",
    )
    candidate_sha256 = sha256_bytes(canonical_json_bytes(candidate))
    contracts = {
        venue: _strict_object(
            members[f"payload/source/config/research/{venue}-public-contract-v1.json"],
            label=f"{venue} official contract",
        )
        for venue in ("polymarket", "kalshi")
    }
    contract_hashes = {
        venue: sha256_bytes(canonical_json_bytes(contract))
        for venue, contract in contracts.items()
    }
    campaign = _strict_object(
        members["payload/campaign/campaign-manifest.json"],
        label="campaign manifest",
    )
    campaign_body = {
        key: value for key, value in campaign.items() if key != "manifest_sha256"
    }
    campaign_sha256 = sha256_bytes(canonical_json_bytes(campaign_body))
    campaign_contracts = campaign.get("contracts")
    candidate_plans = candidate.get("collection_plans")
    policy = candidate.get("prospective_shard_policy")
    if (
        campaign.get("manifest_sha256") != campaign_sha256
        or campaign.get("candidate_config_sha256") != candidate_sha256
        or not isinstance(campaign_contracts, dict)
        or any(campaign_contracts.get(venue) != contract_hashes[venue] for venue in contracts)
        or not isinstance(candidate_plans, dict)
        or not isinstance(policy, dict)
        or candidate.get("boundary") != BOUNDARY
        or campaign.get("boundary") != BOUNDARY
    ):
        raise ForensicError("campaign/candidate/contract frozen identities diverged")
    scope = _strict_object(members[SCOPE_NAME], label="forensic scope")
    shard_names = scope.get("shards")
    if not isinstance(shard_names, dict):
        raise ForensicError("forensic scope lacks shard identities")
    reports: dict[str, object] = {}
    for label, venue_value in (
        ("polymarket", "polymarket"),
        ("kalshi", "kalshi"),
    ):
        prefix = f"payload/venues/{label}/reports"
        probe_raw = members[f"{prefix}/probe-config.json"]
        result_raw = members[f"{prefix}/result.json"]
        result = _strict_object(result_raw, label=f"{label} terminal result")
        expected_fields = {
            "boundary", "bytes", "campaign_manifest_sha256", "candidate_config_sha256",
            "collection_id", "connection_attempts", "duplicates", "elapsed_ms", "error",
            "frames", "gaps", "limitations", "manifest_sha256", "network_calls",
            "official_contract_sha256", "probe_binding_sha256", "queue_high_water",
            "reconnects", "requested_duration_seconds", "root_sha256", "schema_version",
            "segments", "source_timestamp_max_ns", "source_timestamp_min_ns",
            "terminal_health", "venue",
        }
        runtime_checks: list[dict[str, object]] = []
        binding_error: str | None = None
        try:
            binding = _binding_view(probe_raw)
        except (ForensicError, OSError, ValueError) as error:
            binding_error = f"{type(error).__name__}:{error}"
            binding = None
        runtime_checks.append(
            {
                "expected": "authenticated prediction collection binding",
                "field": "probe_config.binding",
                "observed": binding_error,
                "ok": binding_error is None,
            }
        )
        runtime_checks.append(
            {
                "expected": sorted(expected_fields),
                "field": "result.fields",
                "observed": sorted(result),
                "ok": set(result) == expected_fields,
            }
        )
        runtime_checks.append(
            {
                "expected": "canonical JSON without trailing bytes",
                "field": "result.canonical_json",
                "observed": "exact" if result_raw == canonical_json_bytes(result) else "diverged",
                "ok": result_raw == canonical_json_bytes(result),
            }
        )
        if binding is not None:
            for field in ("manifest_sha256", "root_sha256", "terminal_health"):
                value = result.get(field)
                runtime_checks.append(
                    {
                        "expected": "nonempty text",
                        "field": f"result.{field}.text",
                        "observed": _brief(value),
                        "ok": type(value) is str and bool(value.strip()),
                    }
                )
            runtime_checks.extend(_result_checks(result=result, binding=binding))

        context_checks: list[dict[str, object]] = []
        raw_plan = candidate_plans.get(label)
        plan = raw_plan if isinstance(raw_plan, dict) else {}
        plan_error: str | None = None
        try:
            if binding is None:
                raise ForensicError("binding unavailable after runtime parsing stage")
            if not isinstance(raw_plan, dict):
                raise ForensicError(f"{label} frozen collection plan is absent")
            _verify_binding_plan(binding, venue=venue_value, plan=plan)
        except (ForensicError, OSError, ValueError) as error:
            plan_error = f"{type(error).__name__}:{error}"
        context_checks.append(
            {
                "expected": "binding exactly matching frozen venue plan",
                "field": "post_result_context.verify_collection_plan",
                "observed": plan_error,
                "ok": plan_error is None,
            }
        )
        context_checks.append(
            {
                "expected": "authenticated raw segment replay and required-feed coverage",
                "field": "from_probe_output.raw_payload_replay",
                "observed": "NOT_EVALUATED_RAW_CONTENT_EXCLUDED",
                "ok": None,
            }
        )
        if binding is not None and plan_error is None:
            shard_name = shard_names.get(label)
            match = _SHARD_LEAF.fullmatch(str(shard_name))
            if match is None:
                raise ForensicError(f"{label} shard identity is malformed")
            ordinal = int(match.group(1))
            start_raw = campaign.get("starts_at_utc")
            if not isinstance(start_raw, str):
                raise ForensicError("campaign start is absent")
            start = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(UTC)
            cadence = policy.get("cadence_seconds")
            if type(cadence) is not int or cadence <= 0:
                raise ForensicError("prospective shard cadence is invalid")
            scheduled = start + timedelta(seconds=ordinal * cadence)
            campaign_id = _nonempty_text(campaign.get("campaign_id"), label="campaign id")
            attempt_id = _nonempty_text(plan.get("attempt_id"), label=f"{label} attempt id")
            base_collection_id = f"{campaign_id}-{attempt_id}"
            identity = {
                "campaign_manifest_sha256": campaign_sha256,
                "ordinal": ordinal,
                "scheduled_start_utc": scheduled.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "venue": venue_value,
            }
            digest = sha256_bytes(canonical_json_bytes(identity))
            expected_collection_id = f"{base_collection_id}-shard-{ordinal:04d}-{digest[:16]}"
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            delta = scheduled - epoch
            expected_cutoff = (
                delta.days * 86_400_000_000_000
                + delta.seconds * 1_000_000_000
                + delta.microseconds * 1_000
                + cadence * 1_000_000_000
            )
            for field, observed, expected in (
                ("runner.venue", binding.venue, venue_value),
                ("runner.campaign_manifest_sha256", binding.campaign_manifest_sha256, campaign_sha256),
                ("runner.candidate_config_sha256", binding.candidate_config_sha256, candidate_sha256),
                ("runner.official_contract_sha256", binding.official_contract_sha256, contract_hashes[label]),
                ("runner.collection_id", binding.collection_id, expected_collection_id),
                ("runner.collection_cutoff_utc_ns_exclusive", binding.payload.get("collection_cutoff_utc_ns_exclusive"), expected_cutoff),
            ):
                context_checks.append(
                    {"expected": expected, "field": field, "observed": observed, "ok": observed == expected}
                )
        if binding is not None:
            manifest_hash = result.get("manifest_sha256")
            if isinstance(manifest_hash, str) and _SHA256.fullmatch(manifest_hash):
                manifest_path = f"payload/venues/{label}/raw/manifests/{manifest_hash}.manifest.json"
                raw_manifest = members.get(manifest_path)
                manifest_error: str | None = None
                try:
                    if raw_manifest is None:
                        raise ValueError("head manifest absent from forensic archive")
                    decoded_manifest = _manifest_metadata(
                        raw_manifest,
                        expected_sha256=manifest_hash,
                    )
                    manifest_checks = (
                        ("raw_manifest.root_sha256", decoded_manifest["root_sha256"], result.get("root_sha256")),
                        ("raw_manifest.frame_count", decoded_manifest["frame_count"], result.get("frames")),
                        ("raw_manifest.stored_segment_bytes", decoded_manifest["stored_segment_bytes"], result.get("bytes")),
                        ("raw_manifest.segment_count", decoded_manifest["segment_count"], result.get("segments")),
                    )
                    for field, observed, expected_value in manifest_checks:
                        context_checks.append(
                            {
                                "expected": expected_value,
                                "field": field,
                                "observed": observed,
                                "ok": observed == expected_value,
                            }
                        )
                except (ForensicError, ValueError) as error:
                    manifest_error = f"{type(error).__name__}:{error}"
                context_checks.append(
                    {
                        "expected": "content-addressed manifest metadata authenticated",
                        "field": "raw_manifest.metadata_authentication",
                        "observed": manifest_error,
                        "ok": manifest_error is None,
                    }
                )
        first = next((item for item in runtime_checks if item["ok"] is False), None)
        reports[label] = {
            "checks": runtime_checks,
            "context_checks_after_result_stage": context_checks,
            "first_divergence": first,
            "result_terminal_health": result.get("terminal_health"),
            "runtime_error_class": (
                "prediction terminal collection result is not admissible"
                if first is not None and str(first["field"]).startswith((
                    "frames", "bytes", "segments", "elapsed_ms", "network_calls",
                    "binding.", "boundary", "schema_version", "requested_duration_seconds",
                    "manifest_sha256", "root_sha256", "terminal_health", "error.",
                    "probe_binding_sha256", "campaign_manifest_sha256",
                    "candidate_config_sha256", "official_contract_sha256",
                    "collection_id", "venue", "connection_attempts", "limitations",
                    "duplicates", "gaps", "queue_high_water", "reconnects",
                    "source_timestamp",
                ))
                else None
            ),
        }
    divergent = {
        venue: cast(Mapping[str, object], report).get("first_divergence")
        for venue, report in reports.items()
    }
    status = (
        "PREDICTION_MARKETS_RECEIPT_AUTH_DIVERGENCE_IDENTIFIED"
        if all(value is not None for value in divergent.values())
        else "PREDICTION_MARKETS_RECEIPT_AUTH_RESULT_STAGE_NOT_FULLY_EXPLAINED"
    )
    return {
        "boundary": BOUNDARY,
        "forensic_inventory_sha256": inventory["inventory_sha256"],
        "raw_segments_read": 0,
        "reports": reports,
        "source_commit": expected_source_commit,
        "status": status,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets receipt-auth forensics")
    subparsers = parser.add_subparsers(dest="command", required=True)
    export = subparsers.add_parser("export")
    export.add_argument("--campaign-root", type=Path, required=True)
    export.add_argument("--incoming-root", type=Path, required=True)
    export.add_argument("--source-root", type=Path, required=True)
    export.add_argument("--output-root", type=Path, required=True)
    export.add_argument("--expected-source-commit", required=True)
    diagnose = subparsers.add_parser("diagnose")
    diagnose.add_argument("--bundle-root", type=Path, required=True)
    diagnose.add_argument("--expected-source-commit", required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "export":
            result = export_forensics(
                campaign_root=arguments.campaign_root,
                incoming_root=arguments.incoming_root,
                source_root=arguments.source_root,
                output_root=arguments.output_root,
                expected_source_commit=arguments.expected_source_commit,
            )
        else:
            result = diagnose_forensics(
                arguments.bundle_root,
                expected_source_commit=arguments.expected_source_commit,
            )
    except (ForensicError, OSError, ValueError) as error:
        _fail(str(error))
    print(canonical_json_bytes(result).decode("utf-8"))
    print(cast(str, result["status"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
