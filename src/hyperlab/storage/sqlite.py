from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from hyperlab.api.public import CarrySnapshot

SCHEMA = """
CREATE TABLE IF NOT EXISTS carry_snapshots (
    observed_at_ms INTEGER NOT NULL,
    asset TEXT NOT NULL,
    spot_pair TEXT NOT NULL,
    spot_mid TEXT NOT NULL,
    perp_mid TEXT NOT NULL,
    funding_hourly TEXT NOT NULL,
    basis_bps TEXT NOT NULL,
    perp_volume_usd TEXT NOT NULL,
    spot_volume_usd TEXT NOT NULL,
    open_interest TEXT NOT NULL,
    PRIMARY KEY (observed_at_ms, asset)
);
CREATE TABLE IF NOT EXISTS collector_events (
    observed_at_ms INTEGER NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);
"""


def initialize(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA)


def save_carry_snapshots(path: Path, snapshots: Iterable[CarrySnapshot]) -> int:
    initialize(path)
    rows = [
        (
            item.observed_at_ms,
            item.asset,
            item.spot_pair,
            str(item.spot_mid),
            str(item.perp_mid),
            str(item.funding_hourly),
            str(item.basis_bps),
            str(item.perp_volume_usd),
            str(item.spot_volume_usd),
            str(item.open_interest),
        )
        for item in snapshots
    ]
    with sqlite3.connect(path) as connection:
        connection.executemany(
            """
            INSERT OR REPLACE INTO carry_snapshots (
                observed_at_ms, asset, spot_pair, spot_mid, perp_mid,
                funding_hourly, basis_bps, perp_volume_usd,
                spot_volume_usd, open_interest
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def database_status(path: Path) -> dict[str, int | None]:
    if not path.exists():
        return {"snapshot_count": 0, "last_observed_at_ms": None}
    initialize(path)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT COUNT(*), MAX(observed_at_ms) FROM carry_snapshots"
        ).fetchone()
    return {
        "snapshot_count": int(row[0]) if row else 0,
        "last_observed_at_ms": int(row[1]) if row and row[1] is not None else None,
    }


def write_runtime_status(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)
