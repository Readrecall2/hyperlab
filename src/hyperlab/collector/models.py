from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from hyperlab.data.schema import RecordType

# `1M` stays rejected until the lake models non-fixed calendar intervals.

VALID_CANDLE_INTERVALS = frozenset(
    {
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
    }
)
PUBLIC_SUBSCRIPTION_CHANNELS = (
    "activeAssetCtx",
    "bbo",
    "l2Book",
    "trades",
    "candle",
)
_PUBLIC_SUBSCRIPTION_CHANNEL_SET = frozenset(PUBLIC_SUBSCRIPTION_CHANNELS)


class CollectorState(StrEnum):
    STOPPED = "stopped"
    BOOTSTRAPPING = "bootstrapping"
    CONNECTING = "connecting"
    SUBSCRIBING = "subscribing"
    RESYNCING = "resyncing"
    LIVE = "live"
    BACKOFF = "backoff"


@dataclass(frozen=True, slots=True)
class WireEnvelope:
    raw_message: str
    received_time: datetime
    connection_id: str
    connection_epoch: int
    arrival_sequence: int
    capture_epoch_id: str | None = None

    def __post_init__(self) -> None:
        if self.received_time.tzinfo is None or self.received_time.utcoffset() is None:
            raise ValueError("received_time must be timezone-aware")
        if self.received_time.utcoffset() != UTC.utcoffset(self.received_time):
            raise ValueError("received_time must use UTC")
        if not self.connection_id:
            raise ValueError("connection_id must not be empty")
        if self.connection_epoch < 1:
            raise ValueError("connection_epoch must be positive")
        if self.arrival_sequence < 1:
            raise ValueError("arrival_sequence must be positive")
        if self.capture_epoch_id is not None and not self.capture_epoch_id.strip():
            raise ValueError("capture_epoch_id must be non-empty when present")


@dataclass(frozen=True, slots=True)
class ParsedRecord:
    record_type: RecordType
    asset: str
    row: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ParsedMessage:
    channel: str | None
    records: tuple[ParsedRecord, ...]
    issues: tuple[str, ...] = ()
    acknowledged_subscription: Mapping[str, Any] | None = None
    is_pong: bool = False


@dataclass(frozen=True, slots=True)
class PublicSubscription:
    channel: str
    coin: str
    interval: str | None = None

    def __post_init__(self) -> None:
        if self.channel not in _PUBLIC_SUBSCRIPTION_CHANNEL_SET:
            raise ValueError(f"unsupported public channel: {self.channel}")
        if not self.coin or any(key in self.coin.lower() for key in ("0x", "user", "wallet")):
            raise ValueError("coin must be a public market symbol")
        if ":" in self.coin:
            raise ValueError("coin must not contain the stream key separator ':'")
        if self.channel == "candle" and not self.interval:
            raise ValueError("candle subscription requires an interval")
        if self.channel != "candle" and self.interval is not None:
            raise ValueError("interval is only valid for candle subscriptions")
        if self.interval is not None and self.interval not in VALID_CANDLE_INTERVALS:
            raise ValueError(f"unsupported candle interval: {self.interval}")

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> PublicSubscription:
        interval = payload.get("interval")
        return cls(
            channel=str(payload.get("type", "")),
            coin=str(payload.get("coin", "")),
            interval=None if interval is None else str(interval),
        )

    @property
    def key(self) -> str:
        suffix = f":{self.interval}" if self.interval is not None else ""
        return f"{self.channel}:{self.coin}{suffix}"

    def payload(self) -> dict[str, str]:
        payload = {"type": self.channel, "coin": self.coin}
        if self.interval is not None:
            payload["interval"] = self.interval
        return payload


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    network: str = "mainnet"
    assets: tuple[str, ...] = ("BTC",)
    candle_intervals: tuple[str, ...] = ("1m",)
    subscription_channels: tuple[str, ...] = PUBLIC_SUBSCRIPTION_CHANNELS
    collect_funding_history: bool = True
    batch_size: int = 500
    reconnect_on_rest_refresh_failure: bool = False
    critical_funding_history: bool = False
    flush_interval_seconds: float = 5.0
    heartbeat_interval_seconds: float = 20.0
    pong_timeout_seconds: float = 45.0
    ws_connect_timeout_seconds: float = 15.0
    stale_after_seconds: float = 30.0
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    backoff_jitter_ratio: float = 0.2
    backoff_reset_after_seconds: float = 60.0
    history_lookback_hours: int = 24
    rest_refresh_interval_seconds: float = 300.0
    funding_grace_seconds: float = 600.0
    queue_capacity: int = 10_000

    def __post_init__(self) -> None:
        if self.network not in {"mainnet", "testnet"}:
            raise ValueError(f"unsupported network: {self.network}")
        if not self.assets or len(set(self.assets)) != len(self.assets):
            raise ValueError("assets must be a non-empty unique list")
        if not self.subscription_channels or len(set(self.subscription_channels)) != len(
            self.subscription_channels
        ):
            raise ValueError("subscription_channels must be a non-empty unique list")
        invalid_channels = sorted(set(self.subscription_channels) - _PUBLIC_SUBSCRIPTION_CHANNEL_SET)
        if invalid_channels:
            raise ValueError(f"unsupported public subscription channels: {invalid_channels}")
        has_candles = "candle" in self.subscription_channels
        if has_candles and (
            not self.candle_intervals or len(set(self.candle_intervals)) != len(self.candle_intervals)
        ):
            raise ValueError("candle_intervals must be a non-empty unique list when candle is subscribed")
        if not has_candles and self.candle_intervals:
            raise ValueError("candle_intervals must be empty when candle is not subscribed")
        invalid_intervals = sorted(set(self.candle_intervals) - VALID_CANDLE_INTERVALS)
        if invalid_intervals:
            raise ValueError(f"unsupported candle intervals: {invalid_intervals}")
        boolean_policies = {
            "collect_funding_history": self.collect_funding_history,
            "critical_funding_history": self.critical_funding_history,
            "reconnect_on_rest_refresh_failure": self.reconnect_on_rest_refresh_failure,
        }
        if any(type(value) is not bool for value in boolean_policies.values()):
            raise ValueError("collector boolean policies must be boolean")
        for asset in self.assets:
            PublicSubscription(channel="activeAssetCtx", coin=asset)
        positive = {
            "batch_size": self.batch_size,
            "flush_interval_seconds": self.flush_interval_seconds,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "pong_timeout_seconds": self.pong_timeout_seconds,
            "ws_connect_timeout_seconds": self.ws_connect_timeout_seconds,
            "stale_after_seconds": self.stale_after_seconds,
            "backoff_initial_seconds": self.backoff_initial_seconds,
            "backoff_max_seconds": self.backoff_max_seconds,
            "backoff_reset_after_seconds": self.backoff_reset_after_seconds,
            "history_lookback_hours": self.history_lookback_hours,
            "rest_refresh_interval_seconds": self.rest_refresh_interval_seconds,
            "funding_grace_seconds": self.funding_grace_seconds,
            "queue_capacity": self.queue_capacity,
        }
        if any(value <= 0 for value in positive.values()):
            raise ValueError("collector limits and intervals must be positive")
        if self.backoff_initial_seconds > self.backoff_max_seconds:
            raise ValueError("initial backoff cannot exceed maximum backoff")
        if not 0 <= self.backoff_jitter_ratio <= 1:
            raise ValueError("backoff_jitter_ratio must be between 0 and 1")
        if self.pong_timeout_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("pong timeout must exceed heartbeat interval")
        if self.subscription_count > 1_000:
            raise ValueError("Hyperliquid permits at most 1000 subscriptions per connection")

    @property
    def subscription_count(self) -> int:
        non_candle_channels = sum(channel != "candle" for channel in self.subscription_channels)
        candle_count = len(self.candle_intervals) if "candle" in self.subscription_channels else 0
        return len(self.assets) * (non_candle_channels + candle_count)

    @property
    def rest_refresh_enabled(self) -> bool:
        return self.collect_funding_history or "candle" in self.subscription_channels

    def subscriptions(self) -> tuple[PublicSubscription, ...]:
        result: list[PublicSubscription] = []
        for coin in self.assets:
            for channel in self.subscription_channels:
                if channel == "candle":
                    result.extend(
                        PublicSubscription(
                            channel="candle",
                            coin=coin,
                            interval=interval,
                        )
                        for interval in self.candle_intervals
                    )
                else:
                    result.append(PublicSubscription(channel=channel, coin=coin))
        return tuple(result)


@dataclass(slots=True)
class CollectorMetrics:
    state: CollectorState = CollectorState.STOPPED
    connection_epoch: int = 0
    connections: int = 0
    reconnects: int = 0
    resyncs: int = 0
    gaps: int = 0
    pings_sent: int = 0
    pongs_received: int = 0
    subscription_acks: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None
    last_failure: str | None = None
    last_failure_at: datetime | None = None
    last_recovered_at: datetime | None = None
    connection_alive: bool = False
    messages_received: int = 0
    records_parsed: int = 0
    normalization_issues: int = 0
    batches_written: int = 0
    rows_written: int = 0
    duplicates_suppressed: int = 0
    queue_high_water: int = 0
    rest_refreshes: int = 0
    last_received_at: datetime | None = None
    last_pong_at: datetime | None = None
    last_event_by_channel: dict[str, datetime] = field(default_factory=dict)
    last_exchange_event_by_channel: dict[str, datetime] = field(default_factory=dict)
    last_funding_by_asset: dict[str, datetime] = field(default_factory=dict)
    stale_channels: tuple[str, ...] = ()
    current_backoff_seconds: float = 0.0

    def as_dict(self, now: datetime) -> dict[str, object]:
        def age(value: datetime | None) -> float | None:
            return None if value is None else max((now - value).total_seconds(), 0.0)

        return {
            "state": self.state.value,
            "connection_epoch": self.connection_epoch,
            "connections": self.connections,
            "reconnects": self.reconnects,
            "resyncs": self.resyncs,
            "gaps": self.gaps,
            "pings_sent": self.pings_sent,
            "pongs_received": self.pongs_received,
            "subscription_acks": self.subscription_acks,
            "connection_alive": self.connection_alive,
            "messages_received": self.messages_received,
            "records_parsed": self.records_parsed,
            "normalization_issues": self.normalization_issues,
            "batches_written": self.batches_written,
            "rows_written": self.rows_written,
            "duplicates_suppressed": self.duplicates_suppressed,
            "queue_high_water": self.queue_high_water,
            "rest_refreshes": self.rest_refreshes,
            "last_receive_age_seconds": age(self.last_received_at),
            "last_pong_age_seconds": age(self.last_pong_at),
            "last_error": self.last_error,
            "last_error_age_seconds": age(self.last_error_at),
            "last_failure": self.last_failure,
            "last_failure_age_seconds": age(self.last_failure_at),
            "last_recovery_age_seconds": age(self.last_recovered_at),
            "channel_ingest_age_seconds": {
                channel: age(value) for channel, value in sorted(self.last_event_by_channel.items())
            },
            "channel_event_age_seconds": {
                channel: age(value) for channel, value in sorted(self.last_exchange_event_by_channel.items())
            },
            "funding_event_age_seconds": {
                asset: age(value) for asset, value in sorted(self.last_funding_by_asset.items())
            },
            "stale_channels": list(self.stale_channels),
            "current_backoff_seconds": self.current_backoff_seconds,
        }
