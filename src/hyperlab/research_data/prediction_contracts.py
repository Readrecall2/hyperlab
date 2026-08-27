from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from .canonical import CanonicalValue, canonical_json_bytes, canonical_value
from .envelope import SYNTHETIC_FIXTURE_LABEL, CaptureProvenance, Venue
from .prediction import (
    MarketRuleVersion,
    OutcomeIdentity,
    RelationStatus,
    RelationType,
    SemanticCatalog,
    SemanticRelation,
)
from .prediction_evidence import PredictionRawEvidenceIndex, PredictionRawRecordRef

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
_OFFICIAL_HOSTS = {
    Venue.POLYMARKET: {
        "clob.polymarket.com",
        "data-api.polymarket.com",
        "docs.polymarket.com",
        "gamma-api.polymarket.com",
        "ws-subscriptions-clob.polymarket.com",
    },
    Venue.KALSHI: {
        "docs.kalshi.com",
        "external-api.kalshi.com",
        "kalshi.com",
    },
}
_POLYMARKET_CLOB_HOST = "clob.polymarket.com"
_KALSHI_PUBLIC_HOST = "external-api.kalshi.com"


def polymarket_gamma_token_outcomes(
    market: Mapping[str, object],
) -> tuple[tuple[str, str], ...]:
    """Return the exact binary Gamma token/outcome relation."""

    labels = _json_array(market.get("outcomes"), label="Polymarket outcomes")
    tokens = _json_array(market.get("clobTokenIds"), label="Polymarket CLOB tokens")
    if len(labels) != 2 or len(tokens) != 2:
        raise ValueError("Polymarket binary outcome/token relation is incomplete")
    normalized_labels = tuple(
        _text(label, label="Polymarket outcome label").upper() for label in labels
    )
    normalized_tokens = tuple(
        _text(token, label="Polymarket token id") for token in tokens
    )
    if normalized_labels != ("YES", "NO") or len(set(normalized_tokens)) != 2:
        raise ValueError("Polymarket binary token/outcome identities diverged")
    return tuple(zip(normalized_tokens, normalized_labels, strict=True))


def normalize_polymarket_clob_v2_market(
    record: Mapping[str, object],
    *,
    provenance: CaptureProvenance,
    expected_condition_id: str,
    expected_token_outcomes: Sequence[tuple[str, str]],
) -> Mapping[str, object]:
    """Authenticate the current compact CLOB V2 body against its path and Gamma graph."""

    parsed = urlsplit(provenance.source_url)
    public_path = (
        provenance.transport == "PUBLIC_HTTP"
        and provenance.fixture_label is None
        and parsed.scheme == "https"
        and parsed.hostname == _POLYMARKET_CLOB_HOST
        and parsed.username is None
        and parsed.password is None
        and parsed.port is None
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path == f"/clob-markets/{expected_condition_id}"
    )
    fixture_path = (
        provenance.transport == "FIXTURE"
        and provenance.fixture_label == SYNTHETIC_FIXTURE_LABEL
        and parsed.scheme == "fixture"
        and parsed.netloc == "prediction-markets-v1"
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path == f"/polymarket/clob-markets/{expected_condition_id}"
    )
    if not public_path and not fixture_path:
        raise ValueError("Polymarket CLOB V2 provenance path diverged")
    legacy_aliases = {"condition_id", "conditionId", "tokens", "token_id", "tokenId"}
    if legacy_aliases.intersection(record):
        raise ValueError("Polymarket CLOB V2 record mixes legacy identity aliases")
    raw_tokens = _sequence(record.get("t"), label="Polymarket CLOB V2 tokens")
    if len(raw_tokens) != 2:
        raise ValueError("Polymarket CLOB V2 binary token list is incomplete")
    observed: list[tuple[str, str]] = []
    for raw_token in raw_tokens:
        token = _mapping(raw_token, label="Polymarket CLOB V2 token")
        if set(token) != {"o", "t"}:
            raise ValueError("Polymarket CLOB V2 token fields diverged")
        token_id = _text(token.get("t"), label="Polymarket CLOB V2 token id")
        outcome = _text(token.get("o"), label="Polymarket CLOB V2 outcome").upper()
        if len(token_id) > 128 or outcome not in {"YES", "NO"}:
            raise ValueError("Polymarket CLOB V2 token identity is invalid")
        observed.append((token_id, outcome))
    expected = tuple(expected_token_outcomes)
    if (
        len(set(observed)) != 2
        or {item[0] for item in observed} != {item[0] for item in expected}
        or dict(observed) != dict(expected)
    ):
        raise ValueError("Polymarket Gamma/CLOB V2 token-outcome relation diverged")
    return {
        "condition_id": expected_condition_id,
        "fd": record.get("fd"),
        "tokens": [
            {"outcome": outcome, "token_id": token_id}
            for token_id, outcome in observed
        ],
    }


def normalize_kalshi_event_metadata(
    record: Mapping[str, object],
    *,
    provenance: CaptureProvenance,
    expected_event_ticker: str,
    expected_market_ticker: str,
) -> Mapping[str, object]:
    """Bind the direct official metadata response to its event path."""

    parsed = urlsplit(provenance.source_url)
    expected_path = f"/trade-api/v2/events/{expected_event_ticker}/metadata"
    public_path = (
        provenance.transport == "PUBLIC_HTTP"
        and provenance.fixture_label is None
        and parsed.scheme == "https"
        and parsed.hostname == _KALSHI_PUBLIC_HOST
        and parsed.username is None
        and parsed.password is None
        and parsed.port is None
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path == expected_path
    )
    fixture_path = (
        provenance.transport == "FIXTURE"
        and provenance.fixture_label == SYNTHETIC_FIXTURE_LABEL
        and parsed.scheme == "fixture"
        and parsed.netloc == "prediction-markets-v1"
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path == f"/kalshi/events/{expected_event_ticker}/metadata"
    )
    if not public_path and not fixture_path:
        raise ValueError("Kalshi event metadata provenance path diverged")
    if "event_metadata" in record or "event_ticker" in record:
        raise ValueError("Kalshi direct event metadata mixes legacy identity fields")
    market_details = _sequence(
        record.get("market_details"), label="Kalshi event metadata market details"
    )
    settlement_sources = _sequence(
        record.get("settlement_sources"), label="Kalshi settlement sources"
    )
    if not market_details or not settlement_sources:
        raise ValueError("Kalshi settlement sources are incomplete")
    if len(market_details) > 1_000 or len(settlement_sources) > 100:
        raise ValueError("Kalshi event metadata arrays exceed their bounded contract")
    market_tickers: list[str] = []
    for item in market_details:
        detail = _mapping(item, label="Kalshi event metadata market detail")
        ticker = _text(detail.get("market_ticker"), label="Kalshi metadata market ticker")
        if len(ticker) > 128:
            raise ValueError("Kalshi metadata market ticker is oversized")
        market_tickers.append(ticker)
    if len(set(market_tickers)) != len(market_tickers):
        raise ValueError("Kalshi event metadata market ticker is duplicated")
    if market_tickers.count(expected_market_ticker) != 1:
        raise ValueError("Kalshi event metadata does not bind the selected market")
    for item in settlement_sources:
        source = _mapping(item, label="Kalshi settlement source")
        name = _text(source.get("name"), label="Kalshi settlement source name")
        url = _text(source.get("url"), label="Kalshi settlement source URL")
        if len(name.encode("utf-8")) > 512 or len(url.encode("utf-8")) > 2_048:
            raise ValueError("Kalshi settlement source is oversized")
    return {
        "event_ticker": expected_event_ticker,
        "market_details": list(market_details),
        "settlement_sources": list(settlement_sources),
    }


class EvidenceClassification(StrEnum):
    DOCUMENTED = "DOCUMENTED"
    OBSERVED_PUBLICLY = "OBSERVED_PUBLICLY"
    INFERRED = "INFERRED"
    UNKNOWN_NOT_OBSERVED = "UNKNOWN_NOT_OBSERVED"


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must be an array")
    return value


def _text(value: object, *, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    return value


def _json_array(value: object, *, label: str) -> Sequence[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{label} is not a JSON array") from error
    return _sequence(value, label=label)


@dataclass(frozen=True, slots=True)
class PublicEndpointContract:
    endpoint_id: str
    method: str
    url: str
    classification: EvidenceClassification
    pagination: Mapping[str, CanonicalValue] | None
    query: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.method not in {"GET", "WSS"}:
            raise ValueError("prediction public endpoint method is not read-only")
        parsed = urlsplit(self.url.replace("{", "").replace("}", ""))
        if parsed.scheme not in {"https", "wss"} or parsed.hostname is None:
            raise ValueError("prediction public endpoint must use HTTPS or WSS")


@dataclass(frozen=True, slots=True)
class OfficialPublicContract:
    venue: Venue
    contract_id: str
    capture_date: str
    accessibility: EvidenceClassification
    endpoints: tuple[PublicEndpointContract, ...]
    sources: tuple[str, ...]
    payload: Mapping[str, CanonicalValue]
    contract_sha256: str

    @classmethod
    def from_bytes(cls, raw: bytes) -> OfficialPublicContract:
        try:
            decoded = json.loads(
                raw.decode("utf-8"),
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("public contract must be strict UTF-8 JSON") from error
        root = _mapping(decoded, label="public contract")
        if root.get("boundary") != BOUNDARY or root.get("schema_version") != 1:
            raise ValueError("public contract boundary or schema is invalid")
        venue = Venue(_text(root.get("venue"), label="public contract venue"))
        if venue not in {Venue.POLYMARKET, Venue.KALSHI}:
            raise ValueError("public contract supports prediction venues only")
        vocabulary = tuple(
            _text(item, label="classification vocabulary item")
            for item in _sequence(
                root.get("classification_vocabulary"),
                label="classification vocabulary",
            )
        )
        if set(vocabulary) != {item.value for item in EvidenceClassification}:
            raise ValueError("public contract classification vocabulary is incomplete")
        accessibility = EvidenceClassification(
            _text(
                _mapping(root.get("accessibility"), label="accessibility").get("classification"),
                label="accessibility classification",
            )
        )
        endpoint_items = _sequence(root.get("endpoints"), label="public endpoints")
        endpoints: list[PublicEndpointContract] = []
        for item in endpoint_items:
            endpoint = _mapping(item, label="public endpoint")
            raw_url = endpoint.get("url") or endpoint.get("url_template")
            url = _text(raw_url, label="public endpoint URL")
            parsed = urlsplit(url.replace("{", "").replace("}", ""))
            if parsed.hostname not in _OFFICIAL_HOSTS[venue]:
                raise ValueError("public endpoint host is not allowlisted for its venue")
            pagination_raw = endpoint.get("pagination")
            pagination = None
            if pagination_raw is not None:
                canonical_pagination = canonical_value(pagination_raw)
                if not isinstance(canonical_pagination, dict):
                    raise ValueError("endpoint pagination contract must be an object")
                pagination = canonical_pagination
            query_raw = endpoint.get("query", ())
            if not isinstance(query_raw, Sequence) or isinstance(query_raw, (str, bytes, bytearray)):
                raise ValueError("endpoint query contract must be an array")
            query = tuple(_text(item, label="endpoint query field") for item in query_raw)
            if len(query) != len(set(query)):
                raise ValueError("endpoint query fields must be unique")
            endpoints.append(
                PublicEndpointContract(
                    endpoint_id=_text(endpoint.get("id"), label="endpoint id"),
                    method=_text(endpoint.get("method"), label="endpoint method"),
                    url=url,
                    classification=EvidenceClassification(
                        _text(
                            endpoint.get("classification"),
                            label="endpoint classification",
                        )
                    ),
                    pagination=pagination,
                    query=query,
                )
            )
        if len({item.endpoint_id for item in endpoints}) != len(endpoints):
            raise ValueError("public endpoint ids must be unique")
        source_items = tuple(
            _text(item, label="official source")
            for item in _sequence(root.get("sources"), label="official sources")
        )
        if not source_items or len(set(source_items)) != len(source_items):
            raise ValueError("official source URLs must be unique and non-empty")
        for source in source_items:
            parsed = urlsplit(source)
            if parsed.scheme != "https" or parsed.hostname not in _OFFICIAL_HOSTS[venue]:
                raise ValueError("contract source is not an allowlisted official HTTPS URL")
        canonical = canonical_value(root)
        if not isinstance(canonical, dict):
            raise AssertionError("canonical contract root must remain an object")
        canonical_bytes = canonical_json_bytes(canonical)
        return cls(
            venue=venue,
            contract_id=_text(root.get("contract_id"), label="public contract id"),
            capture_date=_text(root.get("capture_date"), label="capture date"),
            accessibility=accessibility,
            endpoints=tuple(endpoints),
            sources=source_items,
            payload=canonical,
            contract_sha256=hashlib.sha256(canonical_bytes).hexdigest(),
        )

    @classmethod
    def from_path(cls, path: Path) -> OfficialPublicContract:
        return cls.from_bytes(path.read_bytes())

    def endpoint(self, endpoint_id: str) -> PublicEndpointContract:
        matches = [item for item in self.endpoints if item.endpoint_id == endpoint_id]
        if len(matches) != 1:
            raise KeyError(f"public endpoint contract not found: {endpoint_id}")
        return matches[0]


@dataclass(slots=True)
class BoundedCursorPager:
    max_pages: int
    max_items: int
    pages: int = 0
    items: int = 0
    _seen_cursors: set[str] = field(default_factory=set)
    _seen_items: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.max_pages <= 0 or self.max_items <= 0:
            raise ValueError("cursor pagination limits must be positive")

    def admit(
        self,
        *,
        requested_cursor: str | None,
        next_cursor: str | None,
        item_ids: Sequence[str],
    ) -> tuple[str, ...]:
        if self.pages >= self.max_pages:
            raise BufferError("PAGINATION_MAX_PAGES_REACHED")
        if requested_cursor is not None:
            if requested_cursor in self._seen_cursors:
                raise ValueError("PAGINATION_CURSOR_REPEATED")
            self._seen_cursors.add(requested_cursor)
        if next_cursor is not None and next_cursor in self._seen_cursors:
            raise ValueError("PAGINATION_CURSOR_CYCLE")
        if any(type(item) is not str or not item for item in item_ids):
            raise ValueError("pagination item ids must be non-empty text")
        self.pages += 1
        admitted: list[str] = []
        for item_id in item_ids:
            if item_id in self._seen_items:
                continue
            if self.items >= self.max_items:
                raise BufferError("PAGINATION_MAX_ITEMS_REACHED")
            self._seen_items.add(item_id)
            admitted.append(item_id)
            self.items += 1
        return tuple(admitted)


@dataclass(frozen=True, slots=True)
class PredictionIdentityGraph:
    venue: Venue
    series_id: str | None
    event_id: str
    market_id: str
    outcomes: tuple[OutcomeIdentity, ...]
    rule_version: MarketRuleVersion
    negative_risk: bool
    multivariate: bool
    raw_graph_sha256: str
    execution_admissible: bool
    ineligibility_reasons: tuple[str, ...]
    source_refs: tuple[PredictionRawRecordRef, ...] = ()

    def __post_init__(self) -> None:
        if self.venue not in {Venue.POLYMARKET, Venue.KALSHI}:
            raise ValueError("prediction identity graph requires a prediction venue")
        if not self.event_id or not self.market_id or len(self.outcomes) < 2:
            raise ValueError("prediction identity graph is incomplete")
        if self.rule_version.economic_market_id != self.market_id:
            raise ValueError("identity graph and rule version market differ")
        graph_outcomes = tuple(sorted(self.outcomes, key=lambda item: (item.outcome_id, item.outcome_label)))
        if self.rule_version.outcomes != graph_outcomes:
            raise ValueError("identity graph and rule version outcomes differ")
        if self.multivariate:
            raise ValueError("multivariate prediction markets are fail-closed in candidate v1")
        if self.execution_admissible == bool(self.ineligibility_reasons):
            raise ValueError("prediction graph eligibility and reasons are inconsistent")
        if (
            tuple(
                sorted(
                    set(self.source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            )
            != self.source_refs
        ):
            raise ValueError("prediction graph source references must be unique and causal")

    @property
    def source_content_sha256s(self) -> tuple[str, ...]:
        return tuple(sorted({item.content_sha256 for item in self.source_refs}))

    @classmethod
    def from_polymarket(
        cls,
        *,
        market: Mapping[str, object],
        event: Mapping[str, object],
        clob_markets: Sequence[Mapping[str, object]],
        source_metadata_version: str,
        source_refs: Sequence[PredictionRawRecordRef] = (),
    ) -> PredictionIdentityGraph:
        market_id = _text(market.get("conditionId"), label="Polymarket condition id")
        gamma_market_id = _text(market.get("id"), label="Polymarket Gamma market id")
        _text(market.get("questionID"), label="Polymarket resolution question id")
        event_id = _text(event.get("id"), label="Polymarket event id")
        embedded_events = market.get("events")
        if not isinstance(embedded_events, list) or not embedded_events:
            raise ValueError("Polymarket market omitted its event relationship")
        embedded_ids = {
            str(item.get("id"))
            for item in embedded_events
            if isinstance(item, Mapping) and item.get("id") is not None
        }
        if embedded_ids != {event_id}:
            raise ValueError("Polymarket event/market relationship is ambiguous")
        event_markets = event.get("markets")
        if not isinstance(event_markets, list) or not any(
            isinstance(item, Mapping) and str(item.get("id")) == gamma_market_id for item in event_markets
        ):
            raise ValueError("Polymarket event does not reference the Gamma market")
        token_outcomes = polymarket_gamma_token_outcomes(market)
        tokens = tuple(item[0] for item in token_outcomes)
        labels = tuple(item[1] for item in token_outcomes)
        if not clob_markets:
            raise ValueError("Polymarket graph requires authenticated CLOB market identity")
        clob_conditions: set[str] = set()
        clob_tokens: set[str] = set()
        clob_token_outcomes: dict[str, str] = {}
        for clob_market in clob_markets:
            raw_condition = clob_market.get("condition_id", clob_market.get("conditionId"))
            clob_conditions.add(_text(raw_condition, label="Polymarket CLOB condition id"))
            raw_tokens = clob_market.get("tokens")
            if isinstance(raw_tokens, list):
                for raw_token in raw_tokens:
                    token_mapping = _mapping(raw_token, label="Polymarket CLOB token")
                    token_id = _text(
                        token_mapping.get("token_id", token_mapping.get("tokenId")),
                        label="Polymarket CLOB token id",
                    )
                    clob_tokens.add(token_id)
                    outcome = _text(
                        token_mapping.get("outcome"),
                        label="Polymarket CLOB token outcome",
                    ).upper()
                    if token_id in clob_token_outcomes:
                        raise ValueError("Polymarket CLOB token identity is duplicated")
                    clob_token_outcomes[token_id] = outcome
            direct_token = clob_market.get("token_id", clob_market.get("tokenId"))
            if direct_token is not None:
                clob_tokens.add(_text(direct_token, label="Polymarket CLOB token id"))
        expected_tokens = set(tokens)
        if (
            clob_conditions != {market_id}
            or clob_tokens != expected_tokens
            or clob_token_outcomes != dict(token_outcomes)
        ):
            raise ValueError("Polymarket Gamma/CLOB market-token identity diverged")
        neg_risk = market.get("negRisk")
        enable_neg_risk = market.get("enableNegRisk")
        if type(neg_risk) is not bool or type(enable_neg_risk) is not bool:
            raise ValueError("Polymarket negative-risk flags must be explicit booleans")
        if neg_risk != enable_neg_risk:
            raise ValueError("Polymarket negative-risk flags diverged")
        augmented = event.get("negRiskAugmented")
        if neg_risk and type(augmented) is not bool:
            raise ValueError("Polymarket negative-risk event augmentation is unauthenticated")
        outcomes = tuple(
            OutcomeIdentity(
                venue=Venue.POLYMARKET,
                economic_market_id=market_id,
                outcome_id=_text(token, label="Polymarket token id"),
                outcome_label=_text(label, label="Polymarket outcome label"),
            )
            for label, token in zip(labels, tokens, strict=True)
        )
        graph_body = {
            "clob_markets": [
                {
                    "condition_id": item.get("condition_id", item.get("conditionId")),
                    "token_id": item.get("token_id", item.get("tokenId")),
                    "tokens": item.get("tokens"),
                }
                for item in clob_markets
            ],
            "event": {
                "id": event_id,
                "negRiskAugmented": event.get("negRiskAugmented"),
                "seriesSlug": event.get("seriesSlug"),
            },
            "market": {
                "acceptingOrders": market.get("acceptingOrders"),
                "archived": market.get("archived"),
                "closed": market.get("closed"),
                "closedTime": market.get("closedTime"),
                "conditionId": market_id,
                "enableNegRisk": market.get("enableNegRisk"),
                "endDate": market.get("endDate"),
                "enableOrderBook": market.get("enableOrderBook"),
                "id": gamma_market_id,
                "negRisk": market.get("negRisk"),
                "questionID": market.get("questionID"),
                "resolutionSource": market.get("resolutionSource"),
                "restricted": market.get("restricted"),
                "rules": market.get("rules") or market.get("description"),
                "startDate": market.get("startDate"),
            },
            "outcomes": [item.to_dict() for item in outcomes],
            "source_refs": [
                item.to_dict()
                for item in sorted(
                    set(source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            ],
        }
        raw_graph_sha256 = hashlib.sha256(canonical_json_bytes(graph_body)).hexdigest()
        rule_observation = {key: value for key, value in graph_body.items() if key != "source_refs"}
        rule_content_sha256 = hashlib.sha256(
            canonical_json_bytes(rule_observation)
        ).hexdigest()
        rule_text = _text(
            market.get("rules")
            or market.get("description")
            or event.get("rules")
            or event.get("description"),
            label="Polymarket rules",
        )
        resolution_source = _text(
            market.get("resolutionSource") or event.get("resolutionSource"),
            label="Polymarket resolution source",
        )
        ineligibility_reasons: list[str] = []
        operational_requirements = {
            "acceptingOrders": True,
            "archived": False,
            "closed": False,
            "enableOrderBook": True,
            "restricted": False,
        }
        for field_name, required_value in operational_requirements.items():
            if market.get(field_name) is not required_value:
                ineligibility_reasons.append(f"POLYMARKET_OPERATIONAL_{field_name.upper()}_NOT_AUTHENTICATED")
        rule_version = MarketRuleVersion.create(
            venue=Venue.POLYMARKET,
            economic_market_id=market_id,
            rule_text=rule_text,
            resolution_source=resolution_source,
            opens_at=cast(str | None, market.get("startDate")),
            closes_at=cast(str | None, market.get("endDate")),
            resolves_at=cast(str | None, market.get("closedTime")),
            market_status="CLOSED" if market.get("closed") is True else "ACTIVE",
            outcomes=outcomes,
            source_metadata_version=source_metadata_version,
            raw_content_sha256=rule_content_sha256,
        )
        return cls(
            venue=Venue.POLYMARKET,
            series_id=(None if event.get("seriesSlug") is None else str(event["seriesSlug"])),
            event_id=event_id,
            market_id=market_id,
            outcomes=outcomes,
            rule_version=rule_version,
            negative_risk=neg_risk,
            multivariate=False,
            raw_graph_sha256=raw_graph_sha256,
            execution_admissible=not ineligibility_reasons,
            ineligibility_reasons=tuple(ineligibility_reasons),
            source_refs=tuple(
                sorted(
                    set(source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            ),
        )

    @classmethod
    def from_kalshi(
        cls,
        *,
        market: Mapping[str, object],
        event: Mapping[str, object],
        event_metadata: Mapping[str, object],
        series: Mapping[str, object],
        source_metadata_version: str,
        source_refs: Sequence[PredictionRawRecordRef] = (),
    ) -> PredictionIdentityGraph:
        market_id = _text(market.get("ticker"), label="Kalshi market ticker")
        event_id = _text(event.get("event_ticker"), label="Kalshi event ticker")
        if market.get("event_ticker") != event_id:
            raise ValueError("Kalshi event/market ticker relationship diverged")
        series_id = _text(event.get("series_ticker"), label="Kalshi series ticker")
        if _text(
            series.get("ticker") or series.get("series_ticker"),
            label="Kalshi series record ticker",
        ) != series_id:
            raise ValueError("Kalshi event/series ticker relationship diverged")
        if bool(event.get("is_multivariate")) or market.get("mve_collection_ticker"):
            raise ValueError("Kalshi multivariate event is unsupported fail-closed")
        market_type = str(market.get("market_type") or "binary").lower()
        if market_type not in {"binary", "binary_contract"}:
            raise ValueError("Kalshi non-binary market is unsupported fail-closed")
        metadata_event_ticker = event_metadata.get("event_ticker")
        if str(metadata_event_ticker or "") != event_id:
            raise ValueError("Kalshi event metadata identity diverged")
        settlement_sources = event_metadata.get("settlement_sources")
        if not isinstance(settlement_sources, list) or not settlement_sources:
            raise ValueError("Kalshi settlement sources are incomplete")
        market_details = event_metadata.get("market_details")
        if not isinstance(market_details, list):
            raise ValueError("Kalshi event metadata market details are incomplete")
        metadata_markets = tuple(
            _text(
                _mapping(item, label="Kalshi event metadata market detail").get(
                    "market_ticker"
                ),
                label="Kalshi metadata market ticker",
            )
            for item in market_details
        )
        if len(set(metadata_markets)) != len(metadata_markets) or market_id not in metadata_markets:
            raise ValueError("Kalshi event metadata does not bind the selected market")
        outcomes = (
            OutcomeIdentity(Venue.KALSHI, market_id, f"{market_id}:YES", "YES"),
            OutcomeIdentity(Venue.KALSHI, market_id, f"{market_id}:NO", "NO"),
        )
        graph_body = {
            "event": {
                "event_ticker": event_id,
                "rules_primary": event.get("rules_primary"),
                "rules_secondary": event.get("rules_secondary"),
                "series_ticker": series_id,
            },
            "event_metadata": {
                "event_ticker": event_metadata.get("event_ticker"),
                "market_tickers": list(metadata_markets),
                "settlement_sources": settlement_sources,
            },
            "market": {
                "close_time": market.get("close_time"),
                "event_ticker": market.get("event_ticker"),
                "expected_expiration_time": market.get("expected_expiration_time"),
                "latest_expiration_time": market.get("latest_expiration_time"),
                "market_type": market.get("market_type"),
                "open_time": market.get("open_time"),
                "price_ranges": market.get("price_ranges"),
                "result": market.get("result"),
                "rules_primary": market.get("rules_primary"),
                "rules_secondary": market.get("rules_secondary"),
                "settlement_ts": market.get("settlement_ts"),
                "settlement_value_dollars": market.get("settlement_value_dollars"),
                "status": market.get("status"),
                "ticker": market_id,
            },
            "outcomes": [item.to_dict() for item in outcomes],
            "series": {
                "category": series.get("category"),
                "frequency": series.get("frequency"),
                "ticker": series_id,
            },
            "source_refs": [
                item.to_dict()
                for item in sorted(
                    set(source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            ],
        }
        raw_graph_sha256 = hashlib.sha256(canonical_json_bytes(graph_body)).hexdigest()
        rule_observation = {key: value for key, value in graph_body.items() if key != "source_refs"}
        rule_content_sha256 = hashlib.sha256(
            canonical_json_bytes(rule_observation)
        ).hexdigest()
        primary = _text(
            market.get("rules_primary") or event.get("rules_primary"),
            label="Kalshi primary rules",
        )
        secondary = str(market.get("rules_secondary") or event.get("rules_secondary") or "")
        source_names = tuple(
            sorted(
                _text(
                    _mapping(item, label="Kalshi settlement source").get("name")
                    or _mapping(item, label="Kalshi settlement source").get("url"),
                    label="Kalshi settlement source identity",
                )
                for item in settlement_sources
            )
        )
        rule_version = MarketRuleVersion.create(
            venue=Venue.KALSHI,
            economic_market_id=market_id,
            rule_text=f"{primary}\n{secondary}".rstrip(),
            resolution_source=" | ".join(source_names),
            opens_at=cast(str | None, market.get("open_time")),
            closes_at=cast(str | None, market.get("close_time")),
            resolves_at=cast(str | None, market.get("settlement_ts")),
            market_status=str(market.get("status") or "UNKNOWN").upper(),
            outcomes=outcomes,
            source_metadata_version=source_metadata_version,
            raw_content_sha256=rule_content_sha256,
        )
        return cls(
            venue=Venue.KALSHI,
            series_id=series_id,
            event_id=event_id,
            market_id=market_id,
            outcomes=outcomes,
            rule_version=rule_version,
            negative_risk=False,
            multivariate=False,
            raw_graph_sha256=raw_graph_sha256,
            execution_admissible=str(market.get("status") or "").lower() == "active",
            ineligibility_reasons=(
                ()
                if str(market.get("status") or "").lower() == "active"
                else ("KALSHI_MARKET_ACTIVE_STATE_NOT_AUTHENTICATED",)
            ),
            source_refs=tuple(
                sorted(
                    set(source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            ),
        )

    def assert_compatible_successor(
        self,
        successor: PredictionIdentityGraph,
        *,
        explicit_rule_version_transition: bool,
    ) -> None:
        if (
            successor.venue is not self.venue
            or successor.series_id != self.series_id
            or successor.event_id != self.event_id
            or successor.market_id != self.market_id
            or successor.outcomes != self.outcomes
            or successor.negative_risk != self.negative_risk
            or successor.multivariate != self.multivariate
        ):
            raise ValueError("PREDICTION_SEMANTIC_IDENTITY_CHANGED_SILENTLY")
        if (
            successor.rule_version.version_id != self.rule_version.version_id
            and not explicit_rule_version_transition
        ):
            raise ValueError("PREDICTION_RULE_VERSION_CHANGED_WITHOUT_TRANSITION")


def build_prediction_semantic_catalog_from_graphs(
    graphs: Sequence[PredictionIdentityGraph],
    *,
    semantic_versions: Mapping[tuple[Venue, str, str], int] | None = None,
) -> SemanticCatalog:
    """Derive only within-market complete-set relations from authenticated graphs."""

    relations: list[SemanticRelation] = []
    observations: dict[
        tuple[Venue, str, str, str], PredictionIdentityGraph
    ] = {}
    for graph in graphs:
        observation_key = (
            graph.venue,
            graph.market_id,
            graph.rule_version.version_id,
            graph.raw_graph_sha256,
        )
        previous = observations.get(observation_key)
        if previous is not None and previous != graph:
            raise ValueError("prediction graph observation identity is ambiguous")
        observations[observation_key] = graph
    semantic_keys = {
        (graph.venue, graph.market_id, graph.rule_version.version_id)
        for graph in observations.values()
    }
    if semantic_versions is None:
        by_market: dict[tuple[Venue, str], list[tuple[Venue, str, str]]] = {}
        for semantic_key in semantic_keys:
            by_market.setdefault(semantic_key[:2], []).append(semantic_key)
        if any(len(items) != 1 for items in by_market.values()):
            raise ValueError("prediction semantic version ordering requires authenticated timeline")
        semantic_versions = {items[0]: 1 for items in by_market.values()}
    elif set(semantic_versions) != semantic_keys or any(
        type(version) is not int or version <= 0 for version in semantic_versions.values()
    ):
        raise ValueError("prediction semantic version mapping diverged from graph observations")

    ordered_graphs = [
        (semantic_versions[(graph.venue, graph.market_id, graph.rule_version.version_id)], graph)
        for graph in observations.values()
    ]
    for relation_version, graph in sorted(
        ordered_graphs,
        key=lambda item: (
            item[1].venue.value,
            item[1].market_id,
            item[0],
            item[1].raw_graph_sha256,
        ),
    ):
        if not graph.source_content_sha256s:
            raise ValueError("prediction semantic relations require raw graph provenance")
        status = (
            RelationStatus.VERIFIED
            if graph.execution_admissible
            and graph.rule_version.market_status.upper() == "ACTIVE"
            else RelationStatus.UNVERIFIED
        )
        confidence = Decimal("1") if status is RelationStatus.VERIFIED else Decimal("0")
        resolution_versions = {
            f"{item.economic_market_id}:{item.outcome_id}": graph.rule_version.version_id
            for item in graph.outcomes
        }
        legs = [
            {
                "market_id": item.economic_market_id,
                "outcome_id": item.outcome_id,
                "side": "YES",
            }
            for item in graph.outcomes
        ]
        machine = {
            "derivation": "AUTHENTICATED_SINGLE_MARKET_OUTCOME_GRAPH_V1",
            "economic_market_id": graph.market_id,
            "raw_graph_sha256": graph.raw_graph_sha256,
            "rule_version_id": graph.rule_version.version_id,
        }
        justification = (
            "Within-market outcome completeness is derived from the authenticated venue "
            "rule graph; no wording or cross-market equivalence is asserted."
        )
        relations.append(
            SemanticRelation.create(
                relation_type=RelationType.EXHAUSTIVE,
                members=graph.outcomes,
                formal_rule={
                    "guaranteed_payout": "1",
                    "guaranteed_payout_per_unit": "1",
                    "legs": legs,
                    "resolution_rule_versions": resolution_versions,
                    "resolution_unambiguous": graph.execution_admissible,
                    "scanner_contract": "BUY_COMPLETE_SET_V1",
                },
                provenance=graph.source_content_sha256s,
                version=relation_version,
                confidence=confidence,
                status=status,
                human_justification=justification,
                machine_justification=machine,
            )
        )
        relations.append(
            SemanticRelation.create(
                relation_type=RelationType.MUTUALLY_EXCLUSIVE,
                members=graph.outcomes,
                formal_rule={
                    "resolution_rule_versions": resolution_versions,
                    "resolution_unambiguous": graph.execution_admissible,
                    "simultaneous_winners_max": 1,
                },
                provenance=graph.source_content_sha256s,
                version=relation_version,
                confidence=confidence,
                status=status,
                human_justification=justification,
                machine_justification=machine,
            )
        )
    return SemanticCatalog.build(relations)


def build_prediction_graph_from_raw(
    index: PredictionRawEvidenceIndex,
    *,
    venue: Venue,
    market_ref: PredictionRawRecordRef,
    event_ref: PredictionRawRecordRef,
    event_metadata_ref: PredictionRawRecordRef | None = None,
    series_ref: PredictionRawRecordRef | None = None,
    clob_market_refs: Sequence[PredictionRawRecordRef] = (),
) -> PredictionIdentityGraph:
    market_envelope, market = index.require_record(
        market_ref,
        venue=venue,
        allowed_feeds=("historical_markets", "markets", "metadata"),
    )
    event_envelope, event = index.require_record(
        event_ref,
        venue=venue,
        allowed_feeds=("events", "metadata"),
    )
    if market_envelope.source_metadata_version != event_envelope.source_metadata_version:
        raise ValueError("prediction graph source metadata versions diverged")
    references = [market_ref, event_ref]
    if venue is Venue.POLYMARKET:
        if event_metadata_ref is not None or series_ref is not None:
            raise ValueError("Polymarket graph does not accept Kalshi event metadata")
        if not clob_market_refs:
            raise ValueError("Polymarket graph requires CLOB market references")
        market_id = _text(market.get("conditionId"), label="Polymarket condition id")
        token_outcomes = polymarket_gamma_token_outcomes(market)
        clob_records: list[Mapping[str, object]] = []
        for reference in clob_market_refs:
            clob_envelope, clob_record = index.require_record(
                reference,
                venue=venue,
                allowed_feeds=("metadata",),
            )
            if clob_envelope.source_metadata_version != market_envelope.source_metadata_version:
                raise ValueError("prediction graph CLOB metadata version diverged")
            references.append(reference)
            clob_records.append(
                normalize_polymarket_clob_v2_market(
                    clob_record,
                    provenance=clob_envelope.provenance,
                    expected_condition_id=market_id,
                    expected_token_outcomes=token_outcomes,
                )
            )
        return PredictionIdentityGraph.from_polymarket(
            market=market,
            event=event,
            clob_markets=clob_records,
            source_metadata_version=market_envelope.source_metadata_version,
            source_refs=references,
        )
    if clob_market_refs:
        raise ValueError("Kalshi graph does not accept Polymarket CLOB references")
    if venue is not Venue.KALSHI or event_metadata_ref is None or series_ref is None:
        raise ValueError("Kalshi graph requires authenticated event metadata and series")
    metadata_envelope, event_metadata = index.require_record(
        event_metadata_ref,
        venue=venue,
        allowed_feeds=("event_metadata",),
    )
    if metadata_envelope.source_metadata_version != market_envelope.source_metadata_version:
        raise ValueError("prediction graph event metadata version diverged")
    references.append(event_metadata_ref)
    event_metadata = normalize_kalshi_event_metadata(
        event_metadata,
        provenance=metadata_envelope.provenance,
        expected_event_ticker=_text(
            event.get("event_ticker"), label="Kalshi event ticker"
        ),
        expected_market_ticker=_text(
            market.get("ticker"), label="Kalshi market ticker"
        ),
    )
    series_envelope, series = index.require_record(
        series_ref,
        venue=venue,
        allowed_feeds=("series",),
    )
    if series_envelope.source_metadata_version != market_envelope.source_metadata_version:
        raise ValueError("prediction graph series metadata version diverged")
    references.append(series_ref)
    return PredictionIdentityGraph.from_kalshi(
        market=market,
        event=event,
        event_metadata=event_metadata,
        series=series,
        source_metadata_version=market_envelope.source_metadata_version,
        source_refs=references,
    )


def revalidate_prediction_graph(
    index: PredictionRawEvidenceIndex,
    graph: PredictionIdentityGraph,
) -> None:
    if not graph.source_refs:
        raise ValueError("prediction graph lacks authenticated raw record references")
    resolved = [
        index.require_record(
            reference,
            venue=graph.venue,
            allowed_feeds=(
                "event_metadata",
                "events",
                "historical_markets",
                "markets",
                "metadata",
                "series",
            ),
        )
        for reference in graph.source_refs
    ]
    if graph.venue is Venue.POLYMARKET:
        market_matches = [
            (reference, record)
            for reference, (_envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if str(record.get("conditionId") or "") == graph.market_id
            and record.get("clobTokenIds") is not None
        ]
        event_matches = [
            (reference, record)
            for reference, (_envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if str(record.get("id") or "") == graph.event_id and isinstance(record.get("markets"), list)
        ]
        clob_matches = [
            (reference, record)
            for reference, (envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if envelope.feed_type == "metadata"
            and record.get("t") is not None
            and record.get("clobTokenIds") is None
            and (
                urlsplit(envelope.provenance.source_url).path
                in {
                    f"/clob-markets/{graph.market_id}",
                    f"/polymarket/clob-markets/{graph.market_id}",
                }
            )
        ]
        if len(market_matches) != 1 or len(event_matches) != 1 or not clob_matches:
            raise ValueError("Polymarket graph raw roles are ambiguous")
        rebuilt = build_prediction_graph_from_raw(
            index,
            venue=graph.venue,
            market_ref=market_matches[0][0],
            event_ref=event_matches[0][0],
            clob_market_refs=tuple(item[0] for item in clob_matches),
        )
    else:
        market_matches = [
            (reference, record)
            for reference, (_envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if str(record.get("ticker") or "") == graph.market_id
        ]
        event_matches = [
            (reference, record)
            for reference, (_envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if str(record.get("event_ticker") or "") == graph.event_id
            and record.get("series_ticker") is not None
        ]
        metadata_matches = [
            (reference, record)
            for reference, (envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if envelope.feed_type == "event_metadata"
            and isinstance(record.get("settlement_sources"), list)
            and isinstance(record.get("market_details"), list)
        ]
        series_matches = [
            (reference, record)
            for reference, (_envelope, record) in zip(graph.source_refs, resolved, strict=True)
            if str(record.get("ticker") or record.get("series_ticker") or "")
            == graph.series_id
            and record.get("event_ticker") is None
        ]
        if (
            len(market_matches) != 1
            or len(event_matches) != 1
            or len(metadata_matches) != 1
            or len(series_matches) != 1
        ):
            raise ValueError("Kalshi graph raw roles are ambiguous")
        rebuilt = build_prediction_graph_from_raw(
            index,
            venue=graph.venue,
            market_ref=market_matches[0][0],
            event_ref=event_matches[0][0],
            event_metadata_ref=metadata_matches[0][0],
            series_ref=series_matches[0][0],
        )
    if rebuilt != graph:
        raise ValueError("prediction graph diverged from authenticated raw records")


__all__ = [
    "BOUNDARY",
    "BoundedCursorPager",
    "EvidenceClassification",
    "OfficialPublicContract",
    "PredictionIdentityGraph",
    "PublicEndpointContract",
    "build_prediction_graph_from_raw",
    "build_prediction_semantic_catalog_from_graphs",
    "revalidate_prediction_graph",
]
