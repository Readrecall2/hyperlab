from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    PublicDataEnvelope,
)


class GhostEnvelopeAdapter(Protocol):
    """Pure adapter from authenticated Research envelopes to one Ghost fixture."""

    adapter_id: str

    def fixture_bytes(self, envelopes: Sequence[PublicDataEnvelope]) -> bytes: ...


@dataclass(frozen=True, slots=True)
class CanonicalGhostFixtureEnvelopeAdapter:
    adapter_id: str = "canonical-ghost-fixture-envelope-v1"

    def fixture_bytes(self, envelopes: Sequence[PublicDataEnvelope]) -> bytes:
        if len(envelopes) != 1:
            raise ValueError("ghost fixture manifest must authenticate exactly one envelope")
        envelope = envelopes[0]
        if (
            envelope.feed_type != "ghost_fixture"
            or envelope.provenance.fixture_label != SYNTHETIC_FIXTURE_LABEL
            or envelope.provenance.transport != "FIXTURE"
        ):
            raise ValueError(
                "manifest envelope is not an explicitly synthetic ghost fixture"
            )
        return envelope.raw_payload


__all__ = ["CanonicalGhostFixtureEnvelopeAdapter", "GhostEnvelopeAdapter"]
