from __future__ import annotations

import pandas as pd
import pytest

from hyperlab.data.synthetic import generate_demo_panel
from hyperlab.strategies.registry import STRATEGY_FACTORIES, create_strategy


@pytest.mark.parametrize("strategy_name", tuple(STRATEGY_FACTORIES))
def test_panel_strategy_prefix_is_invariant_to_future_observations(strategy_name: str) -> None:
    panel = generate_demo_panel(hours=800, seed=123)
    cutoff = 650
    baseline = create_strategy(strategy_name).generate(panel).weights.iloc[:cutoff]

    perturbed = generate_demo_panel(hours=800, seed=123)
    future = perturbed.prices.index[cutoff:]
    perturbed.prices.loc[future] *= 10.0
    perturbed.funding.loc[future] += 0.25
    perturbed.spreads_bps.loc[future] *= 100.0
    perturbed.volume_usd.loc[future] *= 0.01
    changed = create_strategy(strategy_name).generate(perturbed).weights.iloc[:cutoff]

    pd.testing.assert_frame_equal(changed, baseline)
