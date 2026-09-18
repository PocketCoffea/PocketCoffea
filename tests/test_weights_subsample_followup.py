"""Offline tests for the subsample-weight follow-up (no EOS needed)."""
import importlib

import numpy as np

from pocket_coffea.lib.columns_manager import ColumnsManager


class _FakeCats:
    def get_mask(self, category):
        return np.array([True, False, True])


class _FakeWM:
    """get_weight folds a x2 factor when the subsample is 'samp__A'."""
    _isMC = False

    def get_weight(self, category, subsample=None, modifier=None):
        base = np.array([3.0, 3.0, 3.0])
        return base * 2.0 if subsample == "samp__A" else base


def _fill(subsample):
    cm = ColumnsManager(cfg={"baseline": []}, categories_config=None, variations_config=None)
    return cm.fill_columns_accumulators(
        events=None,
        cuts_masks=_FakeCats(),
        variation="nominal",
        subsample_mask=None,
        weights_manager=_FakeWM(),
        subsample=subsample,
    )


def test_columns_weight_folds_subsample_factor():
    out = _fill("samp__A")
    # (3*2) masked by [T, F, T] -> [6, 6]
    assert list(out["baseline"]["nominal"]["weight"].value) == [6.0, 6.0]


def test_columns_weight_without_subsample_unchanged():
    out = _fill(None)
    assert list(out["baseline"]["nominal"]["weight"].value) == [3.0, 3.0]


def test_weights_run3_is_importable():
    # Previously raised ValueError (duplicate sf_ele_trigger registration) on import.
    m = importlib.import_module("pocket_coffea.lib.weights.common.weights_run3")
    assert hasattr(m, "SF_ele_trigger")
    # It is the same registered weight as the common one.
    from pocket_coffea.lib.weights.common.common import SF_ele_trigger as common_one
    assert m.SF_ele_trigger is common_one
