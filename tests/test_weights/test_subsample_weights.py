import numpy as np
import awkward as ak
import pytest
from pocket_coffea.lib.weights import WeightLambda
from pocket_coffea.lib.weights.weights_manager import WeightsManager
from pocket_coffea.lib.columns_manager import ColumnsManager

N = 3
# sample weight wA: nominal 2, up 3 ; subsample-only weight wS: nominal 5, up 7
wA = WeightLambda.wrap_func("subtest_wA", lambda p, m, e, s, v: [np.full(s, 2.), np.full(s, 3.), np.full(s, 1.)],
                            has_variations=True)
wS = WeightLambda.wrap_func("subtest_wS", lambda p, m, e, s, v: [np.full(s, 5.), np.full(s, 7.), np.full(s, 4.)],
                            has_variations=True)


def make_manager(sub_split_bycat):
    if sub_split_bycat:
        sub = {"inclusive": [], "bycategory": {"c1": ["subtest_wS"]}, "is_split_bycat": True}
    else:
        sub = {"inclusive": ["subtest_wS"], "bycategory": {}, "is_split_bycat": False}
    conf = {"inclusive": ["subtest_wA"], "bycategory": {}, "is_split_bycat": False,
            "by_subsample": {"smp__sub": sub}}
    wm = WeightsManager({}, conf, [wA, wS], {"sample": "smp", "dataset": "d", "year": "y", "isMC": True})
    wm.compute(ak.Array(np.zeros(N)), N)
    return wm


@pytest.mark.parametrize("sub_split_bycat", [False, True])
def test_get_weight_subsample(sub_split_bycat):
    wm = make_manager(sub_split_bycat)
    assert np.allclose(wm.get_weight("c1", subsample="smp__sub"), 10)
    # sample-level variation keeps the subsample nominal factor
    assert np.allclose(wm.get_weight("c1", subsample="smp__sub", modifier="subtest_wAUp"), 15)
    # subsample-only variation keeps the sample nominal factor
    assert np.allclose(wm.get_weight("c1", subsample="smp__sub", modifier="subtest_wSUp"), 14)
    # must match the HistManager composition
    assert np.allclose(wm.get_weight("c1", modifier="subtest_wAUp")
                       * wm.get_weight_only_subsample("smp__sub", "c1", "subtest_wAUp"), 15)
    with pytest.raises(ValueError):
        wm.get_weight("c1", subsample="smp__sub", modifier="nonexistentUp")


def test_columns_export_subsample_variations():
    wm = make_manager(False)
    variations_config = {"weights": {"c1": ["subtest_wA"]},
                         "by_subsample": {"smp__sub": {"weights": {"c1": ["subtest_wS"]}}}}
    cm = ColumnsManager({}, None, variations_config)
    out = cm.get_weights(wm, "c1", "nominal", subsample="smp__sub")
    expected = {"weight": 10, "weight_variation_subtest_wAUp": 15, "weight_variation_subtest_wADown": 5,
                "weight_variation_subtest_wSUp": 14, "weight_variation_subtest_wSDown": 8}
    assert set(out) == set(expected)
    for k, v in expected.items():
        assert np.allclose(out[k], v), k
    # shape-variation pass: nominal only
    assert set(cm.get_weights(wm, "c1", "JES_Up", subsample="smp__sub")) == {"weight"}
