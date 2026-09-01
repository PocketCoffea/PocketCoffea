"""Offline unit tests for the artificial per-category normalization variation."""
import hist
import numpy as np
import pytest

from pocket_coffea.utils.stat.combine import Datacard
from pocket_coffea.utils.stat.processes import MCProcess, MCProcesses
from pocket_coffea.utils.stat.shape_manipulation import (
    add_binwise_variation,
    add_norm_variation,
)
from pocket_coffea.utils.stat.systematics import Systematics, SystematicUncertainty


def _make_hist(categories, variations, values_by_cat):
    """Build a (cat, variation, x) Weight histogram; every variation gets the
    same per-category values, variance = |value|."""
    nbins = len(next(iter(values_by_cat.values())))
    histogram = hist.Hist(
        hist.axis.StrCategory(categories, name="cat", label="Category"),
        hist.axis.StrCategory(variations, name="variation", label="Variation"),
        hist.axis.Regular(nbins, 0, nbins, name="x"),
        storage=hist.storage.Weight(),
    )
    view = histogram.view()
    for cat, values in values_by_cat.items():
        cat_i = histogram.axes["cat"].index(cat)
        for variation_i in range(len(variations)):
            view["value"][cat_i, variation_i, :] = values
            view["variance"][cat_i, variation_i, :] = np.abs(values)
    return histogram


def test_add_norm_variation_scales_nominal_per_category():
    histogram = _make_hist(
        ["CR", "SR"],
        ["nominal", "existingUp"],
        {"CR": [1.0, 2.0], "SR": [3.0, 4.0]},
    )
    histograms = {
        "ttbb_dilep": {"dset": histogram},
        "other": {"dset": histogram.copy()},
    }
    out = add_norm_variation(
        histograms,
        variation_name="fs45",
        samples=["ttbb_dilep"],
        scale_by_category={"SR": 1.2},
    )

    new = out["ttbb_dilep"]["dset"]
    assert list(new.axes["variation"]) == ["nominal", "existingUp", "fs45Up", "fs45Down"]

    # existing variations copied untouched
    for variation in ("nominal", "existingUp"):
        for cat in ("CR", "SR"):
            assert np.array_equal(
                new[cat, variation, :].view(), histogram[cat, variation, :].view()
            )

    # Up: scaled in SR, default factor 1.0 in CR; variance scales quadratically
    assert np.allclose(new["SR", "fs45Up", :].values(), np.array([3.0, 4.0]) * 1.2)
    assert np.allclose(
        new["SR", "fs45Up", :].variances(), np.array([3.0, 4.0]) * 1.2**2
    )
    assert np.array_equal(new["CR", "fs45Up", :].view(), histogram["CR", "nominal", :].view())

    # Down: one-sided, identical to nominal
    for cat in ("CR", "SR"):
        assert np.array_equal(
            new[cat, "fs45Down", :].view(), histogram[cat, "nominal", :].view()
        )

    # input not mutated; untouched samples shared by reference
    assert list(histogram.axes["variation"]) == ["nominal", "existingUp"]
    assert out["other"]["dset"] is histograms["other"]["dset"]


def test_add_norm_variation_mirror_down():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [2.0, 4.0]})
    out = add_norm_variation(
        {"s": {"d": histogram}},
        variation_name="fs45",
        samples=["s"],
        scale_by_category={"SR": 2.0},
        down_mode="mirror",
    )
    new = out["s"]["d"]
    assert np.allclose(new["SR", "fs45Up", :].values(), [4.0, 8.0])
    assert np.allclose(new["SR", "fs45Down", :].values(), [1.0, 2.0])
    assert np.allclose(new["SR", "fs45Down", :].variances(), [0.5, 1.0])


def test_add_norm_variation_validations():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [1.0]})
    histograms = {"s": {"d": histogram}}
    with pytest.raises(ValueError, match="not found in histograms"):
        add_norm_variation(histograms, "v", ["typo"], {"SR": 1.1})
    with pytest.raises(ValueError, match="not found on the 'cat' axis"):
        add_norm_variation(histograms, "v", ["s"], {"typo_cat": 1.1})
    with pytest.raises(ValueError, match="down_mode"):
        add_norm_variation(histograms, "v", ["s"], {"SR": 1.1}, down_mode="typo")
    already = _make_hist(["SR"], ["nominal", "vUp"], {"SR": [1.0]})
    with pytest.raises(ValueError, match="already present"):
        add_norm_variation({"s": {"d": already}}, "v", ["s"], {"SR": 1.1})


def test_add_binwise_variation_per_bin_weights():
    histogram = _make_hist(
        ["CR", "SR"],
        ["nominal"],
        {"CR": [1.0, 2.0], "SR": [3.0, 4.0]},
    )
    # put content in the flow bins of SR to check they keep weight 1
    view = histogram.view(flow=True)
    sr = histogram.axes["cat"].index("SR")
    view["value"][sr, 0, 0] = 7.0  # underflow
    view["value"][sr, 0, -1] = 9.0  # overflow

    out = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": [1.5, 0.5]},
    )
    new = out["s"]["d"]
    # in-range bins reweighted, variances with the squared weight
    assert np.allclose(new["SR", "rwUp", :].values(), [3.0 * 1.5, 4.0 * 0.5])
    assert np.allclose(new["SR", "rwUp", :].variances(), [3.0 * 1.5**2, 4.0 * 0.5**2])
    # flow bins untouched by an in-range weight array
    up_flow = new["SR", "rwUp", :].view(flow=True)
    assert up_flow["value"][0] == pytest.approx(7.0)
    assert up_flow["value"][-1] == pytest.approx(9.0)
    # unlisted category gets default weight 1.0; Down = nominal everywhere
    assert np.array_equal(new["CR", "rwUp", :].view(), histogram["CR", "nominal", :].view())
    for cat in ("CR", "SR"):
        assert np.array_equal(
            new[cat, "rwDown", :].view(flow=True),
            histogram[cat, "nominal", :].view(flow=True),
        )


def test_add_binwise_variation_extent_weights_scale_flow():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [2.0, 4.0]})
    view = histogram.view(flow=True)
    view["value"][0, 0, 0] = 6.0  # underflow
    view["value"][0, 0, -1] = 8.0  # overflow
    # extent = underflow + 2 bins + overflow
    out = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": [2.0, 1.5, 0.5, 3.0]},
    )
    up_flow = out["s"]["d"]["SR", "rwUp", :].view(flow=True)
    assert np.allclose(up_flow["value"], [12.0, 3.0, 2.0, 24.0])


def test_add_binwise_variation_scalar_matches_norm_variation():
    histogram = _make_hist(
        ["CR", "SR"], ["nominal", "otherUp"], {"CR": [1.0, 2.0], "SR": [3.0, 4.0]}
    )
    histograms = {"s": {"d": histogram}}
    out_norm = add_norm_variation(
        histograms, "v", ["s"], scale_by_category={"SR": 1.2}
    )
    out_binwise = add_binwise_variation(
        histograms, "v", ["s"], weights_by_category={"SR": 1.2}
    )
    assert np.array_equal(
        out_norm["s"]["d"].view(flow=True), out_binwise["s"]["d"].view(flow=True)
    )


def test_add_binwise_variation_mirror_down():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [2.0, 4.0]})
    out = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": [2.0, 0.5]},
        down_mode="mirror",
    )
    new = out["s"]["d"]
    assert np.allclose(new["SR", "rwUp", :].values(), [4.0, 2.0])
    assert np.allclose(new["SR", "rwDown", :].values(), [1.0, 8.0])
    assert np.allclose(new["SR", "rwDown", :].variances(), [0.5, 16.0])


def test_add_binwise_variation_preserve_norm():
    histogram = _make_hist(
        ["CR", "SR"], ["nominal"], {"CR": [1.0, 2.0], "SR": [3.0, 4.0]}
    )
    # flow content participates in the renormalization
    view = histogram.view(flow=True)
    sr = histogram.axes["cat"].index("SR")
    view["value"][sr, 0, 0] = 1.0  # underflow
    out = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": [2.0, 0.5]},
        preserve_norm=True,
    )
    new = out["s"]["d"]
    up = new["SR", "rwUp", :].view(flow=True)["value"]
    nominal = histogram["SR", "nominal", :].view(flow=True)["value"]
    # yield preserved exactly (flow included): pure shape variation
    assert up.sum() == pytest.approx(nominal.sum())
    # bins follow the weights up to one common factor:
    # raw varied = [1*1, 3*2, 4*0.5, 0] = [1, 6, 2, 0], total 9 vs nominal 8
    r = 8.0 / 9.0
    assert np.allclose(up, np.array([1.0, 6.0, 2.0, 0.0]) * r)
    nominal_variance = histogram["SR", "nominal", :].view(flow=True)["variance"]
    assert np.allclose(
        new["SR", "rwUp", :].view(flow=True)["variance"],
        nominal_variance * np.array([1.0, 2.0, 0.5, 1.0]) ** 2 * r**2,
    )
    # unlisted category: weights 1 everywhere -> renormalization is a no-op
    assert np.array_equal(
        new["CR", "rwUp", :].view(), histogram["CR", "nominal", :].view()
    )


def test_add_binwise_variation_preserve_norm_mirror_and_scalar():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [2.0, 4.0]})
    out = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": [2.0, 0.5]},
        down_mode="mirror",
        preserve_norm=True,
    )
    new = out["s"]["d"]
    assert new["SR", "rwUp", :].values().sum() == pytest.approx(6.0)
    assert new["SR", "rwDown", :].values().sum() == pytest.approx(6.0)
    # mirror weights 1/w renormalized independently: raw down [1, 8] -> r = 6/9
    assert np.allclose(new["SR", "rwDown", :].values(), np.array([1.0, 8.0]) * 6.0 / 9.0)

    # a scalar weight under preserve_norm is a no-op by construction
    out_scalar = add_binwise_variation(
        {"s": {"d": histogram}},
        variation_name="rw",
        samples=["s"],
        weights_by_category={"SR": 1.3},
        preserve_norm=True,
    )
    assert np.array_equal(
        out_scalar["s"]["d"]["SR", "rwUp", :].view(flow=True),
        histogram["SR", "nominal", :].view(flow=True),
    )


def test_add_binwise_variation_per_year_datasets():
    h_2024 = _make_hist(["SR"], ["nominal"], {"SR": [2.0, 4.0]})
    h_2022 = _make_hist(["SR"], ["nominal"], {"SR": [1.0, 3.0]})
    histograms = {"s": {"ds_2024": h_2024, "ds_2022": h_2022}}

    # first call: 2024 only; 2022 carried over untouched (shared reference)
    out = add_binwise_variation(
        histograms, "rw", ["s"], {"SR": [1.5, 0.5]}, datasets=["ds_2024"]
    )
    assert out["s"]["ds_2022"] is h_2022
    assert np.allclose(out["s"]["ds_2024"]["SR", "rwUp", :].values(), [3.0, 2.0])

    # second call, same variation name, different weights for 2022
    # (missing/empty datasets in the list are tolerated)
    out2 = add_binwise_variation(
        out, "rw", ["s"], {"SR": [2.0, 1.0]}, datasets=["ds_2022", "ds_empty"]
    )
    assert np.allclose(out2["s"]["ds_2022"]["SR", "rwUp", :].values(), [2.0, 3.0])
    assert out2["s"]["ds_2024"] is out["s"]["ds_2024"]

    # overlapping call on an already-covered dataset raises
    with pytest.raises(ValueError, match="already present"):
        add_binwise_variation(out2, "rw", ["s"], {"SR": 1.1}, datasets=["ds_2024"])

    # a filter matching nothing raises (typo protection)
    with pytest.raises(ValueError, match="None of the requested datasets"):
        add_binwise_variation(histograms, "rw2", ["s"], {"SR": 1.1}, datasets=["typo"])

    # wrapper passes the filter through
    out_norm = add_norm_variation(
        histograms, "vn", ["s"], scale_by_category={"SR": 1.2}, datasets=["ds_2024"]
    )
    assert out_norm["s"]["ds_2022"] is h_2022
    assert np.allclose(out_norm["s"]["ds_2024"]["SR", "vnUp", :].values(), [2.4, 4.8])


def test_add_binwise_variation_validations():
    histogram = _make_hist(["SR"], ["nominal"], {"SR": [1.0, 2.0]})
    histograms = {"s": {"d": histogram}}
    with pytest.raises(ValueError, match="length 3, expected"):
        add_binwise_variation(histograms, "v", ["s"], {"SR": [1.0, 1.0, 1.0]})
    with pytest.raises(ValueError, match="scalar or a 1D array"):
        add_binwise_variation(histograms, "v", ["s"], {"SR": [[1.0], [1.0]]})
    with pytest.raises(ValueError, match="positive weights"):
        add_binwise_variation(
            histograms, "v", ["s"], {"SR": [1.0, 0.0]}, down_mode="mirror"
        )
    with pytest.raises(ValueError, match="must be a scalar"):
        add_norm_variation(histograms, "v", ["s"], scale_by_category={"SR": [1.0, 1.0]})


def _two_sample_datacard(scale_by_category, category, shape_only_for_rateparam=False):
    """One ttbb process made of a dilep sample (gets the variation) and a semilep
    sample (falls back to nominal), two categories CR/SR."""
    year = "2018"
    dilep = _make_hist(["CR", "SR"], ["nominal"], {"CR": [10.0], "SR": [20.0]})
    semilep = _make_hist(["CR", "SR"], ["nominal"], {"CR": [100.0], "SR": [200.0]})
    histograms = {"dilep": {"dilep_dset": dilep}, "semilep": {"semilep_dset": semilep}}
    histograms = add_norm_variation(
        histograms,
        variation_name="fs45",
        samples=["dilep"],
        scale_by_category=scale_by_category,
    )
    datacard = Datacard(
        histograms=histograms,
        datasets_metadata={
            "by_datataking_period": {
                year: {"dilep": ["dilep_dset"], "semilep": ["semilep_dset"]}
            }
        },
        cutflow={
            "presel": {
                "dilep_dset": {"nominal": 100},
                "semilep_dset": {"nominal": 100},
            }
        },
        years=[year],
        mc_processes=MCProcesses(
            [
                MCProcess(
                    name="ttbb",
                    samples=["dilep", "semilep"],
                    is_signal=True,
                    years=[year],
                    has_rateParam=True,
                )
            ]
        ),
        systematics=Systematics(
            [
                SystematicUncertainty(
                    name="fs45",
                    typ="shape",
                    processes=["ttbb"],
                    years=[year],
                    value=1.0,
                )
            ]
        ),
        category=category,
        verbose=False,
        shape_only_for_rateparam=shape_only_for_rateparam,
        rateparam_norm_categories=["CR", "SR"] if shape_only_for_rateparam else None,
    )
    if shape_only_for_rateparam:
        datacard.rateparam_shape_scale = datacard.compute_rateparam_shape_scales()
    return datacard


def test_one_sided_variation_through_datacard():
    dc = _two_sample_datacard({"SR": 1.2}, category="SR")
    templates = dc.create_shape_histogram_dict(is_data=False)
    # Up = scaled dilep + nominal semilep (semilep falls back to nominal)
    assert templates["ttbb_2018_fs45Up"].values().sum() == pytest.approx(
        20.0 * 1.2 + 200.0
    )
    # one-sided: Down template identical to nominal
    assert templates["ttbb_2018_fs45Down"].values().sum() == pytest.approx(220.0)
    assert dc.rate("ttbb_2018") == pytest.approx(220.0)
    # CR untouched (factor defaults to 1.0)
    dc_cr = _two_sample_datacard({"SR": 1.2}, category="CR")
    templates_cr = dc_cr.create_shape_histogram_dict(is_data=False)
    assert templates_cr["ttbb_2018_fs45Up"].values().sum() == pytest.approx(110.0)


def test_interaction_with_shape_only_for_rateparam():
    # Overall normalization is stripped: only the relative CR/SR difference stays.
    dc = _two_sample_datacard({"SR": 1.2}, category="SR", shape_only_for_rateparam=True)
    templates = dc.create_shape_histogram_dict(is_data=False)
    total_nominal = 330.0  # (10+100) + (20+200)
    total_up = 110.0 + 224.0
    expected = 224.0 * total_nominal / total_up
    assert templates["ttbb_2018_fs45Up"].values().sum() == pytest.approx(expected)
    assert templates["ttbb_2018_fs45Up"].values().sum() != pytest.approx(224.0)

    # Equal factors in every category are fully absorbed by the rateParam: no-op.
    dc_flat = _two_sample_datacard(
        {"CR": 1.2, "SR": 1.2}, category="SR", shape_only_for_rateparam=True
    )
    templates_flat = dc_flat.create_shape_histogram_dict(is_data=False)
    assert templates_flat["ttbb_2018_fs45Up"].values().sum() == pytest.approx(220.0)
