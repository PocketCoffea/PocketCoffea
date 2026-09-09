"""Offline unit tests for datacard rate formatting and duplicate-name detection."""
import hist
import numpy as np
import pytest

from pocket_coffea.utils.stat.combine import (
    Datacard,
    _clip_negative,
    _clip_negative_bins,
    format_rate,
)
from pocket_coffea.utils.stat.systematics import (
    Systematics,
    SystematicUncertainty,
)
from pocket_coffea.utils.stat.processes import MCProcesses, MCProcess


def test_format_rate_preserves_exponent():
    # The old `f"{rate}"[:10]` produced "3.45678901" (exponent dropped, ~1e5 too large).
    small = 3.4567890123e-05
    out = format_rate(small)
    assert "e-05" in out
    assert out != "3.45678901"
    assert float(out) == pytest.approx(small, rel=1e-4)
    # Ordinary magnitudes stay compact and correct.
    assert float(format_rate(1234.5678)) == pytest.approx(1234.5678, rel=1e-4)
    assert float(format_rate(0.0)) == 0.0


def test_duplicate_systematic_datacard_name_raises():
    s1 = SystematicUncertainty(
        name="a", typ="lnN", processes=["p"], years=["2018"], value=1.02, datacard_name="dup"
    )
    s2 = SystematicUncertainty(
        name="b", typ="lnN", processes=["p"], years=["2018"], value=1.03, datacard_name="dup"
    )
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Systematics([s1, s2])
    # Distinct names build fine.
    s3 = SystematicUncertainty(
        name="c", typ="lnN", processes=["p"], years=["2018"], value=1.01, datacard_name="other"
    )
    assert set(Systematics([s1, s3]).keys()) == {"dup", "other"}


def test_duplicate_mc_process_name_raises():
    p1 = MCProcess(name="ttbb", samples=["s1"], is_signal=False, years=["2018"])
    p2 = MCProcess(name="ttbb", samples=["s2"], is_signal=True, years=["2018"])
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        MCProcesses([p1, p2])
    p3 = MCProcess(name="ttcc", samples=["s3"], is_signal=False, years=["2018"])
    assert set(MCProcesses([p1, p3]).keys()) == {"ttbb", "ttcc"}


def _single_process_datacard(nominal_values):
    """Build a minimal one-process, one-category Datacard from bin values."""
    year, sample, dataset, category = "2018", "sig_sample", "sig_dataset", "cat"
    values = np.asarray(nominal_values, dtype=float)
    histogram = hist.Hist(
        hist.axis.StrCategory([category], name="cat"),
        hist.axis.StrCategory(["nominal"], name="variation"),
        hist.axis.Regular(len(values), 0, len(values), name="x"),
        storage=hist.storage.Weight(),
    )
    view = histogram.view()
    view["value"][0, 0, :] = values
    view["variance"][0, 0, :] = np.abs(values)
    return Datacard(
        histograms={sample: {dataset: histogram}},
        datasets_metadata={"by_datataking_period": {year: {sample: [dataset]}}},
        cutflow={"presel": {dataset: {"nominal": 100}}},
        years=[year],
        mc_processes=MCProcesses(
            [MCProcess(name="sig", samples=[sample], is_signal=True, years=[year])]
        ),
        systematics=Systematics([]),
        category=category,
        verbose=False,
    )


def test_clip_negative_zeroes_only_negatives():
    out = _clip_negative(np.array([1.0, -2.0, 0.0, 3.5, -0.1]))
    assert np.array_equal(out, np.array([1.0, 0.0, 0.0, 3.5, 0.0]))


def test_rate_matches_clipped_template_integral():
    # Combine reads the clipped template (negatives -> 0); the datacard rate must
    # match its integral, not the raw sum that still counts the negative bins.
    dc = _single_process_datacard([10.0, -3.0, 5.0, 2.0])

    raw_sum = dc.histogram["sig_2018", "nominal", :].sum()["value"]
    assert raw_sum == pytest.approx(14.0)  # includes the -3 bin

    # rate reflects the negative bin clipped to zero: 10 + 0 + 5 + 2
    assert dc.rate("sig_2018") == pytest.approx(17.0)

    # and it equals the integral of the template actually written to ROOT
    templates = dc.create_shape_histogram_dict(is_data=False)
    with pytest.warns(UserWarning, match="negative content"):
        written = _clip_negative_bins(templates["sig_2018_nominal"], "sig_2018_nominal")
    assert dc.rate("sig_2018") == pytest.approx(written.values().sum())


def test_rate_unchanged_without_negative_bins():
    dc = _single_process_datacard([10.0, 3.0, 5.0, 2.0])
    assert dc.rate("sig_2018") == pytest.approx(20.0)


def _norm_variation_histogram(categories=("CR", "SR")):
    """Two categories with disjoint x ranges and an artificial norm variation
    (+20% in SR only): CR content sits at low x, so a card rebinned to [0.8, 1.0]
    only keeps it in the underflow. ``categories`` restricts the category axis
    (like ``HistConf(only_categories=...)``)."""
    histogram = hist.Hist(
        hist.axis.StrCategory(list(categories), name="cat"),
        hist.axis.StrCategory(["nominal", "normUp", "normDown"], name="variation"),
        hist.axis.Regular(10, 0, 1, name="x"),
        storage=hist.storage.Weight(),
    )
    view = histogram.view()
    for variation_i, scale_sr in ((0, 1.0), (1, 1.2), (2, 1.0)):
        if "CR" in categories:
            cr = histogram.axes["cat"].index("CR")
            view["value"][cr, variation_i, 0] = 10.0  # x ~ 0.05, out of [0.8, 1.0]
            view["variance"][cr, variation_i, 0] = 10.0
        if "SR" in categories:
            sr = histogram.axes["cat"].index("SR")
            view["value"][sr, variation_i, 9] = 20.0 * scale_sr  # x ~ 0.95
            view["variance"][sr, variation_i, 9] = 20.0 * scale_sr
    return histogram


def _norm_variation_datacard(category, bins_edges=None, histogram=None, typ="shape", **kwargs):
    """One rateParam process on the ``_norm_variation_histogram`` input."""
    year, sample, dataset = "2018", "ttbb_sample", "ttbb_dataset"
    if histogram is None:
        histogram = _norm_variation_histogram()
    if "rateparam_norm_histograms" in kwargs:
        kwargs["rateparam_norm_histograms"] = {
            sample: {dataset: kwargs["rateparam_norm_histograms"]}
        }
    return Datacard(
        histograms={sample: {dataset: histogram}},
        datasets_metadata={"by_datataking_period": {year: {sample: [dataset]}}},
        cutflow={"presel": {dataset: {"nominal": 100}}},
        years=[year],
        mc_processes=MCProcesses(
            [
                MCProcess(
                    name="ttbb",
                    samples=[sample],
                    is_signal=True,
                    years=[year],
                    has_rateParam=True,
                )
            ]
        ),
        systematics=Systematics(
            [
                SystematicUncertainty(
                    name="norm",
                    typ=typ,
                    processes=["ttbb"],
                    years=[year],
                    value=1.0,
                )
            ]
        ),
        category=category,
        bins_edges=bins_edges,
        verbose=False,
        shape_only_for_rateparam=True,
        rateparam_norm_categories=["CR", "SR"],
        **kwargs,
    )


def test_rateparam_scale_consistent_across_rebinned_cards():
    # Regression: the shape-only factor must be computed from category-inclusive
    # totals. The SR card rebins to [0.8, 1.0], pushing all CR content into the
    # underflow; rearrange_histograms must keep that flow content so the SR card
    # derives the same Sum(nominal)/Sum(varied) as the unrebinned CR card, not an
    # SR-only factor (the old behavior: 30/36 -> 20/24).
    dc_sr = _norm_variation_datacard("SR", bins_edges=[0.8, 0.9, 1.0])
    dc_cr = _norm_variation_datacard("CR")
    expected = 30.0 / 34.0  # (10 + 20) / (10 + 1.2 * 20)

    scales_sr = dc_sr.compute_rateparam_shape_scales()
    scales_cr = dc_cr.compute_rateparam_shape_scales()
    assert scales_sr[("ttbb", "norm", "Up")] == pytest.approx(expected)
    assert scales_cr[("ttbb", "norm", "Up")] == pytest.approx(expected)
    assert scales_sr[("ttbb", "norm", "Down")] == pytest.approx(1.0)

    # the rearranged histogram keeps the rebinning underflow (10 from CR-range x)
    assert dc_sr.rearrange_histograms(category="CR")[
        "ttbb_2018", "nominal", :
    ].values(flow=True).sum() == pytest.approx(10.0)

    # written in-range templates: the relative SR/CR response stays the injected
    # +20%, and both cards share the same overall rescaling
    dc_sr.rateparam_shape_scale = scales_sr
    dc_cr.rateparam_shape_scale = scales_cr
    templates_sr = dc_sr.create_shape_histogram_dict()
    templates_cr = dc_cr.create_shape_histogram_dict()
    ratio_sr = (
        templates_sr["ttbb_2018_normUp"].values().sum()
        / templates_sr["ttbb_2018_nominal"].values().sum()
    )
    ratio_cr = (
        templates_cr["ttbb_2018_normUp"].values().sum()
        / templates_cr["ttbb_2018_nominal"].values().sum()
    )
    assert ratio_sr == pytest.approx(1.2 * expected)
    assert ratio_cr == pytest.approx(1.0 * expected)
    assert ratio_sr / ratio_cr == pytest.approx(1.2)


def test_rateparam_scale_from_norm_histograms():
    # A card whose variable is filled in SR only (only_categories) cannot read the
    # CR totals from its own histogram: it must fail loudly, and it must derive the
    # same category-inclusive factor when a histogram filled in every norm category
    # is given as `rateparam_norm_histograms`.
    sr_only = _norm_variation_histogram(categories=("SR",))
    with pytest.raises(ValueError, match="rateparam_norm_histograms"):
        _norm_variation_datacard("SR", histogram=sr_only).compute_rateparam_shape_scales()

    dc_sr = _norm_variation_datacard(
        "SR",
        bins_edges=[0.8, 0.9, 1.0],
        histogram=sr_only,
        rateparam_norm_histograms=_norm_variation_histogram(),
    )
    scales = dc_sr.compute_rateparam_shape_scales()
    assert scales[("ttbb", "norm", "Up")] == pytest.approx(30.0 / 34.0)
    assert scales[("ttbb", "norm", "Down")] == pytest.approx(1.0)
    # the card's own (SR-only, rebinned) templates are untouched by the norm input
    assert dc_sr.histogram["ttbb_2018", "nominal", :].values().sum() == pytest.approx(
        20.0
    )


def test_shapeu_systematic_gets_templates_and_datacard_type():
    # Combine shape flavours (shapeU here) must be treated like "shape" everywhere
    # templates are needed, and written with their own type in the card.
    dc = _norm_variation_datacard("SR", typ="shapeU")
    assert list(dc.systematics.get_systematics_by_type("shape")) == ["norm"]
    assert dc.systematics.get_systematics_by_type("shapeU")["norm"].typ == "shapeU"
    assert "normUp" in dc.histogram.axes["variation"]
    assert "ttbb_2018_normUp" in dc.create_shape_histogram_dict()
    (line,) = [l for l in dc.systematics_section().splitlines() if l.startswith("norm")]
    assert line.split()[1] == "shapeU"
