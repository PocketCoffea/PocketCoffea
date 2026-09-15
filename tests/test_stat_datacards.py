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
