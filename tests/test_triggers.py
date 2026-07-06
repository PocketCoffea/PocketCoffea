"""Offline unit tests for trigger-name handling (no EOS / proxy needed).

These synthesize a minimal `events` object with an `HLT` record so the trigger
helpers can be exercised without loading NanoAOD. They specifically use a trigger
name (`TkMu50`) that the old `str.lstrip("HLT_")` implementation mangled to
`kMu50` and then silently dropped from the OR.
"""
import numpy as np
import awkward as ak

from pocket_coffea.lib.triggers import (
    remove_trigger_prefix,
    apply_trigger_mask,
    get_trigger_mask_byprimarydataset,
)


def _make_events():
    """A 4-event synthetic object exposing `events.HLT.<path>`."""
    hlt = ak.Array(
        {
            "TkMu50": np.array([True, False, False, True]),
            "IsoMu24": np.array([False, True, False, False]),
            "Ele32_WPTight_Gsf": np.array([False, False, True, False]),
        }
    )
    return ak.Array({"event": np.arange(4, dtype=np.int64), "HLT": hlt})


def test_remove_trigger_prefix_is_not_charset_strip():
    # The whole point: prefix removal, not str.lstrip character-set stripping.
    assert remove_trigger_prefix("HLT_TkMu50", "HLT_") == "TkMu50"
    assert remove_trigger_prefix("HLT_TripleMu_12_10_5", "HLT_") == "TripleMu_12_10_5"
    assert remove_trigger_prefix("L1_LooseIsoEG28er2p1", "L1_") == "LooseIsoEG28er2p1"
    # No prefix present -> unchanged.
    assert remove_trigger_prefix("TkMu50", "HLT_") == "TkMu50"
    # Sanity: the old buggy behaviour is what we are avoiding.
    assert "HLT_TkMu50".lstrip("HLT_") == "kMu50"


def test_apply_trigger_mask_or():
    events = _make_events()
    mask = apply_trigger_mask(events, ["TkMu50", "IsoMu24"], year="2018", trigger_type="HLT")
    expected = events.HLT.TkMu50 | events.HLT.IsoMu24
    assert ak.all(mask == expected)
    assert ak.sum(mask) == 3


def test_apply_trigger_mask_skips_unknown(caplog):
    events = _make_events()
    with caplog.at_level("WARNING"):
        mask = apply_trigger_mask(
            events, ["TkMu50", "ThisPathDoesNotExist"], year="2018", trigger_type="HLT"
        )
    # Unknown path contributes nothing; TkMu50 still drives the mask.
    assert ak.all(mask == events.HLT.TkMu50)
    # ...and the drop is surfaced, not silent.
    assert any("ThisPathDoesNotExist" in rec.message for rec in caplog.records)


def test_bypd_regression_prefixed_names_survive():
    """Config stores full `HLT_...` names; they must map to the stripped fields.

    Pre-fix, `HLT_TkMu50` became `kMu50` (not a field) and was silently dropped,
    so the OR reduced to IsoMu24 only. This asserts both paths contribute.
    """
    events = _make_events()
    trigger_dict = {"2018": {"SingleMu": ["HLT_TkMu50", "HLT_IsoMu24"]}}

    # MC path: OR of all primary datasets.
    mask_mc = get_trigger_mask_byprimarydataset(
        events, trigger_dict, year="2018", isMC=True, trigger_prefix="HLT_"
    )
    expected = events.HLT.TkMu50 | events.HLT.IsoMu24
    assert ak.all(mask_mc == expected)
    assert ak.sum(mask_mc) == 3  # would be 1 if HLT_TkMu50 were dropped

    # Explicit primaryDatasets path (applies to MC and data alike).
    mask_pd = get_trigger_mask_byprimarydataset(
        events, trigger_dict, year="2018", isMC=True,
        primaryDatasets=["SingleMu"], trigger_prefix="HLT_",
    )
    assert ak.all(mask_pd == expected)
