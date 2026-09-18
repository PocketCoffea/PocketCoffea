"""Offline unit tests for gen-lepton flavour splitting (no EOS / proxy needed).

`getGenLeptons` filters and pt-sorts a `leptons` subset of `GenPart`, then splits
by flavour. The split must index the *filtered* collection, not the full `GenPart`
(different per-event lengths) — otherwise awkward 1.x silently returns entries
scrambled across events.
"""
import numpy as np
import awkward as ak
from omegaconf import OmegaConf

from pocket_coffea.lib.gen_objects import getGenLeptons


def _make_events():
    # Event 0: e(30), mu(25), e(10), photon(5)  -> photon dropped by flavour filter
    # Event 1: mu(40), e(20), tau(5)            -> tau dropped by flavour filter
    pdgId = ak.Array([[11, 13, 11, 22], [13, 11, 15]])
    pt = ak.Array([[30.0, 25.0, 10.0, 5.0], [40.0, 20.0, 5.0]])
    eta = ak.Array([[0.5, 0.5, 0.5, 0.5], [0.5, 0.5, 0.5]])
    status = ak.Array([[1, 1, 1, 1], [1, 1, 1]])
    parent = ak.zip({"pdgId": ak.Array([[23, 23, 23, 23], [23, 23, 23]])})
    genpart = ak.zip(
        {"pdgId": pdgId, "pt": pt, "eta": eta, "status": status, "parent": parent}
    )
    return ak.Array({"GenPart": genpart})


def _params():
    return OmegaConf.create({"object_preselection": {"GEN_Lep": {"eta": 2.5, "pt": 6.0}}})


def test_gen_lepton_flavour_split_is_aligned():
    events = _make_events()
    params = _params()

    muons = getGenLeptons(events, "Muon", params)
    electrons = getGenLeptons(events, "Electron", params)
    both = getGenLeptons(events, "Both", params)

    # Every returned muon/electron must have the right flavour (the bug returned mixed).
    assert ak.all(np.abs(ak.flatten(muons.pdgId)) == 13)
    assert ak.all(np.abs(ak.flatten(electrons.pdgId)) == 11)

    # Expected content after |eta|<2.5, pt>6, pt-desc sort.
    assert ak.to_list(muons.pt) == [[25.0], [40.0]]
    assert ak.to_list(electrons.pt) == [[30.0, 10.0], [20.0]]

    # Flavour split partitions "Both".
    assert ak.to_list(ak.num(both)) == [3, 2]
    assert ak.to_list(ak.num(muons) + ak.num(electrons)) == ak.to_list(ak.num(both))


def test_gen_lepton_unsupported_flavour_raises():
    events = _make_events()
    import pytest

    with pytest.raises(ValueError):
        getGenLeptons(events, "Tau", _params())
