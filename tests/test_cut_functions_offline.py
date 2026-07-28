"""Offline unit tests for cut helpers (no EOS / proxy needed).

Covers the `minpt` threshold in nElectron/nMuon (previously accepted but ignored)
and the Cut identity now including `collection` and tolerating non-JSON params.
"""
import numpy as np
import awkward as ak

from pocket_coffea.lib.cut_functions import nElectron, nMuon
from pocket_coffea.lib.cut_definition import Cut


def _events():
    electrons = ak.zip({"pt": ak.Array([[35.0, 20.0, 5.0], [40.0], []])})
    muons = ak.zip({"pt": ak.Array([[50.0, 12.0], [8.0], [33.0, 31.0]])})
    return ak.Array({"ElectronGood": electrons, "MuonGood": muons})


def test_nElectron_applies_minpt():
    events = _events()
    # pt>30 counts: [1, 1, 0]
    assert ak.to_list(nElectron(events, {"coll": "ElectronGood", "N": 1, "minpt": 30}, "2018")) == [True, True, False]
    assert ak.to_list(nElectron(events, {"coll": "ElectronGood", "N": 2, "minpt": 30}, "2018")) == [False, False, False]
    # With no threshold, counts are [3, 1, 0] -> the N=2 result differs, proving minpt is honoured.
    assert ak.to_list(nElectron(events, {"coll": "ElectronGood", "N": 2, "minpt": 0}, "2018")) == [True, False, False]


def test_nMuon_applies_minpt():
    events = _events()
    # pt>30 counts: [1, 0, 2]
    assert ak.to_list(nMuon(events, {"coll": "MuonGood", "N": 2, "minpt": 30}, "2018")) == [False, False, True]
    # No threshold -> counts [2, 1, 2]
    assert ak.to_list(nMuon(events, {"coll": "MuonGood", "N": 2, "minpt": 0}, "2018")) == [True, False, True]


def _f(events, params, **kwargs):
    return None


def test_cut_identity_includes_collection():
    c_events = Cut(name="x", params={"a": 1, "b": 2}, function=_f, collection="events")
    c_jet = Cut(name="x", params={"a": 1, "b": 2}, function=_f, collection="Jet")
    assert hash(c_events) != hash(c_jet)
    assert c_events.id != c_jet.id
    assert c_events != c_jet


def test_cut_hash_is_param_order_independent_and_numpy_safe():
    c1 = Cut(name="y", params={"a": 1, "b": 2}, function=_f)
    c2 = Cut(name="y", params={"b": 2, "a": 1}, function=_f)
    assert hash(c1) == hash(c2)
    # A numpy-scalar param must not break hashing/id.
    c_np = Cut(name="z", params={"thr": np.float64(3.5)}, function=_f)
    assert isinstance(c_np.id, str)
