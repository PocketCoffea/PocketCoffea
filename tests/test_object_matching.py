"""Offline unit tests for object_matching (no EOS needed).

Uses coffea's Lorentz-vector behavior so `obj.metric_table` (deltaR) works on
hand-built objects.
"""
import awkward as ak
from coffea.nanoevents.methods import vector

from pocket_coffea.lib.deltaR_matching import object_matching

ak.behavior.update(vector.behavior)


def _obj(pt, eta, phi):
    return ak.zip(
        {"pt": pt, "eta": eta, "phi": phi, "mass": [[0.0] * len(e) for e in eta]},
        with_name="PtEtaPhiMLorentzVector",
    )


def test_dpt_cut_does_not_block_valid_match():
    # One jet; two quarks. q1 is closest in dR but fails the pt cut; q2 is slightly
    # farther in dR but passes. The jet must be matched to q2, not left unmatched.
    jets = _obj(pt=[[52.0]], eta=[[0.0]], phi=[[0.0]])
    quarks = _obj(pt=[[100.0, 50.0]], eta=[[0.0, 0.0]], phi=[[0.1, 0.2]])

    matched_obj, matched_obj2, dr = object_matching(
        quarks, jets, dr_min=0.5, dpt_max=10.0
    )
    # matched_obj is the quark matched to each jet (dim of jets).
    assert ak.to_list(matched_obj.pt) == [[50.0]]  # q2, not None and not q1


def test_pure_dr_matching_unchanged():
    # Simple 1-1 nearest-dR match still works when no pt cut is given.
    jets = _obj(pt=[[30.0, 30.0]], eta=[[0.0, 1.0]], phi=[[0.0, 0.0]])
    quarks = _obj(pt=[[30.0, 30.0]], eta=[[0.02, 1.02]], phi=[[0.0, 0.0]])

    matched_obj, matched_obj2, dr = object_matching(quarks, jets, dr_min=0.4)
    # Each jet matched to the quark at the same eta.
    assert ak.to_list(matched_obj.eta) == [[0.02, 1.02]]
