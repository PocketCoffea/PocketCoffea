"""Offline checks of the JER gen-matching inputs and of the JERSmear node semantics.

No input file or correctionlib JSON is needed: the JERSmear correction is built from
pocket_coffea.lib.jets.JERSMEAR_SCHEMA and the jets from small in-memory arrays.
"""
import awkward as ak
import numpy as np
from coffea.nanoevents.methods import vector
from correctionlib.schemav2 import CorrectionSet

from pocket_coffea.lib.jets import JERSMEAR_SCHEMA, add_jec_variables, safe_jersmear


def _jets():
    # one event, three jets: A matched at small dR, B matched at dR = 0.3, C unmatched
    gen = ak.zip(
        {"pt": [[50.0, 30.0, 1.0]], "eta": [[0.0, 1.0, 0.0]], "phi": [[0.0, 0.3, 0.0]], "mass": [[0.0, 0.0, 0.0]]},
        with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior,
    )
    gen = ak.mask(gen, ak.Array([[True, True, False]]))
    return ak.zip(
        {
            "pt": [[55.0, 33.0, 20.0]], "eta": [[0.0, 1.0, 2.0]], "phi": [[0.05, 0.0, 0.0]],
            "mass": [[5.0, 5.0, 5.0]], "rawFactor": [[0.1, 0.1, 0.1]], "matched_gen": gen,
        },
        with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior, depth_limit=1,
    )


def test_pt_gen_nomatch_and_dr():
    rho = ak.Array([10.0])
    # legacy default: no dR cut, 0 for no match
    out = add_jec_variables(_jets(), rho, isMC=True)
    assert ak.to_list(out.pt_gen) == [[50.0, 30.0, 0.0]]
    # correctionlib path: dR < 0.2 and -1 for no match -> jet B counts as unmatched
    out = add_jec_variables(_jets(), rho, isMC=True, gen_match_dr=0.2, nomatch_value=-1.0)
    assert ak.to_list(out.pt_gen) == [[50.0, -1.0, -1.0]]


def test_jersmear_node_genpt_zero_is_scaling():
    """GenPt = 0 falls in the scaling bin and gives factor = JERsf: 'no match' must be -1."""
    ev = CorrectionSet.parse_obj({"schema_version": 2, "corrections": [JERSMEAR_SCHEMA]}).to_evaluator()
    smear = ev["JERSmear"]
    sf, jer = 1.2, 0.4
    # pt, eta, genpt, rho, event, JER, JERsf
    assert np.isclose(smear.evaluate(20.0, 3.0, 0.0, 10.0, 1, jer, sf), sf)
    # GenPt = -1: stochastic, deterministic in (pt, eta, rho, event), not equal to sf
    s1 = smear.evaluate(20.0, 3.0, -1.0, 10.0, 1, jer, sf)
    s2 = smear.evaluate(20.0, 3.0, -1.0, 10.0, 1, jer, sf)
    assert s1 == s2 and not np.isclose(s1, sf)
    # matched: scaling formula
    assert np.isclose(smear.evaluate(20.0, 3.0, 18.0, 10.0, 1, jer, sf), 1 + (sf - 1) * 2.0 / 20.0)


def test_safe_jersmear():
    s = np.array([0.9, -0.2, 0.0, np.nan, 1.3])
    assert ak.to_list(safe_jersmear(s)) == [0.9, 1.0, 1.0, 1.0, 1.3]
