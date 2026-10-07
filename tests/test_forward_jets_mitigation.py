"""Offline checks of the JEC applied by level and of the Run3 forward jets mitigations
(``jets_calibration.forward_jets_mitigation``).

No input file is needed: the JEC/JER corrections are written to a small synthetic
correctionlib JSON and the events are built from in-memory arrays.
"""
import os

import awkward as ak
import numpy as np
import pytest
from coffea.nanoevents.methods import vector
from correctionlib.schemav2 import CorrectionSet
from omegaconf import OmegaConf

from pocket_coffea.lib.jets import (
    abs_eta_in_regions,
    forward_jets_pt_cut_mask,
    get_forward_jets_mitigation,
    jet_correction_corrlib,
    jet_correction_corrlib_bylevel,
)

JET_TYPE = "AK4PFPuppi"
LEVELS = ["L1FastJet", "L2Relative", "L3Absolute", "L2L3Residual"]
JEC_INPUTS = {
    "L1FastJet": ["JetA", "JetEta", "JetPt", "Rho"],
    "L2Relative": ["JetEta", "JetPhi", "JetPt"],
    "L3Absolute": ["JetEta", "JetPt"],
    "L2L3Residual": ["JetEta", "JetPt"],
}
# per-level factors as a function of the pt they are evaluated at (x = JetPt);
# the residual is strongly pt dependent, so that its evaluation pt matters
JEC_FORMULAS = {
    "L1FastJet": "0.9+0.0*x",
    "L2Relative": "1.1+0.0*x",
    "L3Absolute": "1.0+0.0*x",
    "L2L3Residual": "1+3/x",
}
JER_FACTORS = {"PtResolution": 0.1, "ScaleFactor": 1.2, "SFUncertainty": 0.1}


def _formula_correction(name, inputs, expression):
    var = "JetPt" if "JetPt" in inputs else inputs[0]
    return {
        "name": name,
        "version": 1,
        "inputs": [{"name": i, "type": "real"} for i in inputs],
        "output": {"name": "factor", "type": "real"},
        "data": {"nodetype": "formula", "expression": expression, "parser": "TFormula", "variables": [var]},
    }


def _jec_json(path):
    corrections, compounds = [], []
    for tag in ("MC", "DATA"):
        for level in LEVELS:
            corrections.append(_formula_correction(f"{tag}_{level}_{JET_TYPE}", JEC_INPUTS[level], JEC_FORMULAS[level]))
        compounds.append({
            "name": f"{tag}_L1L2L3Res_{JET_TYPE}",
            "inputs": [{"name": i, "type": "real"} for i in ["JetA", "JetEta", "JetPhi", "JetPt", "Rho"]],
            "output": {"name": "factor", "type": "real"},
            "inputs_update": ["JetPt"],
            "input_op": "*",
            "output_op": "*",
            "stack": [f"{tag}_{level}_{JET_TYPE}" for level in LEVELS],
        })
    for name, value in JER_FACTORS.items():
        corrections.append(_formula_correction(f"JR_{name}_{JET_TYPE}", ["JetEta", "JetPt"], f"{value}+0.0*x"))
    cset = CorrectionSet.parse_obj(
        {"schema_version": 2, "corrections": corrections, "compound_corrections": compounds}
    )
    import gzip
    with gzip.open(path, "wt") as f:
        f.write(cset.json(exclude_unset=True))


@pytest.fixture(scope="module")
def json_path(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("jec") / "jet_jerc.json.gz")
    _jec_json(path)
    return path


# jets: central, 2.0<|eta|<2.5 (residual region), HE 2.5<|eta|<3 (twice), HF
ETA = [0.5, 2.2, 2.7, -2.8, 3.5]
PT = [20.0, 20.0, 20.0, 60.0, 20.0]
# gen match: the first HE jet is matched, the second is not
GEN_MATCHED = [True, True, True, False, False]


def _events():
    gen = ak.zip(
        {"pt": [[19.0, 19.0, 19.0, 1.0, 1.0]], "eta": [ETA], "phi": [[0.0] * 5], "mass": [[0.0] * 5]},
        with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior,
    )
    gen = ak.mask(gen, ak.Array([GEN_MATCHED]))
    jets = ak.zip(
        {
            "pt": [PT], "eta": [ETA], "phi": [[0.0] * 5], "mass": [[5.0] * 5],
            "rawFactor": [[0.0] * 5], "area": [[0.5] * 5], "matched_gen": gen,
        },
        with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior, depth_limit=2,
    )
    return ak.zip(
        {"Jet": jets, "Rho": ak.zip({"fixedGridRhoFastjetAll": [10.0]}), "event": [1], "run": [1]},
        depth_limit=1,
    )


def _mitigation_cfg(**apply):
    cfg = OmegaConf.load(os.path.join(
        os.path.dirname(__file__), "..", "pocket_coffea", "parameters", "jets_calibration.yaml"
    )).jets_calibration.forward_jets_mitigation
    for name, value in apply.items():
        cfg[name].apply = value
    return cfg


def _correct(json_path, isMC, year="2024", level="L1L2L3Res", by_level=False, mitigation=None,
             variations=("nominal",), apply_jer=None):
    calib_params = {
        "json_path": json_path, "jec_mc": "MC", "jec_data": "DATA", "jer": "JR",
        "level": level, "by_level": by_level,
    }
    jets = jet_correction_corrlib(
        calib_params=calib_params,
        variations=list(variations),
        events=_events(),
        jet_type=JET_TYPE,
        jet_coll_name="Jet",
        chunk_metadata={"year": year, "isMC": isMC, "era": "C", "nano_version": 15},
        nano_version=15,
        jec_syst=len(variations) > 1,
        apply_jer=isMC if apply_jer is None else apply_jer,
        forward_mitigation=mitigation,
    )
    return np.asarray(ak.flatten(jets.pt))


def _residual(pt):
    return 1 + 3 / pt


def test_defaults_all_off():
    cfg = _mitigation_cfg()
    for name in ("pt_cut", "jer_genmatched_only", "residual_pt_floor"):
        assert cfg[name].apply is False
        for year in ("2022_preEE", "2023_postBPix", "2024", "2025"):
            assert get_forward_jets_mitigation(cfg, name, year, JET_TYPE) is None
    assert get_forward_jets_mitigation(None, "pt_cut", "2024") is None


def test_mitigation_years_and_jet_types():
    cfg = _mitigation_cfg(pt_cut=True, jer_genmatched_only=True, residual_pt_floor=True)
    for year in ("2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix", "2024"):
        assert get_forward_jets_mitigation(cfg, "pt_cut", year) is not None
        assert get_forward_jets_mitigation(cfg, "jer_genmatched_only", year, JET_TYPE) is not None
    # nothing to mitigate in 2025, the residual fix is 2024 only
    assert get_forward_jets_mitigation(cfg, "pt_cut", "2025") is None
    assert get_forward_jets_mitigation(cfg, "jer_genmatched_only", "2025", JET_TYPE) is None
    assert get_forward_jets_mitigation(cfg, "residual_pt_floor", "2024", JET_TYPE) is not None
    assert get_forward_jets_mitigation(cfg, "residual_pt_floor", "2023_preBPix", JET_TYPE) is None
    # only the listed jet types
    assert get_forward_jets_mitigation(cfg, "jer_genmatched_only", "2024", "AK8PFPuppi") is None
    # explicit switch overrides the configuration
    assert get_forward_jets_mitigation(cfg, "pt_cut", "2024", apply=False) is None
    assert get_forward_jets_mitigation(_mitigation_cfg(), "pt_cut", "2024", apply=True) is not None


def test_abs_eta_in_regions():
    eta = ak.Array([[0.0, 2.5, -2.99, 3.0, -4.9, 5.0]])
    assert ak.to_list(abs_eta_in_regions(eta, [[2.5, 3.0]])) == [[False, True, True, False, False, False]]
    assert ak.to_list(abs_eta_in_regions(eta, [[2.5, 3.0], [3.0, 5.0]])) == [[False, True, True, True, True, False]]
    assert ak.to_list(abs_eta_in_regions(eta, [])) == [[False] * 6]


def test_forward_jets_pt_cut_mask():
    jets = ak.zip({"pt": [[20.0, 20.0, 60.0, 20.0, 20.0]], "eta": [[0.5, 2.7, -2.8, 3.5, -2.2]]})
    cfg = _mitigation_cfg(pt_cut=True)
    # 2022/2023: HE and HF low-pt jets rejected
    assert ak.to_list(forward_jets_pt_cut_mask(jets, cfg, "2022_postEE")) == [[True, False, True, False, True]]
    # 2024: HF fixed, only HE
    assert ak.to_list(forward_jets_pt_cut_mask(jets, cfg, "2024")) == [[True, False, True, True, True]]
    # 2025: nothing
    assert ak.to_list(forward_jets_pt_cut_mask(jets, cfg, "2025")) == [[True] * 5]
    # switched off by default, can be switched on by the jet_selection flag
    assert ak.to_list(forward_jets_pt_cut_mask(jets, _mitigation_cfg(), "2024")) == [[True] * 5]
    assert ak.to_list(forward_jets_pt_cut_mask(jets, _mitigation_cfg(), "2024", apply=True)) == [[True, False, True, True, True]]
    assert ak.to_list(forward_jets_pt_cut_mask(jets, cfg, "2024", apply=False)) == [[True] * 5]


@pytest.mark.parametrize("isMC", [False, True])
def test_bylevel_equals_compound(json_path, isMC):
    compound = _correct(json_path, isMC)
    bylevel = _correct(json_path, isMC, level=LEVELS, by_level=True)
    np.testing.assert_allclose(bylevel, compound, rtol=1e-6)
    wrapper = jet_correction_corrlib_bylevel(
        {"json_path": json_path, "jec_mc": "MC", "jec_data": "DATA", "jer": "JR", "level": LEVELS},
        variations=["nominal"], events=_events(), jet_type=JET_TYPE, jet_coll_name="Jet",
        chunk_metadata={"year": "2024", "isMC": isMC, "era": "C", "nano_version": 15},
        nano_version=15, jec_syst=False, apply_jer=isMC,
    )
    np.testing.assert_allclose(np.asarray(ak.flatten(wrapper.pt)), compound, rtol=1e-6)
    if not isMC:
        # chained levels: the residual is evaluated at the MC-truth corrected pt
        pt_truth = np.array(PT) * 0.9 * 1.1
        np.testing.assert_allclose(compound, pt_truth * _residual(pt_truth), rtol=1e-6)


def test_bylevel_requires_list(json_path):
    with pytest.raises(Exception, match="must be a list"):
        _correct(json_path, False, by_level=True)


@pytest.mark.parametrize("by_level", [False, True])
def test_residual_pt_floor(json_path, by_level):
    level = LEVELS if by_level else "L1L2L3Res"
    cfg = _mitigation_cfg(residual_pt_floor=True)
    nominal = _correct(json_path, False, level=level, by_level=by_level)
    floored = _correct(json_path, False, level=level, by_level=by_level, mitigation=cfg)
    pt_truth = np.array(PT) * 0.9 * 1.1  # all below 30 GeV except the 60 GeV jet
    expected = pt_truth * _residual(pt_truth)
    # only the jet in 2.0 < |eta| < 2.5: residual evaluated at 30 GeV, applied on the real pt
    expected[1] = pt_truth[1] * _residual(30.0)
    np.testing.assert_allclose(floored, expected, rtol=1e-6)
    assert not np.isclose(floored[1], nominal[1])
    np.testing.assert_allclose(np.delete(floored, 1), np.delete(nominal, 1), rtol=1e-6)


def test_residual_pt_floor_not_applied(json_path):
    cfg = _mitigation_cfg(residual_pt_floor=True)
    # other years, MC, or switched off
    for kwargs in ({"year": "2023_preBPix"}, {"year": "2025"}, {}):
        mitig = cfg if kwargs else _mitigation_cfg()
        np.testing.assert_allclose(
            _correct(json_path, False, mitigation=mitig, **kwargs),
            _correct(json_path, False, **kwargs), rtol=1e-6)
    np.testing.assert_allclose(
        _correct(json_path, True, mitigation=cfg), _correct(json_path, True), rtol=1e-6)


@pytest.mark.parametrize("variations", [("nominal",), ("nominal", "JER")])
def test_jer_genmatched_only(json_path, variations):
    cfg = _mitigation_cfg(jer_genmatched_only=True)
    jes_only = _correct(json_path, True, apply_jer=False)
    nominal = _correct(json_path, True, variations=variations)
    mitigated = _correct(json_path, True, variations=variations, mitigation=cfg)
    # unmatched HE jet (index 3): no smearing with the mitigation
    np.testing.assert_allclose(mitigated[3], jes_only[3], rtol=1e-6)
    assert not np.isclose(nominal[3], jes_only[3])
    # all the other jets (matched, or unmatched outside HE) are smeared as before
    np.testing.assert_allclose(np.delete(mitigated, 3), np.delete(nominal, 3), rtol=1e-6)
    # the matched HE jet is still smeared (scaling method)
    assert not np.isclose(mitigated[2], jes_only[2])
    # 2025: nothing changes
    np.testing.assert_allclose(
        _correct(json_path, True, year="2025", variations=variations, mitigation=cfg),
        _correct(json_path, True, year="2025", variations=variations), rtol=1e-6)
