"""The lepton-term switch of METCalibrator: default on, off removes exactly the lepton deltas."""
import awkward as ak
import numpy as np
from omegaconf import OmegaConf
from pocket_coffea.lib.calibrators.common.common import METCalibrator


def _calibrator(propagate):
    cal = object.__new__(METCalibrator)
    cal.rawMet_branch, cal.met_branch = "RawPuppiMET", "PuppiMET"
    cal.met_calib_active = False          # jets off: isolate the lepton terms
    cal.store_components = True
    cal.component_fields = [f"delta_{k}_{a}" for k in ("jet", "lowpt", "electron", "muon") for a in ("px", "py")]
    cal.propagate_leptons = propagate
    cal.isMC = True
    return cal


def _events():
    return ak.Array({
        "RawPuppiMET": {"pt": [100.0, 50.0], "phi": [0.0, 1.0]},
        "Electron": {"pt": [[55.0], []], "phi": [[0.0], []]},
        "Muon": {"pt": [[], [33.0]], "phi": [[], [1.0]]},
    })


ORIG = {"Electron.pt": ak.Array([[50.0], []]), "Muon.pt": ak.Array([[], [30.0]])}


def test_default_propagates_lepton_shifts():
    out = _calibrator(True).calibrate(_events(), ORIG, "nominal")
    assert np.allclose(ak.to_numpy(out["PuppiMET.pt"]), [95.0, 47.0])
    assert np.allclose(ak.to_numpy(out["PuppiMET.delta_electron_px"]), [5.0, 0.0])


def test_switch_off_leaves_raw_met_untouched():
    out = _calibrator(False).calibrate(_events(), ORIG, "nominal")
    assert np.allclose(ak.to_numpy(out["PuppiMET.pt"]), [100.0, 50.0])
    assert np.allclose(ak.to_numpy(out["PuppiMET.phi"]), [0.0, 1.0])
    for kind in ("electron", "muon"):
        for axis in ("px", "py"):
            assert np.allclose(ak.to_numpy(out[f"PuppiMET.delta_{kind}_{axis}"]), 0.0)
