"""Unit tests for the muon-smearing RNG wiring.

The per-muon uniform used for the Crystal-Ball smearing is now drawn by the
``RandomSmearing`` correction shipped in ``muon_scalesmearing.json.gz`` -- a
correctionlib ``hashprng`` node keyed on ``(event, lumi, phi)``. These tests
guard the PocketCoffea-side wiring in ``muon_scale_and_resolution.get_rndm``:

  * the draw is *reproducible* (same event/lumi/phi -> same value across chunks
    / re-processing), and
  * two muons in the same event (same event & lumi) but different phi get
    *independent* draws -- the historical per-muon correlation bug, where every
    muon in an event drew the same uniform, must not reappear.

The RNG itself lives in the correction JSON on cvmfs, so these tests skip when
that file is not mounted (e.g. a bare runner without cvmfs).
"""
import os

import numpy as np
import awkward as ak
import pytest

from pocket_coffea.lib.muon_scale_and_resolution import get_rndm

# Candidate scale&smearing JSONs (any works -- RandomSmearing is the same node).
_CANDIDATE_JSONS = [
    "/cvmfs/cms-griddata.cern.ch/cat/metadata/MUO/"
    "Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-06-18/muon_scalesmearing.json.gz",
    "/cvmfs/cms-griddata.cern.ch/cat/metadata/MUO/"
    "Run3-22CDSep23-Summer22-NanoAODv12/2026-06-18/muon_scalesmearing.json.gz",
]


@pytest.fixture(scope="module")
def cset():
    path = next((p for p in _CANDIDATE_JSONS if os.path.exists(p)), None)
    if path is None:
        pytest.skip("muon_scalesmearing.json.gz not available (cvmfs not mounted)")
    import correctionlib

    return correctionlib.CorrectionSet.from_file(path)


def _nested(pt, eta, phi, nL, counts):
    """Build a jagged (per-event) muon collection from flat arrays."""
    return {
        "pt": ak.unflatten(np.asarray(pt, dtype=np.float64), counts),
        "eta": ak.unflatten(np.asarray(eta, dtype=np.float64), counts),
        "phi": ak.unflatten(np.asarray(phi, dtype=np.float64), counts),
        "nL": ak.unflatten(np.asarray(nL, dtype=np.int64), counts),
    }


def test_get_rndm_reproducible(cset):
    # One event, two muons; identical inputs on two calls must agree exactly.
    counts = [2]
    mu = _nested([40.0, 55.0], [0.3, -1.1], [0.5, 2.0], [12, 11], counts)
    ev = np.array([12345], dtype=np.uint64)
    lu = np.array([7], dtype=np.uint32)
    r1 = get_rndm(mu["eta"], mu["phi"], mu["nL"], ev, lu, cset, nested=True)
    r2 = get_rndm(mu["eta"], mu["phi"], mu["nL"], ev, lu, cset, nested=True)
    assert ak.all(ak.flatten(r1) == ak.flatten(r2))
    # nested structure preserved and finite
    assert ak.to_list(ak.num(r1)) == counts
    assert not ak.any(np.isnan(ak.flatten(r1)))


def test_get_rndm_independent_per_muon(cset):
    # THE BUG FIX: two muons in the same event & lumi but different phi must get
    # different draws (they are decorrelated by the phi entropy word).
    counts = [2]
    mu = _nested([40.0, 40.0], [0.3, 0.3], [10.0, 3000.0 * np.pi / 4096], [12, 12], counts)
    ev = np.array([999], dtype=np.uint64)
    lu = np.array([5], dtype=np.uint32)
    r = get_rndm(mu["eta"], mu["phi"], mu["nL"], ev, lu, cset, nested=True)
    r = ak.flatten(r)
    assert r[0] != r[1]


def test_get_rndm_independent_across_events(cset):
    # Same muon kinematics but different event numbers -> different draws.
    counts = [1, 1]
    mu = _nested([40.0, 40.0], [0.3, 0.3], [0.5, 0.5], [12, 12], counts)
    ev = np.array([1, 2], dtype=np.uint64)
    lu = np.array([5, 5], dtype=np.uint32)
    r = ak.flatten(get_rndm(mu["eta"], mu["phi"], mu["nL"], ev, lu, cset, nested=True))
    assert r[0] != r[1]


def test_randomsmearing_uniform(cset):
    # The underlying hashprng draw must be a uniform in [0,1).
    n = 200000
    ev = np.arange(n, dtype=np.uint64)
    lu = (np.arange(n) % 1000).astype(np.uint32)
    phi = ((np.arange(n) * 7 % 4096) / 4096.0 * np.pi).astype(np.float64)
    u = np.asarray(cset["RandomSmearing"].evaluate(ev, lu, phi))
    assert np.all(u >= 0.0) and np.all(u < 1.0)
    assert abs(float(u.mean()) - 0.5) < 0.01
    assert abs(float(u.std()) - 1.0 / np.sqrt(12.0)) < 0.01
