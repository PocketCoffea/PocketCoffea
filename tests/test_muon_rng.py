"""Offline unit tests for the vectorized muon-smearing RNG.

These guard the bug fix in ``muon_scale_and_resolution.get_rndm``: the per-muon
uniform used for the Crystal-Ball smearing is now derived from a fully vectorized
64-bit hash of (event, lumi, phi) instead of a per-muon ``SeedSequence`` +
``MT19937`` construction. The old scalar ``SeedSequence.generate(1)`` mixed only
the first entropy word (the event number), so every muon in the same event drew
the *same* uniform -- the tests below assert the draws are now independent.

No EOS / grid proxy needed.
"""
import numpy as np
from pocket_coffea.lib.muon_scale_and_resolution import _hashed_uniform, _splitmix64


def test_hashed_uniform_range_and_dtype():
    n = 5000
    ev = np.arange(n, dtype=np.uint64)
    lumi = (np.arange(n) % 1000).astype(np.uint64)
    phi = (np.arange(n) % 4096).astype(np.uint64)
    u = _hashed_uniform(ev, lumi, phi)
    assert u.dtype == np.float64
    assert np.all(u >= 0.0) and np.all(u < 1.0)


def test_hashed_uniform_reproducible():
    # Identical (event, lumi, phi) must give identical draws (determinism across
    # chunks / re-processing -- the smearing must be reproducible).
    ev = np.array([12345, 12345], dtype=np.uint64)
    lumi = np.array([7, 7], dtype=np.uint64)
    phi = np.array([100, 100], dtype=np.uint64)
    u = _hashed_uniform(ev, lumi, phi)
    assert u[0] == u[1]


def test_hashed_uniform_independent_per_muon():
    # THE BUG FIX. Two muons in the same event (same event & lumi) but different
    # phi must get DIFFERENT draws. Previously they were identical.
    u_phi = _hashed_uniform(
        np.array([999, 999], dtype=np.uint64),
        np.array([5, 5], dtype=np.uint64),
        np.array([10, 3000], dtype=np.uint64),
    )
    assert u_phi[0] != u_phi[1]

    # Different lumi (same event & phi) must also decorrelate.
    u_lumi = _hashed_uniform(
        np.array([999, 999], dtype=np.uint64),
        np.array([5, 6], dtype=np.uint64),
        np.array([10, 10], dtype=np.uint64),
    )
    assert u_lumi[0] != u_lumi[1]


def test_hashed_uniform_uniformity():
    n = 200000
    ev = np.arange(n, dtype=np.uint64)
    lumi = (np.arange(n) % 1000).astype(np.uint64)
    phi = (np.arange(n) * 7 % 4096).astype(np.uint64)
    u = _hashed_uniform(ev, lumi, phi)
    assert abs(float(u.mean()) - 0.5) < 0.01
    assert abs(float(u.std()) - 1.0 / np.sqrt(12.0)) < 0.01


def test_splitmix64_avalanche():
    # A one-bit input change should flip roughly half the output bits.
    a = _splitmix64(np.array([0x0123456789ABCDEF], dtype=np.uint64))[0]
    b = _splitmix64(np.array([0x0123456789ABCDEE], dtype=np.uint64))[0]
    flipped = bin(int(a) ^ int(b)).count("1")
    assert 16 < flipped < 48  # ~32 of 64 bits differ
