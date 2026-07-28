"""Offline unit test for the correctionlib CorrectionSet cache.

Patches correctionlib.CorrectionSet.from_file so no real JSON is read: verifies that
load_correction_set parses each distinct path only once and returns the same object on
repeat calls (the same file requested for many chunks/variations is cached per process).
"""
from pocket_coffea.lib import correction_cache


def test_load_correction_set_caches_by_path(monkeypatch):
    calls = []

    def fake_from_file(path):
        calls.append(path)
        return object()  # a fresh sentinel per real parse

    monkeypatch.setattr(correction_cache.correctionlib.CorrectionSet, "from_file", fake_from_file)
    correction_cache.load_correction_set.cache_clear()
    try:
        a1 = correction_cache.load_correction_set("/eos/x/a.json")
        a2 = correction_cache.load_correction_set("/eos/x/a.json")  # cache hit
        b1 = correction_cache.load_correction_set("/eos/x/b.json")  # different file
        b2 = correction_cache.load_correction_set("/eos/x/b.json")  # cache hit

        assert a1 is a2, "same path must return the cached object"
        assert b1 is b2
        assert a1 is not b1, "different paths must not collide"
        # from_file (the expensive parse) runs once per distinct path, not per call
        assert calls == ["/eos/x/a.json", "/eos/x/b.json"]
    finally:
        correction_cache.load_correction_set.cache_clear()
