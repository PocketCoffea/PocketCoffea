"""Offline tests for merge_outputs / hadd fixes.

do_hadd needs ROOT (present in the CI container). The rucio stub is defensive in case
an import chain pulls it (the local image's rucio file is unreadable).
"""
import sys
import types

import pytest


def _ensure_rucio_importable():
    try:
        import rucio.client  # noqa: F401
        import rucio.common.client  # noqa: F401
        return
    except (ImportError, OSError):
        rucio = types.ModuleType("rucio")
        client = types.ModuleType("rucio.client")
        client.Client = object
        common = types.ModuleType("rucio.common")
        common_client = types.ModuleType("rucio.common.client")
        common_client.detect_client_location = lambda *a, **k: {}
        rucio.client = client
        rucio.common = common
        common.client = common_client
        sys.modules.setdefault("rucio", rucio)
        sys.modules.setdefault("rucio.client", client)
        sys.modules.setdefault("rucio.common", common)
        sys.modules.setdefault("rucio.common.client", common_client)


_ensure_rucio_importable()


def _install_fake_root():
    """Minimal ROOT stub so do_hadd can be exercised where ROOT is not installed."""
    if "ROOT" in sys.modules:
        return

    class _Tree:
        def Write(self):
            pass

    class _Chain:
        def Add(self, f):
            pass

        def CloneTree(self, n):
            return _Tree()

    class _File:
        def Close(self):
            pass

    root = types.ModuleType("ROOT")
    root.TChain = lambda name: _Chain()
    root.TFile = types.SimpleNamespace(Open=lambda path, mode: _File())
    sys.modules["ROOT"] = root


def test_do_hadd_does_not_nameerror_on_R(tmp_path):
    # The fix binds R via `import ROOT as R` inside do_hadd; verify it resolves R (here a
    # stub) and returns a (path, code) tuple instead of raising NameError on unbound R.
    _install_fake_root()
    from pocket_coffea.scripts.hadd_skimmed_files import do_hadd

    out = str(tmp_path / "out.root")
    result = do_hadd((out, [str(tmp_path / "does_not_exist.root")]))
    assert result == (out, 0)


def test_merge_outputs_no_inputs_no_config_exits_cleanly(tmp_path):
    import pocket_coffea.scripts.merge_outputs as mo

    # Previously this raised NameError on the unset job_config; it should now print a
    # message and exit(1).
    with pytest.raises(SystemExit):
        mo.merge_outputs([], str(tmp_path / "out.coffea"), jobs_config=None, force=True)


def test_merge_outputs_loads_reference_file_once(tmp_path, monkeypatch):
    import pocket_coffea.scripts.merge_outputs as mo

    calls = []
    monkeypatch.setattr(mo, "load", lambda f: calls.append(f) or {})
    # Force a "mismatch" so merge stops right after the type-check loop.
    monkeypatch.setattr(mo, "compare_dict_types", lambda a, b: True)

    files = [str(tmp_path / f"f{i}.coffea") for i in range(3)]
    with pytest.raises(TypeError):
        mo.merge_outputs(files, str(tmp_path / "out.coffea"), jobs_config=None, force=True)

    # Reference loaded once + each of the other two once = 3 (old code reloaded f0 -> 4).
    assert len(calls) == 3
