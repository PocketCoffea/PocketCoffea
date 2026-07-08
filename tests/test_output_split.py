"""Unit tests for the split-histogram output format
(:mod:`pocket_coffea.utils.output_split`).

These use a committed reference coffea output as fixture and check that the
split format round-trips histograms/metadata/columns exactly, that the reader
exposes the index and metadata without loading histograms, and that the
convert-output / merge --split write paths agree with the monolithic format.
"""

import os

import numpy as np
import hist
import pytest
from coffea.util import load

from pocket_coffea.utils.output_split import (
    save_split,
    SplitOutput,
    is_split_output,
    load_metadata,
    to_monolithic,
    make_provider,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(
    REPO, "tests/test_full_configs/test_new_weights/output_3b6cf6c/output_all.coffea"
)

pytestmark = pytest.mark.skipif(
    not os.path.exists(FIXTURE), reason="reference coffea fixture not available"
)


# ---------------------------------------------------------------------------
# helpers / fixtures
# ---------------------------------------------------------------------------
def _deep_equal(a, b, path=""):
    """Assert two output object graphs are equal, comparing hist.Hist by their
    bin values/variances and coffea accumulators by their ``.value``."""
    if isinstance(a, hist.Hist) or isinstance(b, hist.Hist):
        assert type(a) == type(b), f"{path}: type mismatch {type(a)} vs {type(b)}"
        assert [ax.name for ax in a.axes] == [ax.name for ax in b.axes], f"{path}: axes"
        assert np.array_equal(a.values(flow=True), b.values(flow=True)), f"{path}: values"
        va, vb = a.variances(flow=True), b.variances(flow=True)
        if va is None or vb is None:
            assert va is vb, f"{path}: variances presence"
        else:
            assert np.array_equal(va, vb), f"{path}: variances"
        return
    if isinstance(a, dict):
        assert isinstance(b, dict) and a.keys() == b.keys(), f"{path}: dict keys"
        for k in a:
            _deep_equal(a[k], b[k], f"{path}.{k}")
        return
    if isinstance(a, (list, tuple)):
        assert len(a) == len(b), f"{path}: length"
        for i, (x, y) in enumerate(zip(a, b)):
            _deep_equal(x, y, f"{path}[{i}]")
        return
    if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
        assert np.array_equal(a, b), f"{path}: ndarray"
        return
    if hasattr(a, "value") and hasattr(b, "value"):  # coffea column_accumulator etc.
        _deep_equal(a.value, b.value, f"{path}.value")
        return
    assert a == b or (isinstance(a, float) and np.isclose(a, b)), f"{path}: {a!r} != {b!r}"


@pytest.fixture(scope="module")
def mono():
    return load(FIXTURE)


@pytest.fixture(scope="module")
def split_path(tmp_path_factory, mono):
    p = str(tmp_path_factory.mktemp("split") / "out_split.coffea")
    save_split(mono, p)
    return p


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------
def test_is_split_output(split_path):
    assert is_split_output(split_path)
    assert not is_split_output(FIXTURE)


def test_variable_index(mono, split_path):
    r = SplitOutput(split_path)
    assert set(r.variables) == set(mono["variables"].keys())
    assert r.version is not None


def test_categories_present(split_path):
    assert len(SplitOutput(split_path).categories) > 0


def test_metadata_without_histograms(split_path):
    md = load_metadata(split_path)
    assert "variables" not in md
    assert "datasets_metadata" in md
    assert "cutflow" in md and "sumw" in md


def test_get_variable_structure(mono, split_path):
    r = SplitOutput(split_path)
    v = r.variables[0]
    sub = r.get_variable(v)
    assert set(sub.keys()) == set(mono["variables"][v].keys())
    for sample in sub:
        assert set(sub[sample].keys()) == set(mono["variables"][v][sample].keys())


def test_roundtrip_lz4(mono, split_path):
    _deep_equal(to_monolithic(split_path), mono, "root")


def test_roundtrip_no_compression(mono, tmp_path):
    p = str(tmp_path / "nc.coffea")
    save_split(mono, p, compression="none")
    assert is_split_output(p)
    _deep_equal(to_monolithic(p), mono, "root")


def test_providers_agree(split_path):
    mp = make_provider([FIXTURE])
    sp = make_provider([split_path])
    assert mp.kind == "monolithic" and sp.kind == "split"
    assert set(mp.variables) == set(sp.variables)
    assert mp.categories == SplitOutput(split_path).categories
    for v in mp.variables:
        _deep_equal(mp.get_variable(v), sp.get_variable(v), f"var:{v}")


def test_restrict_variables_monolithic():
    mp = make_provider([FIXTURE])
    keep = list(mp.variables)[:1]
    mp.restrict_variables(keep)
    assert set(mp.variables) == set(keep)


def test_restrict_variables_split_is_noop(split_path):
    sp = make_provider([split_path])
    before = set(sp.variables)
    sp.restrict_variables(list(before)[:1])  # no-op for lazy provider
    assert set(sp.variables) == before


def test_convert_output_roundtrip(mono, tmp_path):
    from pocket_coffea.scripts.convert_output import convert_output

    sp = str(tmp_path / "c_split.coffea")
    mo = str(tmp_path / "c_mono.coffea")
    convert_output([FIXTURE], sp, target_format="split", force=True)
    assert is_split_output(sp)
    convert_output([sp], mo, target_format="monolithic", force=True)
    assert not is_split_output(mo)
    _deep_equal(load(mo), mono, "root")


def test_merge_save_split(mono, tmp_path):
    from pocket_coffea.scripts.merge_outputs import _save_output

    sp = str(tmp_path / "m_split.coffea")
    _save_output(mono, sp, "split")
    assert is_split_output(sp)
    _save_output(mono, str(tmp_path / "m_mono.coffea"), "monolithic")
    assert not is_split_output(str(tmp_path / "m_mono.coffea"))
    _deep_equal(to_monolithic(sp), mono, "root")
