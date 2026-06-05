"""Tests for the zero-event-chunk sum_genweights recovery mechanism.

Two pieces:
1. `save_skimed_dataset_definition` carries the pre-skim dataset-level totals
   (computed by the skim run before the skim mask, so they include input
   chunks that produced no surviving events) into the new dataset JSON.
2. `apply_skim_sumgenweights_override` reads those totals
   back at postprocess time and overrides the per-chunk-reconstructed totals
   in the accumulator, so the rescaling normalises to the original dataset.
"""
import json

import pytest

from pocket_coffea.utils.skim import (
    apply_skim_sumgenweights_override,
    save_skimed_dataset_definition,
)


# ----------------------- save_skimed_dataset_definition -----------------------

def _make_processing_out(initial=1000, after_skim=400, files=("a.root", "b.root"),
                         sum_gw=12345.6, sum_signof_gw=987.0):
    return {
        "datasets_metadata": {
            "by_dataset": {
                "TT_2018": {
                    "sample": "TT",
                    "year": "2018",
                    "isMC": "True",
                    "nevents": str(initial),
                    "size": "1000",
                }
            }
        },
        "cutflow": {
            "initial": {"TT_2018": initial},
            "skim": {"TT_2018": after_skim},
        },
        "skimmed_files": {"TT_2018": list(files)},
        "nskimmed_events": {"TT_2018": [after_skim // len(files)] * len(files)},
        "sum_genweights": {"TT_2018": sum_gw},
        "sum_signOf_genweights": {"TT_2018": sum_signof_gw},
    }


def test_save_skim_definition_carries_totals(tmp_path):
    """The new dataset JSON must include the pre-skim totals as floats."""
    out_file = tmp_path / "skim_dataset.json"
    save_skimed_dataset_definition(_make_processing_out(), str(out_file))

    written = json.loads(out_file.read_text())
    meta = written["TT_2018"]["metadata"]

    assert meta["isSkim"] == "True"
    assert meta["sum_genweights"] == pytest.approx(12345.6)
    assert meta["sum_signOf_genweights"] == pytest.approx(987.0)


def test_save_skim_definition_no_totals_when_absent(tmp_path):
    """Older skim outputs (no sum_genweights key) keep the legacy schema."""
    proc_out = _make_processing_out()
    proc_out.pop("sum_genweights")
    proc_out.pop("sum_signOf_genweights")

    out_file = tmp_path / "skim_dataset.json"
    save_skimed_dataset_definition(proc_out, str(out_file))

    meta = json.loads(out_file.read_text())["TT_2018"]["metadata"]
    assert "sum_genweights" not in meta
    assert "sum_signOf_genweights" not in meta


# ----------------------- postprocess override helper -----------------------

def _make_accumulator(sum_gw=80.0, sum_signof=60.0):
    """A stub accumulator carrying the per-chunk reconstructed (lossy) totals."""
    return {
        "sum_genweights": {"TT_2018": sum_gw},
        "sum_signOf_genweights": {"TT_2018": sum_signof},
    }


def _make_filesets(meta):
    return {"TT_2018": {"files": ["f.root"], "metadata": meta}}


def test_override_replaces_when_skim_and_totals_present():
    acc = _make_accumulator(sum_gw=80.0, sum_signof=60.0)
    filesets = _make_filesets({
        "sample": "TT", "isSkim": "True",
        "sum_genweights": 100.0, "sum_signOf_genweights": 75.0,
    })

    overridden = apply_skim_sumgenweights_override(acc, filesets)

    assert overridden == ["TT_2018"]
    assert acc["sum_genweights"]["TT_2018"] == pytest.approx(100.0)
    assert acc["sum_signOf_genweights"]["TT_2018"] == pytest.approx(75.0)


def test_override_noop_when_not_skim():
    acc = _make_accumulator()
    filesets = _make_filesets({
        "sample": "TT", "isSkim": "False",
        "sum_genweights": 100.0,
    })

    overridden = apply_skim_sumgenweights_override(acc, filesets)

    assert overridden == []
    assert acc["sum_genweights"]["TT_2018"] == pytest.approx(80.0)


def test_override_noop_when_metadata_missing_totals():
    """Old-style skim datasets (no totals embedded) must fall through silently."""
    acc = _make_accumulator()
    filesets = _make_filesets({"sample": "TT", "isSkim": "True"})

    overridden = apply_skim_sumgenweights_override(acc, filesets)

    assert overridden == []
    assert acc["sum_genweights"]["TT_2018"] == pytest.approx(80.0)
    assert acc["sum_signOf_genweights"]["TT_2018"] == pytest.approx(60.0)


def test_override_accepts_bool_and_string_isSkim_flags():
    """isSkim is historically written as 'True' (string); also accept bool True."""
    for is_skim_value in (True, "True", "true"):
        acc = _make_accumulator()
        filesets = _make_filesets({
            "sample": "TT", "isSkim": is_skim_value,
            "sum_genweights": 200.0,
        })
        apply_skim_sumgenweights_override(acc, filesets)
        assert acc["sum_genweights"]["TT_2018"] == pytest.approx(200.0)


def test_override_only_acts_on_listed_datasets():
    """A dataset present in accumulator but not in filesets is left alone."""
    acc = _make_accumulator()
    acc["sum_genweights"]["UnknownDS"] = 999.0
    filesets = _make_filesets({"sample": "TT", "isSkim": "True", "sum_genweights": 100.0})

    apply_skim_sumgenweights_override(acc, filesets)
    assert acc["sum_genweights"]["UnknownDS"] == pytest.approx(999.0)
