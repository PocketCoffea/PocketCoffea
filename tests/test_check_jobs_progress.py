"""Unit tests for the per-sample / per-dataset progress logic in
`pocket-coffea check-jobs`.

The pure aggregator and the jobs_config.yaml loader live in
`pocket_coffea/utils/job_progress.py` precisely so they can be tested
without pulling in rucio / coffea / rich. The progress-bar renderer
itself does need `rich.markup` strings, but it's pure-string and tested
here directly from check_jobs.py.
"""
import os

import pytest

from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
)


# ----------------------- aggregate_by_group -----------------------

def test_aggregate_basic_counts():
    """Mixed statuses across three samples; verify totals and percentages."""
    group_to_jobs = {
        "TT":  ["job_0", "job_1", "job_2", "job_3"],
        "ttH": ["job_4", "job_5"],
        "DATA":["job_6", "job_7", "job_8", "job_9"],
    }
    idle    = ["job_3", "job_5"]
    running = ["job_2"]
    done    = ["job_0", "job_1", "job_4", "job_6", "job_7"]
    failed  = ["job_8", "job_9"]

    res = aggregate_by_group(group_to_jobs, idle, running, done, failed)

    assert res["TT"]   == {"total": 4, "idle": 1, "running": 1, "done": 2, "failed": 0, "pct_done": 50.0}
    assert res["ttH"]  == {"total": 2, "idle": 1, "running": 0, "done": 1, "failed": 0, "pct_done": 50.0}
    assert res["DATA"] == {"total": 4, "idle": 0, "running": 0, "done": 2, "failed": 2, "pct_done": 50.0}


def test_aggregate_empty_group_gives_zero_pct():
    res = aggregate_by_group({"EmptySample": []}, [], [], [], [])
    assert res["EmptySample"] == {"total": 0, "idle": 0, "running": 0, "done": 0, "failed": 0, "pct_done": 0.0}


def test_aggregate_overlap_counts_under_each_group():
    """A job that touches two samples (uniform-split case) must be counted
    under both — that's the documented behaviour, called out in the table
    title via `multi_sample_overlap`."""
    group_to_jobs = {"A": ["job_0", "job_1"], "B": ["job_0", "job_2"]}
    done = ["job_0"]
    res = aggregate_by_group(group_to_jobs, [], [], done, [])
    assert res["A"]["done"] == 1
    assert res["B"]["done"] == 1
    # Each group still sees the job once even though it's listed in both.
    assert res["A"]["total"] == 2
    assert res["B"]["total"] == 2


def test_aggregate_pct_done_full_completion():
    group_to_jobs = {"ttH": ["job_0", "job_1", "job_2"]}
    done = ["job_0", "job_1", "job_2"]
    res = aggregate_by_group(group_to_jobs, [], [], done, [])
    assert res["ttH"]["pct_done"] == 100.0


# ----------------------- load_job_to_group_map -----------------------

def test_load_returns_none_when_yaml_missing(tmp_path):
    sample_map, ds_map = load_job_to_group_map(str(tmp_path))
    assert sample_map is None
    assert ds_map is None


def test_load_builds_reverse_maps(tmp_path):
    import yaml as pyyaml
    cfg = {
        "jobs_list": {
            "job_0": {"filesets": {"TT_2018":  {"metadata": {"sample": "TT"},  "files": []}}},
            "job_1": {"filesets": {"TT_2018":  {"metadata": {"sample": "TT"},  "files": []}}},
            "job_2": {"filesets": {"ttH_2018": {"metadata": {"sample": "ttH"}, "files": []}}},
            # uniform-split: one job touching two samples
            "job_3": {"filesets": {
                "TT_2018":  {"metadata": {"sample": "TT"},  "files": []},
                "ttH_2018": {"metadata": {"sample": "ttH"}, "files": []},
            }},
        }
    }
    yaml_path = tmp_path / "jobs_config.yaml"
    yaml_path.write_text(pyyaml.dump(cfg))

    sample_map, ds_map = load_job_to_group_map(str(tmp_path))

    assert set(sample_map["TT"])  == {"job_0", "job_1", "job_3"}
    assert set(sample_map["ttH"]) == {"job_2", "job_3"}
    assert set(ds_map["TT_2018"])  == {"job_0", "job_1", "job_3"}
    assert set(ds_map["ttH_2018"]) == {"job_2", "job_3"}


# ----------------------- _render_progress_bar -----------------------

def test_progress_bar_renders_each_status_color():
    """The bar renderer composes rich markup. Verify each colour appears
    when its share is > 0 and the total width is preserved."""
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 10, "done": 4, "running": 2, "idle": 3, "failed": 1, "pct_done": 40.0}
    bar = _render_progress_bar(counts, width=20)

    # All four colour tags must be present
    for color in ("green", "magenta", "blue", "red"):
        assert f"[{color}]" in bar, f"missing colour {color} in bar: {bar!r}"

    # Total filled characters equal `width` regardless of rounding
    filled = bar.count("█")
    assert filled == 20


def test_progress_bar_empty_group_placeholder():
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 0, "done": 0, "running": 0, "idle": 0, "failed": 0, "pct_done": 0.0}
    bar = _render_progress_bar(counts, width=20)
    assert "·" in bar
    # No coloured segments
    for color in ("green", "magenta", "blue", "red"):
        assert f"[{color}]" not in bar


def test_progress_bar_skips_zero_segments():
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 4, "done": 4, "running": 0, "idle": 0, "failed": 0, "pct_done": 100.0}
    bar = _render_progress_bar(counts, width=12)

    # Only green segment
    assert "[green]" in bar
    for color in ("magenta", "blue", "red"):
        assert f"[{color}]" not in bar
    assert bar.count("█") == 12
