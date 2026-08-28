"""Tests for the inner-run-options forwarding mechanism that lets the OUTER
`pocket-coffea run --recreate-jobs --skip-bad-files` reach the INNER
`pocket-coffea run` inside each condor job (via --custom-run-options).

The whitelist filter and the idempotent .sub / job.sh patchers live in
`pocket_coffea/executors/executors_manual_jobs.py`. They are dependency-free
(stdlib + PyYAML) and can be tested directly.
"""
import os

import pytest
import yaml

from pocket_coffea.executors.executors_manual_jobs import (
    INNER_RUN_OPTIONS_FILENAME,
    INNER_RUN_OPTIONS_WHITELIST,
    ensure_job_sh_forwards_inner_yaml,
    ensure_sub_transfers_inner_yaml,
    write_inner_run_options,
)


# ----------------------- write_inner_run_options -----------------------

def test_write_keeps_only_whitelisted_keys(tmp_path):
    run_options = {
        "skip-bad-files": True,
        "tree-reduction": 4,
        # Outer-only keys must NOT be propagated
        "cores-per-worker": 8,
        "mem-per-worker": "4GB",
        "worker-image": "/cvmfs/.../image",
        "queue": "workday",
        "chunksize": 200_000,           # handled via .sub arguments, not via YAML
    }
    path = write_inner_run_options(str(tmp_path), run_options)

    assert os.path.basename(path) == INNER_RUN_OPTIONS_FILENAME
    with open(path) as f:
        # Skip the comment header
        loaded = yaml.safe_load(f.read())
    assert loaded == {"skip-bad-files": True, "tree-reduction": 4}
    # Outer-only keys are absent
    for forbidden in ("cores-per-worker", "mem-per-worker", "worker-image", "queue", "chunksize"):
        assert forbidden not in loaded


def test_write_drops_none_values(tmp_path):
    run_options = {"skip-bad-files": True, "tree-reduction": None}
    path = write_inner_run_options(str(tmp_path), run_options)
    loaded = yaml.safe_load(open(path).read())
    assert loaded == {"skip-bad-files": True}


def test_write_empty_when_nothing_whitelisted(tmp_path):
    """No whitelisted key set: still writes the YAML (so job.sh can
    unconditionally reference it). Loads as an empty dict."""
    run_options = {"cores-per-worker": 8, "queue": "workday"}
    path = write_inner_run_options(str(tmp_path), run_options)
    assert os.path.isfile(path)
    loaded = yaml.safe_load(open(path).read())
    assert loaded in (None, {})


def test_whitelist_is_expected_set():
    """Pin the whitelist so future widening is intentional."""
    assert set(INNER_RUN_OPTIONS_WHITELIST) == {"skip-bad-files", "tree-reduction"}


# ----------------------- ensure_job_sh_forwards_inner_yaml -----------------------

LEGACY_WRAPPER = """#!/bin/bash
JOBDIR=/somewhere
run_with_retries() { :; }

rm -f $JOBDIR/job_$1.idle
echo "Starting job $1"
touch $JOBDIR/job_$1.running
pocket-coffea run --cfg $2 -o output --executor iterative --chunksize $3
echo done
"""


def test_job_sh_patch_idempotent(tmp_path):
    job_sh = tmp_path / "job.sh"
    job_sh.write_text(LEGACY_WRAPPER)

    # First call patches.
    assert ensure_job_sh_forwards_inner_yaml(str(job_sh)) is True
    patched = job_sh.read_text()
    assert "--custom-run-options inner_run_options.yaml" in patched
    # Original chunksize arg is preserved with the new flag *after* it
    assert "--chunksize $3 --custom-run-options inner_run_options.yaml" in patched

    # Second call is a no-op.
    assert ensure_job_sh_forwards_inner_yaml(str(job_sh)) is False
    assert job_sh.read_text() == patched


def test_job_sh_patch_missing_file_returns_false(tmp_path):
    assert ensure_job_sh_forwards_inner_yaml(str(tmp_path / "missing.sh")) is False


# ----------------------- ensure_sub_transfers_inner_yaml -----------------------

LEGACY_SUB = """Executable = job.sh
arguments = $(ProcId) config_job_$(ProcId).pkl $(chunksize)
transfer_input_files = /abs/jobdir/config_job_$(ProcId).pkl,/abs/proxy,/abs/jobdir/job.sh
should_transfer_files = YES
queue
"""


def test_sub_patch_appends_yaml(tmp_path):
    sub = tmp_path / "job_0.sub"
    sub.write_text(LEGACY_SUB)

    assert ensure_sub_transfers_inner_yaml(str(sub), "/abs/jobdir") is True
    patched = sub.read_text()
    expected_target = "/abs/jobdir/inner_run_options.yaml"
    assert expected_target in patched
    # Pre-existing entries kept on the same transfer_input_files line.
    for prev in (
        "/abs/jobdir/config_job_$(ProcId).pkl",
        "/abs/proxy",
        "/abs/jobdir/job.sh",
    ):
        assert prev in patched


def test_sub_patch_idempotent(tmp_path):
    sub = tmp_path / "job_0.sub"
    sub.write_text(LEGACY_SUB)
    ensure_sub_transfers_inner_yaml(str(sub), "/abs/jobdir")
    first = sub.read_text()
    # Second call must not modify.
    assert ensure_sub_transfers_inner_yaml(str(sub), "/abs/jobdir") is False
    assert sub.read_text() == first


def test_sub_patch_missing_file_returns_false(tmp_path):
    assert ensure_sub_transfers_inner_yaml(str(tmp_path / "missing.sub"), "/x") is False
