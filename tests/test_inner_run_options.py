"""Tests for the current-format inner-run-options file."""
import os
import yaml

from pocket_coffea.executors.executors_manual_jobs import (
    INNER_RUN_OPTIONS_FILENAME,
    INNER_RUN_OPTIONS_WHITELIST,
    write_inner_run_options,
)


def test_write_keeps_only_whitelisted_keys(tmp_path):
    options = {
        "skip-bad-files": True,
        "tree-reduction": 4,
        "cores-per-worker": 8,
        "mem-per-worker": "4GB",
        "worker-image": "/cvmfs/image",
        "queue": "workday",
        "chunksize": 200_000,
    }
    path = write_inner_run_options(str(tmp_path), options)
    assert os.path.basename(path) == INNER_RUN_OPTIONS_FILENAME
    loaded = yaml.safe_load(open(path).read())
    assert loaded == {"skip-bad-files": True, "tree-reduction": 4}
    assert not any(key in loaded for key in
                   ("cores-per-worker", "mem-per-worker", "worker-image", "queue", "chunksize"))


def test_write_drops_none_values(tmp_path):
    path = write_inner_run_options(str(tmp_path), {"skip-bad-files": True, "tree-reduction": None})
    assert yaml.safe_load(open(path).read()) == {"skip-bad-files": True}


def test_write_empty_when_nothing_whitelisted(tmp_path):
    path = write_inner_run_options(str(tmp_path), {"cores-per-worker": 8})
    assert os.path.isfile(path)
    assert yaml.safe_load(open(path).read()) in (None, {})


def test_whitelist_is_expected_set():
    assert set(INNER_RUN_OPTIONS_WHITELIST) == {"skip-bad-files", "tree-reduction"}
