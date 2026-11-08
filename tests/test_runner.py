import os
from pathlib import Path
import pytest
import json


@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent


def test_runner_local(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Test the runner script"""
    monkeypatch.chdir(base_path / "test_full_configs/test_subsamples/")
    outputdir = tmp_path_factory.mktemp("test_runner")
    status = os.system(f"pocket-coffea run --cfg config_subsamples.py -o {outputdir} --test -lf 1 -lc 1 --chunksize 100")
    assert status == 0
    assert (outputdir / "output_all.coffea").exists()


def test_failed_jobs_tracking(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Test that failed jobs are tracked when using --process-separately"""
    monkeypatch.chdir(base_path / "test_full_configs/test_subsamples/")
    outputdir = tmp_path_factory.mktemp("test_failed_jobs")
    
    # Run with process-separately to enable failed job tracking
    # Using --test and limiting files to ensure quick execution
    status = os.system(f"pocket-coffea run --cfg config_subsamples.py -o {outputdir} --test -lf 1 -lc 1 --chunksize 100 --process-separately")
    
    # The test should succeed (status 0 means no Python errors)
    assert status == 0
    
    # Check if failed_jobs.json exists (it might not exist if all jobs succeeded)
    failed_jobs_file = outputdir / "failed_jobs.json"
    # If the file doesn't exist, all jobs succeeded, which is also valid
    if failed_jobs_file.exists():
        with open(failed_jobs_file, 'r') as f:
            failed_jobs = json.load(f)
        # failed_jobs should be a list
        assert isinstance(failed_jobs, list)


def test_resubmit_failed_without_process_separately(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Test that --resubmit-failed requires --process-separately"""
    monkeypatch.chdir(base_path / "test_full_configs/test_subsamples/")
    outputdir = tmp_path_factory.mktemp("test_resubmit_error")
    
    # Try to use --resubmit-failed without --process-separately
    status = os.system(f"pocket-coffea run --cfg config_subsamples.py -o {outputdir} --test --resubmit-failed 2>/dev/null")
    
    # Should fail (non-zero exit code)
    assert status != 0


def test_resubmit_failed_without_file(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Test that --resubmit-failed fails when failed_jobs.json doesn't exist"""
    monkeypatch.chdir(base_path / "test_full_configs/test_subsamples/")
    outputdir = tmp_path_factory.mktemp("test_resubmit_no_file")
    
    # Try to use --resubmit-failed without having a failed_jobs.json file
    status = os.system(f"pocket-coffea run --cfg config_subsamples.py -o {outputdir} --test --process-separately --resubmit-failed 2>/dev/null")
    
    # Should fail (non-zero exit code)
    assert status != 0




def test_timeit_worker_count_is_executor_concurrency_not_dataset_count():
    from pocket_coffea.scripts.runner import _executor_worker_count

    assert _executor_worker_count("iterative", {"scaleout": 10}) == 1
    assert _executor_worker_count("futures", {"scaleout": 3}) == 3
    assert _executor_worker_count("iterative", {"scaleout": 1}) == 1


def test_local_timeit_chunksize_mapping_resolves_dataset_sample_and_default():
    from pocket_coffea.scripts.runner import _resolve_local_chunksize

    filesets = {
        "TT_2023": {"metadata": {"sample": "TT"}},
        "DY_2023": {"metadata": {"sample": "DY"}},
        "W_2023": {"metadata": {"sample": "W"}},
    }
    assert _resolve_local_chunksize(
        {"TT_2023": 100_000, "DY": 50_000, "default": 150_000}, filesets
    ) == 150_000


def test_local_timeit_chunksize_mapping_requires_a_fallback():
    from pocket_coffea.scripts.runner import _resolve_local_chunksize

    with pytest.raises(Exception, match="no entry"):
        _resolve_local_chunksize(
            {"other": 100}, {"TT_2023": {"metadata": {"sample": "TT"}}}
        )
