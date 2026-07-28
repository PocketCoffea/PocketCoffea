"""Offline tests for the generated HTCondor job.sh wrapper (no condor/EOS needed).

Importing executors_lxplus pulls in pocket_coffea.utils.rucio (which imports the rucio
package at module load); stub it when unavailable so this stays offline.
"""
import sys
import types


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

from pocket_coffea.executors.executors_lxplus import build_job_script  # noqa: E402


def _script(split_by_category=False, cores=1):
    return build_job_script(
        env_extras="export FOO=bar",
        abs_jobdir_path="/abs/jobs",
        abs_output_path="/abs/out",
        copy_command="xrdcp -f",
        runnercmd="pocket-coffea run",
        inner_yaml_basename="inner.yaml",
        split_by_category=split_by_category,
        cores_per_worker=cores,
    )


def test_flag_files_use_jobid_not_positional_one():
    script = _script()
    # The job id is captured once and used everywhere, including inside run_with_retries.
    assert 'JOBID="$1"' in script
    assert "job_$JOBID.failed" in script
    assert "job_$JOBID.running" in script
    assert "job_$JOBID.idle" in script
    assert "job_$JOBID.done" in script
    # The old bug: bare $1 inside the flag-file names.
    assert "job_$1." not in script


def test_non_split_copies_from_job_local_output():
    script = _script(split_by_category=False)
    assert "output/output_all.coffea /abs/out/output_job_$JOBID.coffea" in script
    assert "EXECUTOR" not in script  # placeholder was substituted
    assert "--executor iterative" in script


def test_split_by_category_runs_in_output_and_is_exit_checked():
    script = _script(split_by_category=True, cores=4)
    # Runs the split inside the job-local output/ dir, not the shared final dir.
    assert "cd output ||" in script
    assert "split-output output_all.coffea" in script
    # Split/copy failures mark the job failed rather than falling through to .done.
    assert "split-output failed" in script
    assert "${f%.coffea}_job_$JOBID.coffea" in script
    # multi-core -> futures executor with scaleout.
    assert "--executor futures --scaleout 4" in script
