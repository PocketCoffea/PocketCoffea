"""Offline tests for the generated HTCondor job.sh wrapper (no condor/EOS needed).

Importing executors_lxplus pulls in pocket_coffea.utils.rucio (which imports the rucio
package at module load); stub it when unavailable so this stays offline.
"""
import os
import signal
import subprocess
import sys
import time
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

from pocket_coffea.executors.executors_lxplus import build_job_script  # noqa: E402


def _script(split_by_category=False, cores=1, timeit=False):
    return build_job_script(
        env_extras="export FOO=bar",
        abs_jobdir_path="/abs/jobs",
        abs_output_path="/abs/out",
        copy_command="xrdcp -f",
        runnercmd="pocket-coffea run",
        inner_yaml_basename="inner.yaml",
        split_by_category=split_by_category,
        cores_per_worker=cores,
        timeit=timeit,
        timeit_dir="/shared/timeit",
        timeit_copy_command="cp -f",
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


def test_timeout_and_xrootd_recovery_are_in_the_wrapper():
    script = _script()
    assert "trap cleanup TERM INT" in script
    assert "job_$JOBID.timeout" in script
    assert "MAX_XROOTD_REWRITES=10" in script
    assert "python -m pocket_coffea.scripts.rewrite_xrootd_site" in script
    cleanup = script.split("cleanup() {", 1)[1].split("trap cleanup", 1)[0]
    assert cleanup.index("terminate_child_group") < cleanup.index('touch "$JOBDIR/job_$JOBID.timeout"')
    assert 'wait "$child"' in script
    assert 'kill -KILL -- "-${child_pgid}"' in script


def test_non_split_copies_from_job_local_output():
    script = _script(split_by_category=False)
    assert "output/output_all.coffea /abs/out/output_job_$JOBID.coffea" in script
    assert 'EXECUTOR_ARGS=(--executor iterative)' in script
    assert 'if [ "$4" -gt 1 ]' in script


def test_split_by_category_runs_in_output_and_is_exit_checked():
    script = _script(split_by_category=True, cores=4)
    # Runs the split inside the job-local output/ dir, not the shared final dir.
    assert "cd output ||" in script
    assert "split-output output_all.coffea" in script
    # Split/copy failures mark the job failed rather than falling through to .done.
    assert "split-output failed" in script
    assert "${f%.coffea}_job_$JOBID.coffea" in script
    # Runtime CPU argument selects futures and carries the same scaleout value.
    assert 'EXECUTOR_ARGS=(--executor futures --scaleout "$4")' in script
    assert '"${EXECUTOR_ARGS[@]}"' in script


def test_timeit_forwards_worker_flag_and_copies_json_with_overwrite():
    script = _script(timeit=True, cores=4)
    assert "--timeit --_timeit-worker" in script
    assert 'EXECUTOR_ARGS=(--executor futures --scaleout "$4")' in script
    assert 'mkdir -p timeit' in script
    assert 'timeit/*.json' in script
    assert 'cp -f' in script
    assert '/shared/timeit/' in script


def test_timeout_cleanup_terminates_complete_local_process_group(tmp_path):
    helper = tmp_path / "worker.py"
    pids = tmp_path / "pids"
    helper.write_text(
        "import os, subprocess, sys, time\n"
        f"path = {str(pids)!r}\n"
        "children = [subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']) for _ in range(2)]\n"
        "open(path, 'w').write('\\n'.join([str(os.getpid())] + [str(p.pid) for p in children]))\n"
        "while True: time.sleep(1)\n"
    )
    script_path = tmp_path / "job.sh"
    script_path.write_text(build_job_script(
        env_extras="",
        abs_jobdir_path=str(tmp_path),
        abs_output_path=str(tmp_path),
        copy_command="cp",
        runnercmd=f"{sys.executable} {helper}",
        inner_yaml_basename="inner.yaml",
        split_by_category=False,
        cores_per_worker=1,
    ))
    script_path.chmod(0o755)
    (tmp_path / "inner.yaml").write_text("{}\n")
    process = subprocess.Popen(
        [str(script_path), "0", "config.pkl", "10", "1"],
        cwd=str(tmp_path),
        env={**os.environ, "_CONDOR_SCRATCH_DIR": str(tmp_path)},
        start_new_session=True,
    )
    for _ in range(50):
        if pids.exists():
            break
        time.sleep(0.1)
    recorded = [int(pid) for pid in pids.read_text().splitlines()]
    process.send_signal(signal.SIGTERM)
    for _ in range(100):
        if (tmp_path / "job_0.timeout").exists():
            break
        time.sleep(0.05)
    assert (tmp_path / "job_0.timeout").exists()
    assert not (tmp_path / "job_0.failed").exists()
    for pid in recorded:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    assert process.wait(timeout=10) == 0
    assert not (tmp_path / "job_0.running").exists()
    assert not (tmp_path / "job_0.idle").exists()
