"""Offline tests for the SLURM partition/walltime helpers (no SLURM needed).

pocket_coffea.utils.slurm_queue is dependency-free (glob/os/re/yaml only), so
these run on a bare CI runner.
"""
import yaml

from pocket_coffea.utils.slurm_queue import (
    PARTITIONS,
    PARTITION_WALLTIMES,
    bump_mem,
    bump_queue,
    classify_slurm_log,
    detect_scheduler,
    read_job_name,
    scancel_job,
    set_queue,
)
from pocket_coffea.utils import slurm_queue


SLURM_TEMPLATE = """#!/bin/bash
#SBATCH --job-name=out_job_1
#SBATCH --partition={partition}
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem={mem}
#SBATCH --chdir=/abs/jobs
#SBATCH --output=logs/job_%j.1.out
#SBATCH --error=logs/job_%j.1.err

bash job.sh 1 config_job_1.pkl 150000
"""


def _write_slurm(tmp_path, partition="quick", mem="4G"):
    p = tmp_path / "job_1.slurm"
    p.write_text(SLURM_TEMPLATE.format(partition=partition, mem=mem))
    return str(p)


# ---------------------------------------------------------------- bump_queue

def test_bump_moves_up_ladder_and_raises_time(tmp_path):
    f = _write_slurm(tmp_path, partition="quick")
    assert bump_queue(f) == "standard"
    content = open(f).read()
    assert "#SBATCH --partition=standard" in content
    assert f"#SBATCH --time={PARTITION_WALLTIMES['standard']}" in content


def test_bump_caps_at_longest_partition(tmp_path):
    f = _write_slurm(tmp_path, partition="long")
    assert bump_queue(f) == "long"
    assert "#SBATCH --partition=long" in open(f).read()


def test_bump_unknown_partition_jumps_to_longest(tmp_path):
    f = _write_slurm(tmp_path, partition="gpu")
    assert bump_queue(f) == PARTITIONS[-1]


def test_bump_without_partition_returns_none(tmp_path):
    p = tmp_path / "job_1.slurm"
    p.write_text("#!/bin/bash\n#SBATCH --time=01:00:00\nbash job.sh\n")
    assert bump_queue(str(p)) is None
    # untouched
    assert "#SBATCH --time=01:00:00" in p.read_text()


def test_bump_shift_two(tmp_path):
    f = _write_slurm(tmp_path, partition="quick")
    assert bump_queue(f, shift=2) == "long"


def test_bump_preserves_other_lines(tmp_path):
    f = _write_slurm(tmp_path, partition="quick")
    bump_queue(f)
    content = open(f).read()
    assert "bash job.sh 1 config_job_1.pkl 150000" in content
    assert "#SBATCH --output=logs/job_%j.1.out" in content


# ----------------------------------------------------------------- set_queue

def test_set_queue_forces_partition_and_time(tmp_path):
    f = _write_slurm(tmp_path, partition="quick")
    assert set_queue(f, "long") is True
    content = open(f).read()
    assert "#SBATCH --partition=long" in content
    assert f"#SBATCH --time={PARTITION_WALLTIMES['long']}" in content


def test_set_queue_noop_when_already_set(tmp_path):
    f = _write_slurm(tmp_path, partition="standard")
    assert set_queue(f, "standard") is False


def test_set_queue_without_partition_returns_false(tmp_path):
    p = tmp_path / "job_1.slurm"
    p.write_text("#!/bin/bash\nbash job.sh\n")
    assert set_queue(str(p), "long") is False


def test_set_queue_unknown_partition_written_verbatim_keeps_time(tmp_path):
    f = _write_slurm(tmp_path, partition="quick")
    assert set_queue(f, "gpu") is True
    content = open(f).read()
    assert "#SBATCH --partition=gpu" in content
    # no walltime known for "gpu" -> --time untouched
    assert "#SBATCH --time=06:00:00" in content


# ------------------------------------------------------------------ bump_mem

def test_bump_mem_doubles_suffixed_value(tmp_path):
    f = _write_slurm(tmp_path, mem="4G")
    assert bump_mem(f) == "8G"
    assert "#SBATCH --mem=8G" in open(f).read()


def test_bump_mem_plain_megabytes(tmp_path):
    f = _write_slurm(tmp_path, mem="4000")
    assert bump_mem(f) == "8000"


def test_bump_mem_without_mem_returns_none(tmp_path):
    p = tmp_path / "job_1.slurm"
    p.write_text("#!/bin/bash\n#SBATCH --partition=quick\nbash job.sh\n")
    assert bump_mem(str(p)) is None


# ------------------------------------------------------- classify_slurm_log

def test_classify_timeout(tmp_path):
    p = tmp_path / "job_123.1.err"
    p.write_text("slurmstepd: error: *** JOB 123 ON wn01 CANCELLED AT "
                 "2026-01-01T00:00:00 DUE TO TIME LIMIT ***\n")
    assert classify_slurm_log(str(p)) == "timeout"


def test_classify_oom(tmp_path):
    p = tmp_path / "job_123.1.err"
    p.write_text("slurmstepd: error: Detected 1 oom-kill event(s) in "
                 "StepId=123.batch. Some of your processes may have been "
                 "killed by the cgroup out-of-memory handler.\n")
    assert classify_slurm_log(str(p)) == "oom"


def test_classify_regular_failure_is_none(tmp_path):
    p = tmp_path / "job_123.1.err"
    p.write_text("Traceback (most recent call last):\nOSError: XRootD error\n")
    assert classify_slurm_log(str(p)) is None


def test_classify_missing_file_is_none(tmp_path):
    assert classify_slurm_log(str(tmp_path / "nope.err")) is None


# ------------------------------------------------------------ detect_scheduler

def test_detect_scheduler_from_jobs_config(tmp_path):
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({"scheduler": "slurm"}))
    assert detect_scheduler(tmp_path) == "slurm"


def test_detect_scheduler_from_slurm_files(tmp_path):
    # jobs_config without the key (pre-feature) but .slurm files present
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({"job_name": "job"}))
    (tmp_path / "job_0.slurm").write_text("#!/bin/bash\n")
    assert detect_scheduler(tmp_path) == "slurm"


def test_detect_scheduler_defaults_to_condor(tmp_path):
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({"job_name": "job"}))
    (tmp_path / "job_0.sub").write_text("queue\n")
    assert detect_scheduler(tmp_path) == "condor"


def test_detect_scheduler_empty_folder_defaults_to_condor(tmp_path):
    assert detect_scheduler(tmp_path) == "condor"


# -------------------------------------------------------- job name / scancel

def test_read_job_name(tmp_path):
    f = _write_slurm(tmp_path)
    assert read_job_name(f) == "out_job_1"


def test_scancel_job_matches_on_job_name(tmp_path, monkeypatch):
    f = _write_slurm(tmp_path)
    captured = {}

    class _R:
        def read(self):
            return "scancel: Terminating job 4242\n"

    def fake_popen(cmd):
        captured["cmd"] = cmd
        return _R()

    monkeypatch.setattr(slurm_queue.os, "popen", fake_popen)
    out = scancel_job(f)
    assert "scancel" in captured["cmd"]
    assert "--jobname=out_job_1" in captured["cmd"]
    assert "Terminating job" in out


def test_scancel_job_without_job_name_is_noop(tmp_path, monkeypatch):
    p = tmp_path / "job_1.slurm"
    p.write_text("#!/bin/bash\nbash job.sh\n")
    monkeypatch.setattr(slurm_queue.os, "popen",
                        lambda cmd: (_ for _ in ()).throw(AssertionError("should not shell out")))
    assert scancel_job(str(p)) == ""
