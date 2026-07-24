"""Offline tests for check-jobs' SLURM support (no SLURM/condor/EOS needed).

Mirrors test_check_jobs_recreate.py for a jobs_dir created by the manual SLURM
executor (jobs_config.yaml carrying ``scheduler: slurm`` + job_<n>.slurm files):
the scheduler is auto-detected, resubmission goes through sbatch, queue bumping
rewrites --partition/--time, and the babysitter loop classifies slurmstepd
timeout/OOM kills from the per-job .err logs.

Importing check_jobs pulls in pocket_coffea.utils.rucio (imports the rucio
package); stub it when unavailable so this stays offline.
"""
import sys
import types

import pytest
import yaml
import cloudpickle
from click.testing import CliRunner


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

from pocket_coffea.scripts import check_jobs  # noqa: E402
from pocket_coffea.scripts.check_jobs import check_jobs as check_jobs_cmd  # noqa: E402
from pocket_coffea.utils.slurm_queue import PARTITION_WALLTIMES  # noqa: E402


SITEA = "root://siteA.example//"
SITEMAP = {"T2_X_SITEA": SITEA}


class FakeConfigurator:
    def __init__(self, filesets):
        self.filesets = filesets

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def _fs(files):
    return {"datasetA": {"files": list(files), "metadata": {"sample": "A", "nevents": "1"}}}


SLURM_TEMPLATE = """#!/bin/bash
#SBATCH --job-name=out_{jn}
#SBATCH --partition={partition}
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --chdir={jobdir}
#SBATCH --output=logs/job_%j.{idx}.out
#SBATCH --error=logs/job_%j.{idx}.err

bash job.sh {idx} config_{jn}.pkl 150000
"""


def _make_slurm_jobs_dir(tmp_path, jobs):
    """Synthetic jobs_dir as written by ExecutorFactorySlurmPSI.

    `jobs` maps job_name -> {"filesets": ..., "flag": failed|running|idle|None,
    "partition": ...}.
    """
    d = tmp_path / "job"
    d.mkdir()
    (d / "logs").mkdir()
    jobs_list = {}
    for jn, spec in jobs.items():
        idx = jn.split("_")[-1]
        cfg = d / f"config_{jn}.pkl"
        with open(cfg, "wb") as f:
            cloudpickle.dump(FakeConfigurator({}), f)
        jobs_list[jn] = {
            "filesets": spec["filesets"],
            "config_file": str(cfg),
            "output_file": str(d / f"output_{jn}.coffea"),
        }
        (d / f"{jn}.slurm").write_text(SLURM_TEMPLATE.format(
            jn=jn, idx=idx, jobdir=d, partition=spec.get("partition", "standard")))
        flag = spec.get("flag")
        if flag:
            (d / f"{jn}.{flag}").write_text("")
    (d / "jobs_config.yaml").write_text(yaml.safe_dump({
        "job_name": "job",
        "job_dir": str(d),
        "scheduler": "slurm",
        "jobs_list": jobs_list,
    }))
    (d / "job.sh").write_text(
        "pocket-coffea run --cfg $CONFIGPKL -o output --executor iterative "
        "--chunksize $CHUNKSIZE --custom-run-options $JOBDIR/inner_run_options.yaml\n"
    )
    return d


@pytest.fixture
def sitemap(monkeypatch):
    monkeypatch.setattr(check_jobs, "get_xrootd_sites_map", lambda: SITEMAP)
    monkeypatch.setattr(check_jobs, "get_rucio_client", lambda: None)


# --------------------------------------------------- recreate (one-shot) pass

def test_recreate_detects_slurm_and_resubmits_with_sbatch(tmp_path, sitemap, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    calls = []
    monkeypatch.setattr(check_jobs.os, "system", lambda cmd: calls.append(cmd) or 0)
    # no explicit scheduler: must be detected from jobs_config.yaml
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=False)
    joined = "\n".join(calls)
    assert "sbatch job_0.slurm" in joined
    assert "condor_submit" not in joined
    assert "touch" in joined and "job_0.idle" in joined


def test_recreate_running_slurm_job_gets_partition_bump(tmp_path, sitemap):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "running", "partition": "standard"}})
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True, dry_run=True)
    content = (d / "job_0.slurm").read_text()
    assert "#SBATCH --partition=long" in content
    assert f"#SBATCH --time={PARTITION_WALLTIMES['long']}" in content


def test_recreate_queue_forces_slurm_partition(tmp_path, sitemap):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "failed", "partition": "quick"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     recreate_queue="long", dry_run=True)
    assert "#SBATCH --partition=long" in (d / "job_0.slurm").read_text()


def test_recreate_remove_running_uses_scancel(tmp_path, sitemap, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "running"},
        "job_1": {"filesets": _fs([f]), "flag": "failed"},
    })
    cancelled = []
    monkeypatch.setattr(check_jobs.slurm_queue, "scancel_job",
                        lambda slurm_file: cancelled.append(slurm_file) or "cancelled")
    monkeypatch.setattr(check_jobs.os, "system", lambda cmd: 0)
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True,
                                     remove_running=True, dry_run=False)
    # only the running job is scancel'd, matched via its own .slurm file
    assert cancelled == [str(d / "job_0.slurm")]


def test_recreate_skip_bad_files_on_slurm_skips_sub_patch(tmp_path, sitemap):
    pytest.importorskip("coffea")  # write_inner_run_options lives next to the executor
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     skip_bad_files=True, dry_run=True)
    inner = d / "inner_run_options.yaml"
    assert inner.exists()
    assert yaml.safe_load(inner.read_text()).get("skip-bad-files") is True
    # the slurm wrapper already forwards the yaml by absolute path; no
    # transfer_input_files patch must be attempted on the .slurm file
    assert "transfer_input_files" not in (d / "job_0.slurm").read_text()


# ------------------------------------------------- babysitter loop (--once)

def test_loop_flags_timeout_and_bumps_partition(tmp_path):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "running", "partition": "standard"}})
    (d / "logs" / "job_9999.0.err").write_text(
        "slurmstepd: error: *** JOB 9999 ON wn01 CANCELLED AT "
        "2026-01-01T00:00:00 DUE TO TIME LIMIT ***\n")
    result = CliRunner().invoke(
        check_jobs_cmd, ["-j", str(d), "--once", "--by", "none"])
    assert result.exit_code == 0, result.output
    assert not (d / "job_0.running").exists()
    assert (d / "job_0.failed").exists()
    content = (d / "job_0.slurm").read_text()
    assert "#SBATCH --partition=long" in content
    # slurm job id + index recorded so the log is not re-processed
    assert "9999_0" in (d / "maxtime.txt").read_text()
    # log parked away from the live logs dir
    assert not (d / "logs" / "job_9999.0.err").exists()
    assert (d / "logs" / "processedlogs" / "job_9999.0.err").exists()


def test_loop_flags_oom_and_bumps_mem(tmp_path):
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "running"}})
    (d / "logs" / "job_4242.0.err").write_text(
        "slurmstepd: error: Detected 1 oom-kill event(s) in StepId=4242.batch.\n")
    result = CliRunner().invoke(
        check_jobs_cmd, ["-j", str(d), "--once", "--by", "none"])
    assert result.exit_code == 0, result.output
    assert (d / "job_0.failed").exists()
    assert "#SBATCH --mem=8G" in (d / "job_0.slurm").read_text()


def test_loop_ordinary_failure_is_not_reclassified(tmp_path):
    # a genuine job failure (flag .failed, no slurmstepd marker) must not
    # trigger the timeout/OOM machinery
    f = SITEA + "/store/data/foo.root"
    d = _make_slurm_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "failed"}})
    (d / "logs" / "job_777.0.err").write_text("Traceback ...\nValueError: boom\n")
    result = CliRunner().invoke(
        check_jobs_cmd, ["-j", str(d), "--once", "--by", "none"])
    assert result.exit_code == 0, result.output
    # untouched: partition/mem stay, log stays in place
    content = (d / "job_0.slurm").read_text()
    assert "#SBATCH --partition=standard" in content
    assert "#SBATCH --mem=4G" in content
    assert (d / "logs" / "job_777.0.err").exists()
