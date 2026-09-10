"""Focused current-format proactive recreation tests."""
import json
import time

import pytest
import yaml
from click.testing import CliRunner

from pocket_coffea.scripts import check_jobs


def make_jobs(tmp_path, executor="condor@lxplus"):
    (tmp_path / "logs").mkdir()
    submission = {
        "format_version": 1,
        "executor": executor,
        "requires_grid_certificate": False,
        "proxy_transfer_path": None,
        "proxy_source": None,
    }
    state = {"0": {"chunksize": 100, "request_cpus": 1,
                    "request_memory": "4GB", "resubmissions": 0}}
    if executor == "condor@lxplus":
        state["0"].update({"queue": "espresso", "resources_scaled": False})
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({
        "submission": submission, "jobs_list": {"job_0": {"filesets": {}}},
    }))
    (tmp_path / "job_state.json").write_text(json.dumps(state))
    for name in ("job.sh", "inner_run_options.yaml"):
        (tmp_path / name).write_text("")
    (tmp_path / "resubmit.sub").write_text(
        "RequestCpus = $(CPUS)\nRequestMemory = $(MEMORY)\n"
        "arguments = $(PROC) $(CHUNKSIZE) $(CPUS)\n"
    )
    (tmp_path / "config_job_0.pkl").write_bytes(b"placeholder")
    (tmp_path / "job_0.running").touch()


class Config:
    def __init__(self):
        self.filesets = {}

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def test_current_contract_is_accepted(tmp_path):
    make_jobs(tmp_path)
    assert check_jobs.load_current_contract(tmp_path)[2]["executor"] == "condor@lxplus"
    assert not (tmp_path / "job_0.sub").exists()


def test_recreate_skips_active_job_that_finishes_during_preparation(tmp_path, monkeypatch):
    make_jobs(tmp_path)

    def finish_job(folder, job):
        (tmp_path / f"{job}.running").unlink()
        (tmp_path / f"{job}.done").touch()
        return None

    monkeypatch.setattr(check_jobs, "latest_job_out", finish_job)
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda *args: (_ for _ in ()).throw(AssertionError()))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "auto", remove_running=True)
    assert result["skipped"] == ["job_0"]
    assert (tmp_path / "job_0.done").exists()


def test_passive_check_jobs_does_not_mutate_markers_or_state(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    stamp = time.strftime("%m/%d %H:%M:%S", time.localtime(time.time() + 2))
    (tmp_path / "logs" / "job_123.log").write_text(
        f"009 (123.0.000) {stamp} Job was aborted.\n"
        "Job removed by SYSTEM_PERIODIC_REMOVE due to wall time exceeded.\n"
    )
    snapshots = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (tmp_path / "job_state.json", tmp_path / "resubmit.sub")
    }
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda *args: (_ for _ in ()).throw(AssertionError()))
    result = CliRunner().invoke(
        check_jobs.check_jobs, ["-j", str(tmp_path), "--once", "--by", "none"])
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "job_0.failed").exists()
    assert not (tmp_path / ".check_jobs.lock").exists()
    for path, snapshot in snapshots.items():
        assert (path.read_bytes(), path.stat().st_mtime_ns) == snapshot


def test_recreate_prepares_before_condor_rm(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").touch()
    config_path = tmp_path / "config_job_0.pkl"
    import cloudpickle
    with config_path.open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    jobs_config = yaml.safe_load((tmp_path / "jobs_config.yaml").read_text())
    jobs_config["jobs_list"]["job_0"]["filesets"] = {
        "dataset": {"files": ["file.root"], "metadata": {"nevents": 1}}
    }
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump(jobs_config))
    order = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs",
                        lambda folder: order.append("proxy"))
    monkeypatch.setattr(check_jobs, "condor_rm_job",
                        lambda job: (order.append("rm") or (True, "")))
    monkeypatch.setattr(check_jobs, "wait_for_condor_job_removal",
                        lambda job: (order.append("wait") or True))
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda folder, sub: (order.append("submit") or (True, "")))
    result = check_jobs.recreate_jobs_oneshot(
        tmp_path, "0", remove_running=True)
    assert result["submitted"] == ["job_0"]
    assert order == ["proxy", "rm", "wait", "submit"]
    assert json.loads((tmp_path / "job_state.json").read_text())["0"]["resubmissions"] == 1


def test_recreate_proxy_failure_leaves_active_job(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs",
                        lambda folder: (_ for _ in ()).throw(RuntimeError("proxy")))
    removed = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0", remove_running=True)
    assert "job_0" in result["failed"]
    assert removed == []
    assert (tmp_path / "job_0.running").exists()


def test_failed_recreate_restores_canonical_config(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    (tmp_path / "job_0.done").touch()
    import cloudpickle
    config = Config()
    config.filesets = {"original": {"files": ["old.root"]}}
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(config, handle)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (False, "submit failed"))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert "job_0" in result["failed"]
    assert (tmp_path / "job_0.done").exists()
    assert not (tmp_path / "job_0.failed").exists()
    assert json.loads((tmp_path / "job_state.json").read_text())["0"]["resubmissions"] == 0
    with (tmp_path / "config_job_0.pkl").open("rb") as handle:
        assert cloudpickle.load(handle).filesets == {"original": {"files": ["old.root"]}}


def test_reactive_marker_failure_stops_after_committing_retry(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    state = json.loads((tmp_path / "job_state.json").read_text())
    log_text = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs", lambda folder: None)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (True, ""))
    monkeypatch.setattr(check_jobs, "mark_job_idle",
                        lambda *args: (_ for _ in ()).throw(OSError("local write")))
    with pytest.raises(RuntimeError, match="accepted the replacement"):
        check_jobs.submit_resubmit_jobs(
            tmp_path, ["0"], state, tmp_path / "job_state.json", log_text)
    assert json.loads((tmp_path / "job_state.json").read_text())["0"]["resubmissions"] == 1


def test_successful_recreate_marker_failure_stops_after_submission(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    import cloudpickle
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (True, ""))
    monkeypatch.setattr(check_jobs, "mark_job_idle",
                        lambda *args: (_ for _ in ()).throw(OSError("marker write")))
    with pytest.raises(RuntimeError, match="accepted the replacement"):
        check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert json.loads((tmp_path / "job_state.json").read_text())["0"]["resubmissions"] == 1
