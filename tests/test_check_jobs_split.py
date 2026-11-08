"""check-jobs --split-jobs: split failed/timed-out jobs at file level."""
import json

import cloudpickle
import pytest
import yaml
from click.testing import CliRunner

from pocket_coffea.scripts import check_jobs
from pocket_coffea.utils.job_split import split_fileset


class Config:
    def __init__(self, filesets):
        self.filesets = filesets

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def fileset(nfiles, dataset="DY_2022"):
    return {dataset: {"files": [f"root://site//store/{dataset}/{i}.root" for i in range(nfiles)],
                      "metadata": {"sample": "DY", "nevents": "1000"}}}


# job -> (number of files, marker)
JOBS = {0: (2, "done"), 3: (7, "failed"), 4: (1, "failed"), 5: (4, "running"), 6: (3, "timeout")}


@pytest.fixture
def submitted(monkeypatch):
    subs = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs", lambda folder: None)
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda folder, sub: subs.append((folder / sub).read_text()) or (True, ""))
    return subs


@pytest.fixture
def jobs_dir(tmp_path, submitted):
    (tmp_path / "logs").mkdir()
    submission = {"format_version": 1, "executor": "condor@lxplus",
                  "requires_grid_certificate": False, "proxy_transfer_path": None,
                  "proxy_source": None}
    state = {}
    jobs_list = {}
    for k, (nfiles, marker) in JOBS.items():
        state[str(k)] = {"chunksize": 100, "request_cpus": 2, "request_memory": "4GB",
                         "resubmissions": 3, "queue": "tomorrow", "resources_scaled": True}
        jobs_list[f"job_{k}"] = {"filesets": fileset(nfiles)}
        with open(tmp_path / f"config_job_{k}.pkl", "wb") as f:
            cloudpickle.dump(Config(fileset(nfiles)), f)
        (tmp_path / f"job_{k}.{marker}").touch()
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump(
        {"submission": submission, "output_dir": "/out", "jobs_list": jobs_list}))
    (tmp_path / "job_state.json").write_text(json.dumps(state))
    for name in ("job.sh", "inner_run_options.yaml"):
        (tmp_path / name).write_text("")
    (tmp_path / "resubmit.sub").write_text("arguments = $(PROC) $(CHUNKSIZE) $(CPUS)\n")
    return tmp_path


def load(jobs_dir):
    return (yaml.safe_load((jobs_dir / "jobs_config.yaml").read_text()),
            json.loads((jobs_dir / "job_state.json").read_text()))


def test_split_fileset_keeps_all_files_and_metadata():
    fs = {**fileset(3, "A"), **fileset(2, "B")}
    chunks = split_fileset(fs, 2)
    assert [sorted(c) for c in chunks] == [["A"], ["A", "B"]]
    assert [f for c in chunks for d in c.values() for f in d["files"]] == fs["A"]["files"] + fs["B"]["files"]
    assert all(d["metadata"] is fs[name]["metadata"] for c in chunks for name, d in c.items())
    assert len(split_fileset(fileset(3), 10)) == 3
    with pytest.raises(ValueError):
        split_fileset(fileset(1), 4)


def test_split_all(jobs_dir, submitted):
    result = check_jobs.split_jobs_oneshot(jobs_dir, "all", 3)
    assert result["split"] == {"job_3": ["job_7", "job_8", "job_9"],
                               "job_6": ["job_10", "job_11", "job_12"]}
    assert set(result["failed"]) == {"job_4"}  # 1 file: stays failed

    jobs_config, state = load(jobs_dir)
    assert sorted(state, key=int) == ["0", "4", "5", "7", "8", "9", "10", "11", "12"]
    assert state["7"] == {"chunksize": 100, "request_cpus": 2, "request_memory": "4GB",
                          "resubmissions": 0, "queue": "tomorrow", "resources_scaled": True}
    assert jobs_config["split_jobs"] == result["split"]
    assert "job_3" not in jobs_config["jobs_list"] and "job_6" not in jobs_config["jobs_list"]

    files = []
    for num in (7, 8, 9):
        job = f"job_{num}"
        fs = cloudpickle.load(open(jobs_dir / f"config_{job}.pkl", "rb")).filesets
        assert jobs_config["jobs_list"][job]["filesets"] == fs
        assert jobs_config["jobs_list"][job]["output_file"] == f"/out/output_{job}.coffea"
        assert (jobs_dir / f"{job}.idle").exists()
        files += fs["DY_2022"]["files"]
    assert files == fileset(7)["DY_2022"]["files"]

    for name in ("config_job_3.pkl", "job_3.failed", "config_job_6.pkl", "job_6.timeout"):
        assert not (jobs_dir / name).exists() and (jobs_dir / "split" / name).exists()
    # One submission with one row per new job.
    assert len(submitted) == 1
    assert [line.split()[0] for line in submitted[0].splitlines() if line.startswith("  ")] \
        == ["7", "8", "9", "10", "11", "12"]
    check_jobs.load_current_contract(jobs_dir)  # still a valid jobs folder


def test_split_refusals_and_no_id_reuse(jobs_dir):
    result = check_jobs.split_jobs_oneshot(jobs_dir, "5,99", 2)
    assert set(result["failed"]) == {"job_5", "job_99"} and not result["split"]
    assert not (jobs_dir / "split").exists()

    assert check_jobs.split_jobs_oneshot(jobs_dir, "6", 2)["split"] == {"job_6": ["job_7", "job_8"]}
    (jobs_dir / "job_8.idle").rename(jobs_dir / "job_8.failed")  # job_8 has 2 files
    # job_8 is the newest job: its id must not be reused.
    assert check_jobs.split_jobs_oneshot(jobs_dir, "job_8", 2)["split"] == {"job_8": ["job_9", "job_10"]}


def test_failed_submission_changes_nothing(jobs_dir, monkeypatch):
    before = {p.name: p.read_bytes() for p in jobs_dir.iterdir() if p.is_file()}
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda folder, sub: (False, "schedd down"))
    result = check_jobs.split_jobs_oneshot(jobs_dir, "all", 2)
    assert result["failed"]["job_3"] == "schedd down" and not result["split"]
    after = {p.name: p.read_bytes() for p in jobs_dir.iterdir() if p.is_file()}
    assert after == before


def test_cli_split_jobs(jobs_dir):
    out = CliRunner().invoke(check_jobs.check_jobs,
                             ["-j", str(jobs_dir), "--split-jobs", "3", "--split-factor", "2"])
    assert out.exit_code == 0, out.output
    assert load(jobs_dir)[0]["split_jobs"] == {"job_3": ["job_7", "job_8"]}
    out = CliRunner().invoke(check_jobs.check_jobs,
                             ["-j", str(jobs_dir), "--split-jobs", "3", "--recreate", "3"])
    assert out.exit_code != 0
