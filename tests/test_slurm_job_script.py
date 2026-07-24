"""Offline tests for the generated SLURM job.sh wrapper and sbatch scripts
(no SLURM/EOS needed). The T3_CH_PSI executor module only needs coffea at
import time (no rucio)."""
import pytest

pytest.importorskip("coffea")

from pocket_coffea.executors.executors_T3_CH_PSI import (  # noqa: E402
    _slurm_mem,
    build_job_script_slurm,
    build_sbatch_script,
)


def _script(split_by_category=False, cores=1, **kwargs):
    return build_job_script_slurm(
        env_extras="export FOO=bar",
        abs_jobdir_path="/abs/jobs",
        abs_output_path="/abs/out",
        runnercmd="pocket-coffea run",
        inner_yaml_basename="inner.yaml",
        split_by_category=split_by_category,
        cores_per_worker=cores,
        **kwargs,
    )


# ------------------------------------------------------------------- job.sh

def test_flag_files_use_jobid_not_positional_one():
    script = _script()
    assert 'JOBID="$1"' in script
    assert "job_$JOBID.failed" in script
    assert "job_$JOBID.running" in script
    assert "job_$JOBID.idle" in script
    assert "job_$JOBID.done" in script
    assert "job_$1." not in script


def test_shared_fs_paths_are_absolute():
    script = _script()
    # config pickle and inner yaml are referenced inside the jobs_dir, no transfer
    assert 'CONFIGPKL="$JOBDIR/$2"' in script
    assert "--custom-run-options $JOBDIR/inner.yaml" in script
    assert "JOBDIR=/abs/jobs" in script


def test_runs_in_node_local_scratch_with_trap_cleanup():
    script = _script()
    assert "/scratch/$USER/pocketcoffea_" in script
    assert "trap 'rm -rf \"$WORKDIR\"' EXIT" in script
    assert 'cd "$WORKDIR"' in script


def test_custom_scratch_dir():
    script = _script(local_scratch_dir="/tmp/myscratch")
    assert "/tmp/myscratch/pocketcoffea_" in script


def test_non_split_copies_from_job_local_output():
    script = _script(split_by_category=False)
    assert 'run_with_retries "cp output/output_all.coffea /abs/out/output_job_$JOBID.coffea"' in script
    assert "EXECUTOR" not in script
    assert "--executor iterative" in script


def test_split_by_category_runs_in_output_and_is_exit_checked():
    script = _script(split_by_category=True, cores=4)
    assert "cd output || fail" in script
    assert "split-output output_all.coffea" in script
    assert "${f%.coffea}_job_$JOBID.coffea" in script
    assert "--executor futures --scaleout 4" in script


# ---------------------------------------------------------------- _slurm_mem

@pytest.mark.parametrize("raw,expected", [
    ("4GB", "4G"),
    ("4G", "4G"),
    ("4000M", "4000M"),
    ("4000", "4000"),
    (4000, "4000"),
    ("2 GB", "2G"),
])
def test_slurm_mem_normalization(raw, expected):
    assert _slurm_mem(raw) == expected


def test_slurm_mem_passthrough_when_unparseable():
    assert _slurm_mem("0.5T") == "0.5T"


# ---------------------------------------------------------- sbatch scripts

def _sbatch(**kwargs):
    defaults = dict(
        slurm_job_name="out_job_5",
        partition="standard",
        walltime="06:00:00",
        cores_per_worker=1,
        mem_per_worker="4GB",
        abs_jobdir_path="/abs/jobs",
        log_stem="logs/job_%j.5",
        command="bash job.sh 5 config_job_5.pkl 150000",
    )
    defaults.update(kwargs)
    return build_sbatch_script(**defaults)


def test_sbatch_headers():
    script = _sbatch()
    assert script.startswith("#!/bin/bash\n")
    assert "#SBATCH --job-name=out_job_5" in script
    assert "#SBATCH --partition=standard" in script
    assert "#SBATCH --time=06:00:00" in script
    assert "#SBATCH --cpus-per-task=1" in script
    assert "#SBATCH --mem=4G" in script            # "4GB" normalized
    assert "#SBATCH --chdir=/abs/jobs" in script
    assert "#SBATCH --output=logs/job_%j.5.out" in script
    assert "#SBATCH --error=logs/job_%j.5.err" in script
    assert "bash job.sh 5 config_job_5.pkl 150000" in script
    # optional bits absent by default
    assert "--account" not in script
    assert "--array" not in script


def test_sbatch_account_and_array():
    script = _sbatch(account="t3", array="0-99%50", log_stem="logs/job_%A.%a")
    assert "#SBATCH --account=t3" in script
    assert "#SBATCH --array=0-99%50" in script
    assert "#SBATCH --output=logs/job_%A.%a.out" in script


def test_sbatch_custom_options_with_and_without_prefix():
    script = _sbatch(custom_sbatch_options=["--constraint=el9",
                                            "#SBATCH --exclude=wn01"])
    assert "#SBATCH --constraint=el9" in script
    assert "#SBATCH --exclude=wn01" in script


# ------------------------------------------- end-to-end dry-run submission

from pocket_coffea.executors.executors_T3_CH_PSI import (  # noqa: E402
    ExecutorFactorySlurmPSI,
    get_executor_factory,
)


class FakeConfigurator:
    """Minimal stand-in for pocket_coffea Configurator (cloudpickle-able)."""

    def __init__(self):
        self.filesets = {}
        self.do_postprocessing = True

    def clone(self):
        return FakeConfigurator()

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def _filesets(nevents=300):
    return {
        "datasetA": {
            "files": ["root://x//store/a1.root", "root://x//store/a2.root"],
            "metadata": {"sample": "A", "nevents": str(nevents)},
        }
    }


def _run_options(tmp_path, **overrides):
    opts = {
        "ignore-grid-certificate": True,
        "jobs-dir": str(tmp_path / "jobs"),
        "queue": "standard",
        "walltime": "06:00:00",
        "cores-per-worker": 1,
        "mem-per-worker": "4GB",
        "chunksize": 100,
        "max-events-per-job": 150,
        "split-by-category": False,
        "dry-run": True,
    }
    opts.update(overrides)
    return opts


def _submit(tmp_path, **overrides):
    outputdir = tmp_path / "out"
    outputdir.mkdir(exist_ok=True)
    factory = get_executor_factory(
        "slurm", run_options=_run_options(tmp_path, **overrides),
        outputdir=str(outputdir))
    assert isinstance(factory, ExecutorFactorySlurmPSI)
    factory.submit(FakeConfigurator(), _filesets(), str(outputdir))
    return tmp_path / "jobs" / "job"


def test_dry_run_submission_writes_all_job_files(tmp_path):
    import yaml
    jobs_dir = _submit(tmp_path)
    # 300 events / 150 per job -> 2 jobs
    for i in (0, 1):
        assert (jobs_dir / f"job_{i}.slurm").exists()
        assert (jobs_dir / f"job_{i}.idle").exists()
        assert (jobs_dir / f"config_job_{i}.pkl").exists()
    assert not (jobs_dir / "job_2.slurm").exists()
    assert (jobs_dir / "job.sh").exists()
    assert (jobs_dir / "inner_run_options.yaml").exists()
    assert (jobs_dir / "chunksizes.txt").read_text() == "100\n100\n"
    jobs_config = yaml.safe_load((jobs_dir / "jobs_config.yaml").read_text())
    assert jobs_config["scheduler"] == "slurm"
    assert sorted(jobs_config["jobs_list"]) == ["job_0", "job_1"]
    array = (jobs_dir / "jobs_all.slurm").read_text()
    assert "#SBATCH --array=0-1" in array
    assert "config_job_${SLURM_ARRAY_TASK_ID}.pkl" in array
    assert 'sed -n "$((SLURM_ARRAY_TASK_ID+1))p" chunksizes.txt' in array
    single = (jobs_dir / "job_1.slurm").read_text()
    assert "bash job.sh 1 config_job_1.pkl 100" in single
    assert "#SBATCH --partition=standard" in single
    assert "#SBATCH --output=logs/job_%j.1.out" in single


def test_dry_run_submission_apptainer_wrap_and_throttle(tmp_path):
    jobs_dir = _submit(
        tmp_path,
        **{"worker-image": "/cvmfs/img", "apptainer-binds": "/scratch,/pnfs",
           "max-concurrent-jobs": 10})
    array = (jobs_dir / "jobs_all.slurm").read_text()
    assert "#SBATCH --array=0-1%10" in array
    assert "apptainer exec -B /scratch -B /pnfs /cvmfs/img bash job.sh" in array
    single = (jobs_dir / "job_0.slurm").read_text()
    assert "apptainer exec -B /scratch -B /pnfs /cvmfs/img bash job.sh 0" in single
