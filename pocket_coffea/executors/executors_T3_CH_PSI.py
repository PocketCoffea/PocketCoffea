import os
import re
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from .executors_manual_jobs import (
    ExecutorFactoryManualABC,
    write_inner_run_options,
)
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask
import pocket_coffea
import cloudpickle
import yaml
from rich.progress import Progress


def get_worker_env(run_options, x509_path, exec_name="dask"):
    '''Worker-node environment setup commands, shared by the Dask (slurm-backed)
    and manual SLURM executors. `exec_name` is "dask" or "slurm".'''
    env_worker = [
        'export XRD_RUNFORKHANDLER=1',
        'export MALLOC_TRIM_THRESHOLD_=0',
        ]
    if exec_name == "dask":
        env_worker.append('ulimit -u unlimited')

    if not run_options['ignore-grid-certificate']:
        env_worker.append(f'export X509_USER_PROXY={x509_path}')

    # Adding list of custom setup commands from user defined run options
    if run_options.get("custom-setup-commands", None):
        env_worker += list(run_options["custom-setup-commands"])

    # Now checking for conda environment  conda-env:true
    if run_options.get("conda-env", False):
        env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')
        if "CONDA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['CONDA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        elif "MAMBA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['MAMBA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        else:
            raise Exception("CONDA prefix not found in env! Something is wrong with your conda installation if you want to use conda in the dask cluster.")

    # if local-virtual-env: true the job is configured to pickup
    # the local virtual environment (shared filesystem).
    if run_options.get("local-virtualenv", False):
        env_worker.append(f"source {sys.prefix}/bin/activate")
        if exec_name == "slurm":
            if os.getenv("PYTHONPATH"):
                pythonpath = os.getenv("PYTHONPATH")
            else:
                pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])
            env_worker.append(f"export PYTHONPATH={pythonpath}")

    return env_worker


class DaskExecutorFactory(ExecutorFactoryABC):
    '''
    At T3_CH_PSI the dask executor is based on slurm
    '''

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        return get_worker_env(self.run_options, getattr(self, "x509_path", None), "dask")


    def setup(self):
        ''' Start the DASK cluster here'''
        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        from dask_jobqueue import SLURMCluster
        setup_dask(dask.config)

        # Slurm cluster
        print(">>> Creating a SLURM cluster")
        self.dask_cluster = SLURMCluster(
                queue=self.run_options['queue'],
                cores=self.run_options.get('cores-per-worker', 1),
                processes=self.run_options.get('cores-per-worker', 1),
                memory=self.run_options['mem-per-worker'],
                walltime=self.run_options["walltime"],
                job_script_prologue=self.get_worker_env(),
                local_directory=os.path.join("/scratch", os.environ["USER"], "slurm_localdir"),
                log_directory=os.path.join(self.outputdir, "slurm_log"),
            )
        print(self.get_worker_env())

        #Cluster adaptive number of jobs only if requested
        print(">> Sending out jobs")
        self.dask_cluster.adapt(minimum=1 if self.run_options["adaptive"]
                                else self.run_options['scaleout'],
                      maximum=self.run_options['scaleout'])

        self.dask_client = Client(self.dask_cluster)
        print(">> Waiting for the first job to start...")
        self.dask_client.wait_for_workers(1)
        print(">> You can connect to the Dask viewer at http://localhost:8787")

        # if self.run_options["performance-report"]:
        #     self.performance_report_path = os.path.join(self.outputdir, f"{log_folder}/dask-report.html")
        #     print(f"Saving performance report to {self.performance_report_path}")
        #     self.performance_report(filename=performance_report_path):


    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        # in the futures executor Nworkers == N scaleout
        args["client"] = self.dask_client
        args["treereduction"] = self.run_options["tree-reduction"]
        args["retries"] = self.run_options["retries"]
        return args

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()




#--------------------------------------------------------------------
# Manual jobs executor (SLURM)

def _slurm_mem(mem):
    '''Normalize a mem-per-worker value ("4GB", "4G", 4000) to sbatch --mem
    syntax, which accepts K/M/G/T suffixes but not "GB".'''
    mem = str(mem).strip()
    m = re.fullmatch(r"(\d+)\s*([KMGT]?)B?", mem, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1)}{m.group(2).upper()}"
    return mem


def build_job_script_slurm(
    env_extras,
    abs_jobdir_path,
    abs_output_path,
    runnercmd,
    inner_yaml_basename,
    split_by_category,
    cores_per_worker,
    local_scratch_dir="/scratch/$USER",
):
    '''Build the per-job SLURM wrapper (job.sh) as a string.

    The SLURM counterpart of ``executors_lxplus.build_job_script``, adapted for
    a shared filesystem: nothing is transferred, the per-job config pickle and
    the inner run-options YAML are referenced by absolute path inside the
    jobs_dir, and the job runs inside a node-local scratch directory (removed
    via trap whatever way the job exits) with outputs copied back with plain
    ``cp``. The flag-file protocol (.idle/.running/.done/.failed) is identical
    to the condor executors so check-jobs/merge-outputs work unchanged.

    Arguments passed by the sbatch script: ``$1`` job id, ``$2`` config pickle
    basename inside the jobs_dir, ``$3`` chunksize.
    '''
    if split_by_category:
        splitcommands = f'''
    cd output || fail 'cd output failed'
    split-output output_all.coffea -b category -o output.coffea || fail 'split-output failed'
    rm -f output_all.coffea
    for f in *.coffea; do
        run_with_retries "cp $f {abs_output_path}/${{f%.coffea}}_job_$JOBID.coffea"
    done
    cd ..
'''
    else:
        splitcommands = f'run_with_retries "cp output/output_all.coffea {abs_output_path}/output_job_$JOBID.coffea"'

    script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}
JOBID="$1"
CONFIGPKL="$JOBDIR/$2"
CHUNKSIZE="$3"

run_with_retries() {{
    local cmd="$*"
    for i in {{1..10}}; do
        eval "$cmd" && return 0
        sleep 10
    done
    echo "$cmd failed after 10 attempts."
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
    exit 1
}}

fail() {{
    echo "$1"
    rm -f $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
    exit 1
}}

rm -f $JOBDIR/job_$JOBID.idle

echo "Starting job $JOBID"
touch $JOBDIR/job_$JOBID.running

# Run inside node-local scratch; removed whatever way the job exits.
WORKDIR="{local_scratch_dir}/pocketcoffea_${{SLURM_JOB_ID:-$$}}_job_$JOBID"
mkdir -p "$WORKDIR" || fail 'could not create local scratch dir'
trap 'rm -rf "$WORKDIR"' EXIT
cd "$WORKDIR" || fail 'cd to local scratch dir failed'

{runnercmd} --cfg $CONFIGPKL -o output EXECUTOR --chunksize $CHUNKSIZE --custom-run-options $JOBDIR/{inner_yaml_basename}
# Do things only if the job is successful
if [ $? -eq 0 ]; then
    echo 'Job successful'
    {splitcommands}
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.done
else
    echo 'Job failed'
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
fi
echo 'Done'
"""

    if int(cores_per_worker) > 1:
        script = script.replace("EXECUTOR", f"--executor futures --scaleout {cores_per_worker}")
    else:
        script = script.replace("EXECUTOR", "--executor iterative")
    return script


def build_sbatch_script(
    slurm_job_name,
    partition,
    walltime,
    cores_per_worker,
    mem_per_worker,
    abs_jobdir_path,
    log_stem,
    command,
    account=None,
    array=None,
    custom_sbatch_options=None,
):
    '''Build an sbatch submission script (the SLURM analogue of a condor .sub).

    ``--chdir`` points at the jobs_dir so job.sh, the config pickles, the
    chunksizes file and the relative ``logs/`` paths all resolve. `log_stem` is
    the log path without extension (e.g. ``logs/job_%j.5`` for a single job,
    ``logs/job_%A.%a`` for the array): the ``job_<schedulerid>.<jobindex>``
    naming is what check-jobs parses to map a log back to its job.
    `custom_sbatch_options` is a list of extra option lines, with or without
    the ``#SBATCH`` prefix.
    '''
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={slurm_job_name}",
        f"#SBATCH --partition={partition}",
        f"#SBATCH --time={walltime}",
        f"#SBATCH --cpus-per-task={cores_per_worker}",
        f"#SBATCH --mem={_slurm_mem(mem_per_worker)}",
        f"#SBATCH --chdir={abs_jobdir_path}",
        f"#SBATCH --output={log_stem}.out",
        f"#SBATCH --error={log_stem}.err",
    ]
    if account:
        lines.append(f"#SBATCH --account={account}")
    if array:
        lines.append(f"#SBATCH --array={array}")
    for opt in custom_sbatch_options or []:
        opt = str(opt).strip()
        if not opt.startswith("#SBATCH"):
            opt = f"#SBATCH {opt}"
        lines.append(opt)
    lines += ["", command, ""]
    return "\n".join(lines)


class ExecutorFactorySlurmPSI(ExecutorFactoryManualABC):
    '''Manual-job executor for the PSI Tier-3 SLURM cluster
    (``--executor slurm@T3_CH_PSI``).

    Submits one self-contained sbatch job per chunk-group (splitting logic in
    ExecutorFactoryManualABC) instead of streaming work through a Dask
    scheduler. Relies on the shared filesystem (/t3home, /work, /pnfs are
    mounted on the worker nodes): job configs, the grid proxy and outputs are
    referenced by absolute path — nothing is transferred. Jobs run inside
    node-local scratch and copy their output back with plain ``cp``.

    If ``worker-image`` is set the job wrapper runs inside ``apptainer exec``
    with the ``apptainer-binds`` mounts; otherwise the environment comes from
    ``conda-env`` / ``local-virtualenv`` / ``custom-setup-commands`` exactly
    like the Dask executor above.
    '''

    def get(self):
        pass

    def prepare_jobs(self, splits):
        config_files = []
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "scheduler": "slurm",
            "split_by_category": self.run_options["split-by-category"],
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "jobs_list": {}
        }
        # Disabling the postprocessing
        self.config.do_postprocessing = False
        # Splitting the filesets creating a new configuration for each and pickling it
        with Progress() as progress:
            task1 = progress.add_task("[cyan]Preparing config pkls...", total=len(splits))
            for i, split in enumerate(splits):
                # We want to create an unloaded copy of the configurator, and setting the filtered
                # fileset
                partial_config = self.config.clone()
                partial_config.set_filesets_manually(split)
                cloudpickle.dump(partial_config, open(f"{self.jobs_dir}/config_job_{i}.pkl", "wb"))
                config_files.append(f"{self.jobs_dir}/config_job_{i}.pkl")
                jobs_config["jobs_list"][f"job_{i}"] = {
                    "filesets": split,
                    "config_file": f"{self.jobs_dir}/config_job_{i}.pkl",
                    "output_file": f"{os.path.abspath(self.outputdir)}/output_job_{i}.coffea",
                }
                progress.update(task1, advance=1)

        yaml.dump(jobs_config, open(f"{self.jobs_dir}/jobs_config.yaml", "w"))
        return config_files

    def submit_jobs(self, jobs_config):
        '''Prepare the job wrapper and sbatch scripts and submit the job array.'''
        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)

        env_extras_list = get_worker_env(self.run_options,
                                         getattr(self, "x509_path", None), "slurm")
        env_extras = "\n".join(env_extras_list)

        # Prefer running the checked-out runner.py (shared filesystem) over the
        # entrypoint so a dev checkout on PYTHONPATH wins, like on lxplus.
        if os.getenv("PYTHONPATH"):
            pythonpath = os.getenv("PYTHONPATH")
        else:
            pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])
        runnerpath = f"{pythonpath}/pocket_coffea/scripts/runner.py"
        if os.path.isfile(runnerpath):
            runnercmd = "python " + runnerpath
        else:
            runnercmd = "pocket-coffea run"

        # Persist the inner-relevant run-option overrides (e.g. skip-bad-files)
        # for the inner pocket-coffea call on the worker (referenced by absolute
        # path in job.sh — no transfer on the shared filesystem).
        inner_yaml_path = write_inner_run_options(self.jobs_dir, self.run_options)
        inner_yaml_basename = os.path.basename(inner_yaml_path)

        script = build_job_script_slurm(
            env_extras=env_extras,
            abs_jobdir_path=abs_jobdir_path,
            abs_output_path=abs_output_path,
            runnercmd=runnercmd,
            inner_yaml_basename=inner_yaml_basename,
            split_by_category=self.run_options["split-by-category"],
            cores_per_worker=self.run_options["cores-per-worker"],
            local_scratch_dir=self.run_options.get("local-scratch-dir", "/scratch/$USER"),
        )
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Resolve per-job chunksize (scalar or per-sample dict, see
        # ExecutorFactoryManualABC._resolve_chunksize_for_job) and persist it:
        # the array tasks look their value up by line number.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        with open(f"{self.jobs_dir}/chunksizes.txt", "w") as f:
            f.write("\n".join(str(cs) for cs in per_job_chunksize) + "\n")

        # Optional apptainer wrapping of the whole job wrapper.
        exec_prefix = ""
        if (worker_image := self.run_options.get("worker-image", None)):
            binds = self.run_options.get("apptainer-binds",
                                         "/scratch,/pnfs,/t3home,/work,/cvmfs")
            bindargs = " ".join(f"-B {b.strip()}" for b in str(binds).split(",") if b.strip())
            exec_prefix = f"apptainer exec {bindargs} {worker_image} "

        n_jobs = len(jobs_config)
        # Unique-ish slurm job names (scancel matches by name, see
        # utils/slurm_queue.scancel_job): prefix with the output dir name.
        slurm_name_prefix = os.path.basename(abs_output_path)

        common = dict(
            partition=self.run_options["queue"],
            walltime=self.run_options["walltime"],
            cores_per_worker=self.run_options["cores-per-worker"],
            mem_per_worker=self.run_options["mem-per-worker"],
            abs_jobdir_path=abs_jobdir_path,
            account=self.run_options.get("account", None),
            custom_sbatch_options=self.run_options.get("custom-sbatch-options", None),
        )

        # Single job-array submission for the whole set; each task reads its own
        # chunksize from chunksizes.txt. max-concurrent-jobs throttles via %N.
        array_spec = f"0-{n_jobs - 1}"
        if (max_concurrent := self.run_options.get("max-concurrent-jobs", None)):
            array_spec += f"%{int(max_concurrent)}"
        array_command = (
            'CHUNKSIZE=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" chunksizes.txt)\n'
            f'{exec_prefix}bash job.sh $SLURM_ARRAY_TASK_ID '
            'config_job_${SLURM_ARRAY_TASK_ID}.pkl $CHUNKSIZE'
        )
        with open(f"{self.jobs_dir}/jobs_all.slurm", "w") as f:
            f.write(build_sbatch_script(
                slurm_job_name=f"{slurm_name_prefix}_jobs",
                log_stem="logs/job_%A.%a",
                command=array_command,
                array=array_spec,
                **common,
            ))

        # Individual sbatch files for resubmission (hard-coded chunksize), plus
        # the .idle flag seeding the check-jobs state machine.
        print(f"Creating {n_jobs} .slurm files for individual job submission.")
        for i in range(n_jobs):
            with open(f"{self.jobs_dir}/job_{i}.slurm", "w") as f:
                f.write(build_sbatch_script(
                    slurm_job_name=f"{slurm_name_prefix}_job_{i}",
                    log_stem=f"logs/job_%j.{i}",
                    command=f"{exec_prefix}bash job.sh {i} config_job_{i}.pkl {per_job_chunksize[i]}",
                    **common,
                ))
            with open(f"{self.jobs_dir}/job_{i}.idle", "w") as f:
                f.write("")

        if self.run_options.get("dry-run", False):
            print(f"Dry run, not submitting jobs. You can find all files: {abs_jobdir_path}")
            return
        print("Submitting jobs")
        os.system(f"cd {abs_jobdir_path} && sbatch jobs_all.slurm")


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "slurm":
        return ExecutorFactorySlurmPSI(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, dask, slurm")
