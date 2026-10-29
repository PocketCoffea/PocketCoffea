import os
import sys
import socket
import subprocess
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_manual_jobs import (
    ExecutorFactoryManualABC,
    write_inner_run_options,
)
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask
from pocket_coffea.utils.configurator import Configurator
import pocket_coffea
import cloudpickle
import yaml
import json
import multiprocessing as mp
from statistics import median
import click
from rich.console import Console
from rich.table import Table
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from pocket_coffea.utils.benchmarking import load_sample_throughputs
from pocket_coffea.utils.htcondor_queue import (
    QUEUE_SECONDS,
    queue_for_runtime,
)

_PREPARE_CFG = None
_PREPARE_JOBS_DIR = None
_PREPARE_OUTPUTDIR = None


def _prepare_job_config_worker(config, split, i, jobs_dir, outputdir):
    partial_config = config.clone()
    partial_config.set_filesets_manually(split)
    outputfile = f"{os.path.abspath(outputdir)}/output_job_{i}.coffea"
    config_file = f"{jobs_dir}/config_job_{i}.pkl"
    with open(config_file, "wb") as f:
        cloudpickle.dump(partial_config, f)
    return i, config_file, split, outputfile

def _prepare_job_config_init(config, jobs_dir, outputdir):
    global _PREPARE_CFG, _PREPARE_JOBS_DIR, _PREPARE_OUTPUTDIR
    _PREPARE_CFG = config
    _PREPARE_JOBS_DIR = jobs_dir
    _PREPARE_OUTPUTDIR = outputdir

def _prepare_job_config_worker_indexed(item):
    i, split = item
    return _prepare_job_config_worker(_PREPARE_CFG, split, i, _PREPARE_JOBS_DIR, _PREPARE_OUTPUTDIR)


CONDOR_GRACEFUL_SHUTDOWN_SECONDS = 5 * 60


def build_condor_shutdown_policy():
    """Return the submit attributes used for the five-minute timeout warning."""
    return {
        "periodic_remove": (
            "(JobStatus == 2) && (JobCurrentStartDate =!= UNDEFINED) && "
            f"((time() - JobCurrentStartDate) > "
            f"(MaxRuntime - {CONDOR_GRACEFUL_SHUTDOWN_SECONDS}))"
        ),
        "want_graceful_removal": True,
        "job_max_vacate_time": CONDOR_GRACEFUL_SHUTDOWN_SECONDS,
        "kill_sig": 15,
    }


def build_condor_job_state(per_job_chunksize, per_job_queue, cpus, memory):
    if isinstance(per_job_queue, str):
        per_job_queue = [per_job_queue] * len(per_job_chunksize)
    if len(per_job_queue) != len(per_job_chunksize):
        raise ValueError("per_job_chunksize and per_job_queue must have the same length")
    return {
        str(i): {
            "queue": queue,
            "chunksize": chunksize,
            "request_cpus": cpus,
            "request_memory": memory,
            "resources_scaled": False,
            "resubmissions": 0,
        }
        for i, (chunksize, queue) in enumerate(zip(per_job_chunksize, per_job_queue))
    }


def get_worker_env(run_options,x509_path,exec_name="dask"):
    env_worker = [
        'export XRD_RUNFORKHANDLER=1',
        'export MALLOC_TRIM_THRESHOLD_=0'        ,
        ]
    if exec_name == "dask":
        env_worker.append('ulimit -u unlimited')

    if not run_options['ignore-grid-certificate']:
        env_worker.append(f'export X509_USER_PROXY={x509_path}')
    
    # Adding list of custom setup commands from user defined run options
    if run_options.get("custom-setup-commands", None):
        env_worker += run_options["custom-setup-commands"]

    # Now checking for conda environment  conda-env:true
    if run_options.get("conda-env", False):
        env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')
        if "CONDA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['CONDA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        elif "MAMBA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['MAMBA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        else:
            raise Exception("CONDA prefix not found in env! Something is wrong with your conda installation if you want to use conda in the dask cluster.")

    # if local-virtual-env: true the dask job is configured to pickup
    # the local virtual environment. 
    if run_options.get("local-virtualenv", False):
        env_worker.append(f"source {sys.prefix}/bin/activate")
        if exec_name == "dask":
            env_worker.append(f"export PYTHONPATH={sys.prefix}/lib:$PYTHONPATH")
        elif exec_name == "condor":
            if os.getenv("PYTHONPATH"):
                pythonpath = os.getenv("PYTHONPATH")
            else:
                pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])
            env_worker.append(f"export PYTHONPATH={pythonpath}")

    return env_worker

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        setup_dask(dask.config)

        # Spin up a HTCondor cluster for dask using dask_lxplus
        from dask_lxplus import CernCluster
        if "lxplus" not in socket.gethostname():
                raise Exception("Trying to run with dask/lxplus not at CERN! Please try different runner options")

        n_port = self.run_options.get("dask-scheduler-port", 8786)
        while not check_port(n_port):
            print(f">> Port {n_port} is already occupied, trying port {n_port + 1}...")
            n_port += 1
        print(">> Creating dask-lxplus cluster transmitting on port:", n_port)
        # Creating a CERN Cluster, special configuration for dask-on-lxplus
        log_folder = "condor_log"
        self.dask_cluster = CernCluster(
                cores=self.run_options['cores-per-worker'],
                memory=self.run_options['mem-per-worker'],
                disk=self.run_options['disk-per-worker'],
                image_type="singularity",
                worker_image=self.run_options["worker-image"],
                death_timeout=self.run_options["death-timeout"],
                scheduler_options={"port": n_port, "host": socket.gethostname()},
                log_directory = f"{self.outputdir}/{log_folder}",
                # shared_temp_directory="/tmp"
                job_extra={
                    "log": f"{self.outputdir}/{log_folder}/dask_job_output.log",
                    "output": f"{self.outputdir}/{log_folder}/dask_job_output.out",
                    "error": f"{self.outputdir}/{log_folder}/dask_job_output.err",
                    "should_transfer_files": "Yes", #
                    "when_to_transfer_output": "ON_EXIT",
                    "+JobFlavour": f'"{self.run_options["queue"]}"'
                },
                env_extra=get_worker_env(self.run_options,self.x509_path,"dask"),
            )

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
# Manual jobs executor

def build_job_script(
    env_extras,
    abs_jobdir_path,
    abs_output_path,
    copy_command,
    runnercmd,
    inner_yaml_basename,
    split_by_category,
    cores_per_worker,
    timeit=False,
    timeit_dir=None,
    timeit_copy_command=None,
):
    '''Build the per-job HTCondor wrapper (job.sh) as a string.

    Extracted from ExecutorFactoryCondorCERN.submit_jobs so the generated script can be
    unit-tested without a live condor submission. The job id is passed to the wrapper as
    ``$1`` and captured once into ``$JOBID`` so it is available inside ``run_with_retries``
    (where bare ``$1`` refers to the *function's* first argument — the command string —
    not the job id, which used to corrupt the flag-file names on copy failure).

    In split-by-category mode the split runs inside the job-local ``output/`` scratch dir
    (never the shared final dir) and every step is exit-checked, so a split/copy failure
    marks the job ``.failed`` instead of silently reporting ``.done``.
    '''
    if split_by_category:
        splitcommands = f'''
    cd output || {{ echo 'cd output failed'; rm $JOBDIR/job_$JOBID.running; touch $JOBDIR/job_$JOBID.failed; exit 1; }}
    split-output output_all.coffea -b category -o output.coffea || {{ echo 'split-output failed'; rm $JOBDIR/job_$JOBID.running; touch $JOBDIR/job_$JOBID.failed; exit 1; }}
    rm -f output_all.coffea
    for f in *.coffea; do
        run_with_retries "{copy_command} $f {abs_output_path}/${{f%.coffea}}_job_$JOBID.coffea"
    done
    cd ..
'''
    else:
        splitcommands = f'run_with_retries "{copy_command} output/output_all.coffea {abs_output_path}/output_job_$JOBID.coffea"'

    timeitcommands = ""
    if timeit:
        timeitcommands = f'''\n    mkdir -p timeit\n    if compgen -G "timeit/*.json" > /dev/null; then\n        for f in timeit/*.json; do\n            run_with_retries "{timeit_copy_command} \\\"$f\\\" {timeit_dir}/"\n        done\n    fi\n'''

    script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}
JOBID="$1"
MAX_XROOTD_REWRITES=10
cleanup_started=0

terminate_child_group() {{
    if [[ -z "${{child:-}}" ]]; then
        return 0
    fi
    local group_gone=0
    if [[ -n "${{child_pgid:-}}" ]]; then
        kill -TERM -- "-${{child_pgid}}" 2>/dev/null || true
        for _ in {{1..50}}; do
            if ! kill -0 -- "-${{child_pgid}}" 2>/dev/null; then
                group_gone=1
                break
            fi
            sleep 0.1
        done
        if [ "$group_gone" -eq 0 ]; then
            kill -KILL -- "-${{child_pgid}}" 2>/dev/null || true
            for _ in {{1..20}}; do
                if ! kill -0 -- "-${{child_pgid}}" 2>/dev/null; then
                    group_gone=1
                    break
                fi
                sleep 0.1
            done
        fi
    else
        kill -TERM "$child" 2>/dev/null || true
        group_gone=1
    fi
    wait "$child" 2>/dev/null || true
    if [[ -n "${{child_pgid:-}}" ]]; then
        group_gone=0
        for _ in {{1..20}}; do
            if ! kill -0 -- "-${{child_pgid}}" 2>/dev/null; then
                group_gone=1
                break
            fi
            sleep 0.1
        done
    fi
    child=""
    child_pgid=""
    if [ "$group_gone" -eq 1 ]; then
        return 0
    fi
    return 1
}}

cleanup() {{
    trap - TERM INT
    if [ "$cleanup_started" -eq 1 ]; then
        return
    fi
    cleanup_started=1
    echo "Termination signal received."
    if ! terminate_child_group; then
        rm -f "$JOBDIR/job_$JOBID.running" "$JOBDIR/job_$JOBID.idle"
        touch "$JOBDIR/job_$JOBID.failed"
        echo "Worker process-group cleanup failed; job marked failed." >&2
        exit 1
    fi
    rm -f "$JOBDIR/job_$JOBID.running" "$JOBDIR/job_$JOBID.idle"
    touch "$JOBDIR/job_$JOBID.timeout"
    echo "Marked job as timeout."
    exit 0
}}

trap cleanup TERM INT

run_with_retries() {{
    local cmd="$*"
    for i in {{1..10}}; do
        setsid bash -c "$cmd" &
        child=$!
        child_pgid=$child
        wait "$child"
        command_status=$?
        child=""
        child_pgid=""
        [ $command_status -eq 0 ] && return 0
        sleep 10
    done
    echo "$cmd failed after 10 attempts."
    rm -f $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
    exit 1
}}

rm -f $JOBDIR/job_$JOBID.idle

if [ "$4" -gt 1 ]; then
    EXECUTOR_ARGS=(--executor futures --scaleout "$4")
else
    EXECUTOR_ARGS=(--executor iterative)
fi

echo "Starting job $JOBID"
touch $JOBDIR/job_$JOBID.running

attempt=0
job_succeeded=0
failed_xrootd_sites="$_CONDOR_SCRATCH_DIR/failed_xrootd_sites.txt"
while true; do
    attempt_log="$_CONDOR_SCRATCH_DIR/job_attempt_${{attempt}}.out"
    echo "Runner attempt $attempt"
    setsid {runnercmd} --cfg "$2" -o output "${{EXECUTOR_ARGS[@]}}" --chunksize "$3" --custom-run-options {inner_yaml_basename}{' --timeit --_timeit-worker' if timeit else ''} > "$attempt_log" 2>&1 &
    child=$!
    child_pgid=$child
    wait "$child"
    runner_status=$?
    child=""
    child_pgid=""
    cat "$attempt_log"
    if [ $runner_status -eq 0 ]; then
        job_succeeded=1
        break
    fi
    if [ $attempt -ge $MAX_XROOTD_REWRITES ]; then
        echo "Reached the maximum number of XRootD recovery attempts ($MAX_XROOTD_REWRITES)."
        break
    fi
    setsid python -m pocket_coffea.scripts.rewrite_xrootd_site --config "$2" --log "$attempt_log" --failed-sites-file "$failed_xrootd_sites" &
    child=$!
    child_pgid=$child
    wait "$child"
    rewrite_status=$?
    child=""
    child_pgid=""
    if [ $rewrite_status -eq 0 ]; then
        echo "XRootD site rewrite changed the config. Rerunning job $JOBID."
        attempt=$((attempt + 1))
        continue
    fi
    break
done

if [ $job_succeeded -eq 1 ]; then
    echo 'Job successful'
    {splitcommands}
    {timeitcommands}
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.done
else
    echo 'Job failed'
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
fi
echo 'Done'
"""

    return script


class ExecutorFactoryCondorCERN(ExecutorFactoryManualABC):
    def get(self):
        pass

    def prepare_splitting(self, filesets):
        timeit_path = self.run_options.get("_timeit-dir")
        throughputs = load_sample_throughputs(timeit_path) if timeit_path else {}
        queue = self.run_options["queue"]
        threshold = self._queue_threshold()
        if queue == "auto" and not throughputs:
            raise click.ClickException(
                "--queue auto requires at least one valid timeit/*.json throughput file"
            )

        jobs = super().prepare_splitting(filesets)
        if queue == "auto":
            fallback = median(throughputs.values())
            rates = {dataset: throughputs.get(dataset, fallback) for dataset in filesets}
            job_seconds = self._job_runtime_seconds(filesets, jobs, rates)
            self._per_job_queue = [
                queue_for_runtime(seconds, threshold) for seconds in job_seconds
            ]
        else:
            rates = throughputs
            job_seconds = None
            self._per_job_queue = [queue] * len(jobs)
        if throughputs:
            self._print_runtime_forecast(
                filesets, jobs, throughputs, self._per_job_queue,
                rates, job_seconds, queue == "auto",
            )
        return jobs

    def _queue_threshold(self):
        try:
            threshold = float(self.run_options.get("queue_time_threshold_percent", 80))
        except (TypeError, ValueError):
            raise click.ClickException("queue_time_threshold_percent must be a number")
        if not 0 < threshold < 100:
            raise click.ClickException("queue_time_threshold_percent must be between 0 and 100")
        return threshold

    def _job_runtime_seconds(self, filesets, jobs, rates):
        workers = max(1, int(self.run_options["cores-per-worker"]))
        job_seconds = []
        for job in jobs:
            seconds = 0.0
            for dataset, split in job.items():
                nfiles = len(filesets[dataset]["files"])
                if nfiles == 0:
                    raise click.ClickException(f"Cannot estimate runtime for empty dataset {dataset!r}")
                average_events = int(filesets[dataset]["metadata"]["nevents"]) / nfiles
                seconds += len(split["files"]) * average_events / (rates[dataset] * workers)
            job_seconds.append(seconds)
        return job_seconds

    def _print_runtime_forecast(
        self, filesets, jobs, throughputs, per_job_queue,
        rates, job_seconds, auto,
    ):
        queue = self.run_options["queue"]
        threshold = self._queue_threshold()
        if not auto and queue not in QUEUE_SECONDS:
            raise click.ClickException(
                f"Cannot estimate queue usage for unknown LXPLUS queue {queue!r}"
            )

        rows = []
        whole_job_seconds = job_seconds if auto else [
            None if any(dataset not in rates for dataset in job)
            else self._job_runtime_seconds(filesets, [job], rates)[0]
            for job in jobs
        ]
        for dataset, fileset in filesets.items():
            rate = rates.get(dataset)
            if rate is None and not auto:
                rows.append((1, 0, dataset, None, None))
                continue
            nfiles = len(fileset["files"])
            if not nfiles:
                rows.append((1, 0, dataset, None, None))
                continue
            candidates = []
            for index, job in enumerate(jobs):
                if dataset not in job:
                    continue
                seconds = whole_job_seconds[index]
                if seconds is not None:
                    candidates.append((seconds, per_job_queue[index]))
            if not candidates:
                rows.append((1, 0, dataset, None, None))
                continue
            seconds, assigned_queue = max(candidates)
            percent = 100 * seconds / QUEUE_SECONDS[assigned_queue]
            rows.append((0, seconds, dataset, assigned_queue, percent))
        rows.sort(key=lambda row: (row[0], row[1], row[2]))

        table = Table(title="Expected max time per job")
        table.add_column("Sample", style="cyan")
        table.add_column("Expected max time per job", justify="right")
        table.add_column("Queue" if auto else "% of queue time", justify="right")
        exceeds_limit = False
        for missing, seconds, dataset, assigned_queue, percent in rows:
            if missing:
                table.add_row(dataset, "Missing", "Missing")
                continue
            exceeds_limit |= percent > threshold
            colour = (
                "red"
                if (auto and assigned_queue == "nextweek" and percent > threshold)
                or percent >= 95
                else "yellow" if percent > threshold else "green"
            )
            hours, minutes = divmod(round(seconds / 60), 60)
            queue_or_percent = assigned_queue if auto else f"{percent:.1f}%"
            table.add_row(
                dataset,
                f"[{colour}]{hours}h{minutes:02d}m[/]",
                f"[{colour}]{queue_or_percent}[/]",
            )
        Console().print(table)

        if not auto and exceeds_limit:
            Console().print(
                "Alternatively, use --queue auto to select a queue per job.",
                style="yellow",
            )
            if not click.confirm(
                f"Some jobs may not fit in the {queue} queue. Proceed anyway?",
                default=False,
            ):
                raise click.Abort()

    def prepare_jobs(self, splits):
        config_files = [ ]
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "split_by_category": self.run_options["split-by-category"],
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "submission": {
                "format_version": 1,
                "executor": "condor@lxplus",
                "requires_grid_certificate": not self.run_options["ignore-grid-certificate"],
                "proxy_transfer_path": getattr(self, "x509_path", None),
                "proxy_source": (
                    None if self.run_options["ignore-grid-certificate"]
                    else "explicit" if self.run_options.get("voms-proxy") else "default"
                ),
            },
            "jobs_list": {}
        }
        # Disabling the postprocessing
        self.config.do_postprocessing = False
        # Splitting the filesets, creating a new configuration for each and pickling it.
        # Parallelised with multiprocessing.Pool to speed up large submission batches.
        total_splits = len(splits)
        config_files = [None] * total_splits
        console = Console(stderr=True)
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[cyan]Preparing config pkls"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=10,
            transient=False,
        )
        work_items = [(i, split) for i, split in enumerate(splits)]
        with progress:
            task1 = progress.add_task("prepare_pkls", total=total_splits)
            ctx = mp.get_context("fork")
            with ctx.Pool(
                processes=8,
                initializer=_prepare_job_config_init,
                initargs=(self.config, self.jobs_dir, self.outputdir),
            ) as pool:
                for i, config_file, split, output_file in pool.imap_unordered(
                    _prepare_job_config_worker_indexed, work_items, chunksize=1
                ):
                    config_files[i] = config_file
                    jobs_config["jobs_list"][f"job_{i}"] = {
                        "filesets": split,
                        "config_file": config_file,
                        "output_file": output_file,
                    }
                    progress.update(task1, advance=1)

        yaml.dump(jobs_config, open(f"{self.jobs_dir}/jobs_config.yaml", "w"))
        # save the configuration
        return config_files

    def submit_jobs(self, jobs_config):
        '''Prepare job config and script and submit the jobs to the cluster'''
        
        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)
        
        env_extras_list = get_worker_env(
            self.run_options, getattr(self, "x509_path", ""), "condor"
        )
        env_extras= "\n".join(env_extras_list)

        if os.getenv("PYTHONPATH"):
            pythonpath = os.getenv("PYTHONPATH")
        else:
            pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])        

        copy_command = "cp"
        eos_prefix = self.run_options["eos-prefix"]
        if abs_output_path.startswith("/eos/"):
            abs_output_path = eos_prefix + abs_output_path
        if abs_output_path.startswith(eos_prefix):
            copy_command = "xrdcp -f"

        timeit = bool(self.run_options.get("timeit", False))
        timeit_dir = self.run_options.get("_timeit-dir")
        timeit_destination = timeit_dir
        timeit_copy_command = "cp -f"
        if timeit:
            os.makedirs(timeit_dir, exist_ok=True)
            if timeit_dir.startswith("/eos/"):
                timeit_destination = eos_prefix + timeit_dir
                timeit_copy_command = "xrdcp -f"

        # Handle columns
        columncommand = ""
        if len(self.config.columns) > 0 and "dump_columns_as_arrays_per_chunk" in self.config.workflow_options:
            column_out_dir = self.config.workflow_options["dump_columns_as_arrays_per_chunk"]
            if not os.path.isabs(column_out_dir) and not column_out_dir.startswith(eos_prefix):
                # If the config contains an absolute path, then the
                # parquets are written directly to disk (e.g. eos.)
                # This may be unstable, but not much to do at the executor level.
                # Otherwise, check that the directory exists and then copy the directory to outputdir
                columncommand = f'[ -d "{column_out_dir}" ] && run_with_retries "{copy_command} -r {column_out_dir} {abs_output_path}"'

        runnerpath = f"{pythonpath}/pocket_coffea/scripts/runner.py"
        if os.path.isfile(runnerpath):
            runnercmd = "python " + runnerpath
        else:
            runnercmd = "pocket-coffea run"

        # Persist the inner-relevant run-option overrides (e.g. skip-bad-files)
        # so the *inner* pocket-coffea call running on the worker can honour
        # them via --custom-run-options. The YAML is whitelisted to keep
        # outer-only keys (worker-image, queue, cores-per-worker, ...) from
        # leaking through.
        inner_yaml_path = write_inner_run_options(self.jobs_dir, self.run_options)
        inner_yaml_basename = os.path.basename(inner_yaml_path)
        transfer_input_files = [
            f"{abs_jobdir_path}/config_job_$(ProcId).pkl",
            f"{abs_jobdir_path}/job.sh",
            f"{abs_jobdir_path}/{inner_yaml_basename}",
        ]
        if not self.run_options["ignore-grid-certificate"]:
            transfer_input_files.insert(1, self.x509_path)

        # Build the per-job wrapper (extracted into build_job_script so it is unit-testable).
        script = build_job_script(
            env_extras=env_extras,
            abs_jobdir_path=abs_jobdir_path,
            abs_output_path=abs_output_path,
            copy_command=copy_command,
            runnercmd=runnercmd,
            inner_yaml_basename=inner_yaml_basename,
            split_by_category=self.run_options["split-by-category"],
            cores_per_worker=self.run_options["cores-per-worker"],
            timeit=timeit,
            timeit_dir=timeit_destination,
            timeit_copy_command=timeit_copy_command,
        )
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Resolve per-job chunksize. Accepts a scalar or a per-sample dict.
        # See ExecutorFactoryManualABC._resolve_chunksize_for_job.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        per_job_queue = getattr(
            self, "_per_job_queue", [self.run_options["queue"]] * len(per_job_chunksize)
        )
        if len(per_job_queue) != len(per_job_chunksize):
            raise ValueError("per_job_chunksize and per_job_queue must have the same length")
        if len(set(per_job_chunksize)) > 1:
            print(f"[chunksize] Per-job chunksize varies (min={min(per_job_chunksize)}, "
                  f"max={max(per_job_chunksize)}); HTCondor `queue chunksize from arglist.txt` "
                  f"will inject the right value per job.")

        # Writing the jid file as the htcondor python submission does not work in the singularity
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            'MY.SendCredential': True,
            'MY.SingularityImage': f'"{self.run_options["worker-image"]}"',
            '+JobFlavour': '"$(QUEUE)"',
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl $(chunksize) {self.run_options['cores-per-worker']}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_output_files' : '',
            'transfer_input_files' : ",".join(transfer_input_files),
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["retries"],
            'requirements' : 'Machine =!= LastRemoteHost'
        }
        sub.update(build_condor_shutdown_policy())

        # HTCondor inline `queue <var> from ( ... )` form: one job per item, with
        # the per-job value made available as $(chunksize) in the arguments line.
        # Single condor_submit jobs_all.sub submits everything; the items are
        # embedded directly in the sub file (no separate arglist file to manage).
        with open(f"{self.jobs_dir}/jobs_all.sub", "w") as f:
            for k, v in sub.items():
                f.write(f"{k} = {v}\n")
            f.write("queue chunksize, QUEUE from (\n")
            for cs, queue in zip(per_job_chunksize, per_job_queue):
                f.write(f"  {cs} {queue}\n")
            f.write(")\n")

        job_state = build_condor_job_state(
            per_job_chunksize,
            per_job_queue,
            self.run_options["cores-per-worker"],
            self.run_options["mem-per-worker"],
        )
        with open(f"{self.jobs_dir}/job_state.json", "w") as f:
            json.dump(job_state, f, indent=2, sort_keys=True)

        # check-jobs appends one queue row per failed job to this template.
        resubmit_sub = dict(sub)
        resubmit_sub.update({
            "Error": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).out",
            "Output": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).out",
            "Log": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).log",
            "+JobFlavour": '"$(QUEUE)"',
            "RequestCpus": "$(CPUS)",
            "RequestMemory": "$(MEMORY)",
            "arguments": "$(PROC) config_job_$(PROC).pkl $(CHUNKSIZE) $(CPUS)",
            "transfer_input_files": ",".join(
                path.replace("$(ProcId)", "$(PROC)") for path in transfer_input_files
            ),
        })
        with open(f"{self.jobs_dir}/resubmit.sub", "w") as f:
            for k, v in resubmit_sub.items():
                f.write(f"{k} = {v}\n")

        dry_run = self.run_options.get("dry-run", False)
        if dry_run:
            print(f"Dry run, not submitting jobs. You can find all files: {abs_jobdir_path}")
            return
        idle_markers = []
        for i, _ in enumerate(per_job_chunksize):
            marker = f"{self.jobs_dir}/job_{i}.idle"
            open(marker, "w").close()
            idle_markers.append(marker)

        print("Submitting jobs")
        try:
            result = subprocess.run(
                ["condor_submit", "jobs_all.sub"], cwd=abs_jobdir_path,
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                check=False,
            )
        except OSError as exc:
            submit_error = str(exc)
        else:
            submit_error = "" if result.returncode == 0 else (
                f"exit code {result.returncode}: {result.stdout.strip()}")
        if submit_error:
            for marker in idle_markers:
                try:
                    os.remove(marker)
                except FileNotFoundError:
                    pass
            raise RuntimeError(
                f"condor_submit jobs_all.sub failed: {submit_error}"
            )


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "condor":
        return ExecutorFactoryCondorCERN(**kwargs)
