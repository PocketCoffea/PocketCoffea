import os
import sys
import socket
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
from rich.progress import Progress

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

    script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}
JOBID="$1"

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

rm -f $JOBDIR/job_$JOBID.idle

echo "Starting job $JOBID"
touch $JOBDIR/job_$JOBID.running

{runnercmd} --cfg $2 -o output EXECUTOR --chunksize $3 --custom-run-options {inner_yaml_basename}
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


class ExecutorFactoryCondorCERN(ExecutorFactoryManualABC):
    def get(self):
        pass

    def prepare_jobs(self, splits):
        config_files = [ ]
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "split_by_category": self.run_options["split-by-category"],
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "jobs_list": {}
        }
        # Disabling the postprocessing
        self.config.do_postprocessing = False
        # Splitting the filets creating a new configuration for each and pickling it
        with Progress() as progress:
            task1 = progress.add_task("[cyan]Preparing config pkls...", total=len(splits))
            for i, split in enumerate(splits):
                # We want to create an unloaded copy of the configurator, and setting the filtered
                # fileset
                partial_config = self.config.clone()
                # take the config filr, set the fileset and save it.
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
        # save the configuration
        return config_files

    def submit_jobs(self, jobs_config):
        '''Prepare job config and script and submit the jobs to the cluster'''
        
        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)
        
        env_extras_list=get_worker_env(self.run_options,self.x509_path,"condor")
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
        )

        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Resolve per-job chunksize. Accepts a scalar (legacy) or a per-sample dict.
        # See ExecutorFactoryManualABC._resolve_chunksize_for_job.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        if len(set(per_job_chunksize)) > 1:
            print(f"[chunksize] Per-job chunksize varies (min={min(per_job_chunksize)}, "
                  f"max={max(per_job_chunksize)}); HTCondor `queue chunksize from arglist.txt` "
                  f"will inject the right value per job.")

        # Writing the jid file as the htcondor python submission does not work in the singularity
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).err",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            'MY.SendCredential': True,
            'MY.SingularityImage': f'"{self.run_options["worker-image"]}"',
            '+JobFlavour': f'"{self.run_options["queue"]}"',
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl $(chunksize)",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_input_files' : f"{abs_jobdir_path}/config_job_$(ProcId).pkl,{self.x509_path},{abs_jobdir_path}/job.sh,{abs_jobdir_path}/{inner_yaml_basename}",
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["retries"],
            'requirements' : 'Machine =!= LastRemoteHost'
        }

        # HTCondor inline `queue <var> from ( ... )` form: one job per item, with
        # the per-job value made available as $(chunksize) in the arguments line.
        # Single condor_submit jobs_all.sub submits everything; the items are
        # embedded directly in the sub file (no separate arglist file to manage).
        with open(f"{self.jobs_dir}/jobs_all.sub", "w") as f:
            for k, v in sub.items():
                f.write(f"{k} = {v}\n")
            f.write("queue chunksize from (\n")
            for cs in per_job_chunksize:
                f.write(f"  {cs}\n")
            f.write(")\n")
        # Creating also single sub files for resubmission. These hard-code their
        # own chunksize since they will be submitted with plain `queue`.
        print(f"Creating {len(jobs_config)} .sub files for individual job submission.")
        for i, _ in enumerate(jobs_config):
            with open(f"{self.jobs_dir}/job_{i}.sub", "w") as f:
                for k,v in sub.items():
                    if isinstance(v, str):
                        v = v.replace("$(ProcId)", str(i))
                        v = v.replace("$(ClusterId).log", f"$(ClusterId).{i}.log")
                        v = v.replace("$(chunksize)", str(per_job_chunksize[i]))
                    f.write(f"{k} = {v}\n")
                f.write(f"queue\n")
            # Let's also create a .idle file to indicate the the job is in idle
            with open(f"{self.jobs_dir}/job_{i}.idle", "w") as f:
                f.write("")

        dry_run = self.run_options.get("dry-run", False)
        if dry_run:
            print(f"Dry run, not submitting jobs. You can find all files: {abs_jobdir_path}")
            return
        else:
            print("Submitting jobs")
            os.system(f"cd {abs_jobdir_path} && condor_submit jobs_all.sub")


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "condor":
        return ExecutorFactoryCondorCERN(**kwargs)
