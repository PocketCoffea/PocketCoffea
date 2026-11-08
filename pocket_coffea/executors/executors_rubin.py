import os, getpass
import subprocess
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_manual_jobs import ExecutorFactoryManualABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port

from pocket_coffea.parameters.dask_env import setup_dask
import dask.config
from dask_jobqueue import HTCondorCluster

from .executors_manual_jobs import (
    write_inner_run_options,
)
import cloudpickle
import yaml
        
class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        pathvar = [i for i in os.environ["PATH"].split(":") if "envs/PocketCoffea/" in i][0]
        env_worker = [
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_CERT_DIR={pathvar[:-4]}/etc/grid-security/certificates',   #Note: this needs `conda install conda-forge::ca-certificates`
            'ulimit -s unlimited',
            # f'source {os.environ["HOME"]}/.bashrc',
            f"cd {os.getcwd()}",
            f"echo PWD `pwd`",
            f"export PATH={pathvar}:$PATH",
            'echo "Proxy:"',
            'voms-proxy-info',
            'echo Path $PATH'
            ]
        if not self.run_options.get("ignore-grid-certificate", False):
            env_worker.insert(1, f'export X509_USER_PROXY={self.x509_path}')
        
        # Adding list of custom setup commands from user defined run options
        if self.run_options.get("custom-setup-commands", None):
            env_worker += self.run_options["custom-setup-commands"]

        return env_worker
    
        
    def setup(self):
        ''' Start the DASK cluster here'''

        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        setup_dask(dask.config)

        print(">>> Creating an HTCondorCluster Dask cluster")
        self.dask_cluster = HTCondorCluster(
            cores  = self.run_options['cores-per-worker'],
            memory = self.run_options['mem-per-worker'],
            disk = self.run_options.get('disk-per-worker', "2GB"),
            job_script_prologue = self.get_worker_env(),
            log_directory = os.path.join(self.outputdir, "dask_log"),
            scheduler_options={"host": socket.gethostname()},
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

        
    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        args["client"] = self.dask_client
        args["treereduction"] = self.run_options["tree-reduction"]
        args["retries"] = self.run_options["retries"]
        return args

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()


#--------------------------------------------------------------------
# Manual jobs executor
class ExecutorFactoryCondorUMD(ExecutorFactoryManualABC):
    def get(self):
        pass

    def prepare_jobs(self, splits):
        config_files = [ ]
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "submission": {
                "format_version": 1,
                "executor": "condor@rubin",
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
        # Splitting the filets creating a new configuration for each and pickling it
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
            
        yaml.dump(jobs_config, open(f"{self.jobs_dir}/jobs_config.yaml", "w"))
        # save the configuration
        return config_files

    def submit_jobs(self, jobs_config):
        '''Prepare job config and script and submit the jobs to the cluster'''
        
        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)

        # Forward inner-relevant run-option overrides (skip-bad-files, ...)
        # to the worker so the inner pocket-coffea call honours them via
        # --custom-run-options. See executors_manual_jobs.write_inner_run_options.
        inner_yaml_path = write_inner_run_options(self.jobs_dir, self.run_options)
        inner_yaml_basename = os.path.basename(inner_yaml_path)

        proxy_env = (f"export X509_USER_PROXY={self.x509_path.split('/')[-1]}\n"
                     if not self.run_options.get("ignore-grid-certificate", False) else "")
        script = f"""#!/bin/bash
{proxy_env}export XRD_RUNFORKHANDLER=1
export MALLOC_TRIM_THRESHOLD_=0
JOBDIR={abs_jobdir_path}

rm -f $JOBDIR/job_$1.idle

echo "Starting job $1"
touch $JOBDIR/job_$1.running
pocket-coffea run --cfg $2 -o output EXECUTOR --chunksize $4 --custom-run-options {inner_yaml_basename}
# Do things only if the job is successful
if [ $? -eq 0 ]; then
    echo 'Job successful'
    cp output/output_all.coffea $3/output_job_$1.coffea

    rm $JOBDIR/job_$1.running
    touch $JOBDIR/job_$1.done
else
    echo 'Job failed'
    rm $JOBDIR/job_$1.running
    touch $JOBDIR/job_$1.failed
fi
echo 'Done'"""
        
        if int(self.run_options["cores-per-worker"]) > 1:
            script = script.replace("EXECUTOR", f"--executor futures --scaleout {self.run_options['cores-per-worker']}")
        else:
            script = script.replace("EXECUTOR", "--executor iterative")
            
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Writing the jid file as the htcondor python submission does not work in the singularity
        transfer_input_files = [f"{abs_jobdir_path}/config_job_$(ProcId).pkl",
                                f"{abs_jobdir_path}/job.sh",
                                f"{abs_jobdir_path}/{inner_yaml_basename}"]
        if not self.run_options.get("ignore-grid-certificate", False):
            transfer_input_files.insert(1, self.x509_path)
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).err",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).log",
            'MY.SendCredential': True,
            'MY.SingularityImage': f'"{self.run_options["worker-image"]}"',
            '+MaxRuntime' : self.run_options['max-run-time'],
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl {abs_output_path} $(chunksize)",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_input_files' : ",".join(transfer_input_files),
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["max-retries"],
            'requirements' : 'Machine =!= LastRemoteHost',
            'notify_user': self.run_options['notify-user'],
            'notification': 'always'

        }

        # Resolve per-job chunksize: accepts a scalar or per-sample dict.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        if len(set(per_job_chunksize)) > 1:
            print(f"[chunksize] Per-job chunksize varies (min={min(per_job_chunksize)}, "
                  f"max={max(per_job_chunksize)}); HTCondor `queue chunksize from arglist.txt` "
                  f"will inject the right value per job.")

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
        resubmit_sub = dict(sub)
        resubmit_sub.update({
            "Error": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).err",
            "Output": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).out",
            "Log": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).log",
            "RequestCpus": "$(CPUS)",
            "RequestMemory": "$(MEMORY)",
            "arguments": f"$(PROC) config_job_$(PROC).pkl {abs_output_path} $(CHUNKSIZE)",
            "transfer_input_files": ",".join(
                path.replace("$(ProcId)", "$(PROC)") for path in transfer_input_files
            ),
        })
        with open(f"{self.jobs_dir}/resubmit.sub", "w") as f:
            for k, v in resubmit_sub.items():
                f.write(f"{k} = {v}\n")

        job_state = {
            str(i): {
                "chunksize": chunksize,
                "request_cpus": self.run_options["cores-per-worker"],
                "request_memory": self.run_options["mem-per-worker"],
                "resubmissions": 0,
            }
            for i, chunksize in enumerate(per_job_chunksize)
        }
        with open(f"{self.jobs_dir}/job_state.json", "w") as f:
            import json
            json.dump(job_state, f, indent=2, sort_keys=True)

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
        return ExecutorFactoryCondorUMD(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, dask")
