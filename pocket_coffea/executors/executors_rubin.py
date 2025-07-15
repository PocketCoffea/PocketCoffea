import os, getpass
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

from pocket_coffea.utils.configurator import Configurator
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
            f'export X509_USER_PROXY={self.x509_path}',
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

        script = f"""#!/bin/bash
export X509_USER_PROXY={self.x509_path.split("/")[-1]}
export XRD_RUNFORKHANDLER=1
export MALLOC_TRIM_THRESHOLD_=0
JOBDIR={abs_jobdir_path}

rm -f $JOBDIR/job_$1.idle

echo "Starting job $1"
touch $JOBDIR/job_$1.running
pocket-coffea run --cfg $2 -o output EXECUTOR --chunksize $4
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
            script = script.replace("EXECUTOR", f"--executor futures --scalout {self.run_options['cores-per-worker']}")
        else:
            script = script.replace("EXECUTOR", "--executor iterative")
            
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Writing the jid file as the htcondor python submission does not work in the singularity
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).err",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            'MY.SendCredential': True,
            'MY.SingularityImage': f'"{self.run_options["worker-image"]}"',
            '+MaxRuntime' : self.run_options['max-run-time'],
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl {abs_output_path} {self.run_options['chunksize']}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_input_files' : f"{abs_jobdir_path}/config_job_$(ProcId).pkl,{self.x509_path},{abs_jobdir_path}/job.sh",
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["max-retries"],
            'requirements' : 'Machine =!= LastRemoteHost',
            'notify_user': self.run_options['notify-user'],
            'notification': 'always'
            
        }

        with open(f"{self.jobs_dir}/jobs_all.sub", "w") as f:
            for k,v in sub.items():
                f.write(f"{k} = {v}\n")
            f.write(f"queue {len(jobs_config)}\n")
        # Creating also single sub files for resubmission
        for i, _ in enumerate(jobs_config):
            with open(f"{self.jobs_dir}/job_{i}.sub", "w") as f:
                for k,v in sub.items():
                    if isinstance(v, str):
                        v = v.replace("$(ProcId)", str(i))
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


    def recreate_jobs(self, jobs_to_recreate):
        # Read the jobs config
        if not os.path.exists(f"{self.jobs_dir}/jobs_config.yaml"):
            print("No jobs_config.yaml found. Exiting.")
            exit(1)
        with open(f"{self.jobs_dir}/jobs_config.yaml") as f:
            jobs_config = yaml.safe_load(f)

        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            if not j.startswith("job_"):
                jobs_to_redo.append(f"job_{j}")
        print(f"Recreating jobs: {jobs_to_redo}")
        # Check if the job is in the list of jobs to recreate
        for job in jobs_to_redo:
            if job not in jobs_config["jobs_list"]:
                print(f"Job {job} not found in the list of jobs")
                continue
            # Open the configurator and modify the fileset.
            # This is usually done to change a file location
            # Load the configurator
            config = cloudpickle.load(open(f"{self.jobs_dir}/config_{job}.pkl", "rb"))
            # Modify the fileset
            config.set_filesets_manually(jobs_config["jobs_list"][job]["filesets"])
            # Save the configurator
            cloudpickle.dump(config, open(f"{self.jobs_dir}/config_{job}.pkl", "wb"))
            # Resubmit the job
            dry_run = self.run_options.get("dry-run", False)
            if dry_run:
                print(f"Dry run, not resubmitting  {job}")
            else:
                os.system(f"rm {self.jobs_dir}/{job}.failed")
                os.system(f"touch {self.jobs_dir}/{job}.idle")
                os.system(f"cd {self.jobs_dir} && condor_submit {job}.sub")
                print(f"Resubmitted {job}")


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
