import os
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_manual_jobs import ExecutorFactoryManualABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask
from pocket_coffea.utils.configurator import Configurator
import cloudpickle
import yaml
    

#--------------------------------------------------------------------
# Manual jobs executor

class ExecutorFactoryCondorIC(ExecutorFactoryManualABC):
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
source /vols/grid/cms/setup.sh
export X509_USER_PROXY={self.x509_path}
export HOME={os.environ["HOME"]}
export XRD_RUNFORKHANDLER=1
export MALLOC_TRIM_THRESHOLD_=0
JOBDIR={abs_jobdir_path}"""

        # Conditionally add the conda environment path export
        if self.run_options.get("conda-env", False):
            script += f'\nexport PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH\n'

        script += """\
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

        os.system(f"chmod +x {abs_jobdir_path}/job.sh")

        # Writing the jid file as the htcondor python submission does not work in the singularity
        sub = {
            'Executable': f"{abs_jobdir_path}/job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).err",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            '+MaxRuntime': self.run_options["+MaxRuntime"],
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) {abs_jobdir_path}/config_job_$(ProcId).pkl {abs_output_path} {self.run_options['chunksize']}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["retries"],
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
    if executor_name == "condor":
        return ExecutorFactoryCondorIC(**kwargs)
