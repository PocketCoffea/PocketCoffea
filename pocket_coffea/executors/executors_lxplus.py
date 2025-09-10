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
from pocket_coffea.utils.rucio import get_xrootd_sites_map
import cloudpickle
import yaml
from copy import deepcopy

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
            pythonpath = sys.prefix.rsplit('/', 1)[0]
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

        print(">> Creating dask-lxplus cluster")
        n_port = 8786  #hardcoded by dask-cluster
        if not check_port(8786):
            raise RuntimeError(
                "Port '8786' is already occupied on this node. Try another machine."
            )
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
        
        env_extras_list=get_worker_env(self.run_options,self.x509_path,"condor")
        env_extras= "\n".join(env_extras_list)

        pythonpath = sys.prefix.rsplit('/', 1)[0]
        runnerpath = f"{pythonpath}/pocket_coffea/scripts/runner.py"
        if os.path.isfile(runnerpath):
            runnercmd = "python " + runnerpath
        else:
            runnercmd = "pocket-coffea run"

        # Specify output filename to split-output script ->
        # This will save files such as output_CAT1.coffea, output_CAT2.coffea (remove "_all" from split outputs)...
        if self.run_options["split-by-category"]:
            splitcommands = f'''
                cd {abs_output_path}
                split-output output_all.coffea -b category -o output.coffea
                rm output_all.coffea
                for f in *.coffea; do
                    cp "$f" "$3/${{f%.coffea}}_job_$1.coffea"
                done
            '''
        else:
            splitcommands = f"cp {abs_output_path}/output_all.coffea $3/output_job_$1.coffea"

        script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}

rm -f $JOBDIR/job_$1.idle

echo "Starting job $1"
touch $JOBDIR/job_$1.running

{runnercmd} --cfg $2 -o output EXECUTOR --chunksize $4
# Do things only if the job is successful
if [ $? -eq 0 ]; then
    echo 'Job successful'
    {splitcommands}

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
            '+JobFlavour': f'"{self.run_options["queue"]}"',
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl {abs_output_path} {self.run_options['chunksize']}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_input_files' : f"{abs_jobdir_path}/config_job_$(ProcId).pkl,{self.x509_path},{abs_jobdir_path}/job.sh",
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["retries"],
            'requirements' : 'Machine =!= LastRemoteHost'
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

        if jobs_to_recreate == "auto":
            failedjobs = [f.replace(".failed","") for f in os.listdir(self.jobs_dir) if f.endswith(".failed")]
            runningjobs = [f.replace(".running","") for f in os.listdir(self.jobs_dir) if f.endswith(".running")]
            idlejobs = [f.replace(".idle","") for f in os.listdir(self.jobs_dir) if f.endswith(".idle")]
            jobs_to_recreate = failedjobs + runningjobs + idlejobs
            if len(jobs_to_recreate) == 0:
                print(f"Could not automatically find *.failed/*.running/*.idle files in {self.jobs_dir}. All jobs probably succeeded! Exiting.")
                exit()
            jobs_to_recreate = ','.join(jobs_to_recreate)

        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            if not j.startswith("job_"):
                jobs_to_redo.append(f"job_{j}")
            else:
                jobs_to_redo.append(j)
        print(f"Recreating jobs: {jobs_to_redo}")

        # Check which jobs failed due to an XRootD error:
        # for such files we want to find an alternate site
        xrootdfailurefilelist = os.popen(f"grep -il {self.jobs_dir}/logs/*.err -e 'XRootD error'").read().split()
        xrootdfailurejoblist = ["job_"+f.split("/")[-1].split(".")[-2] for f in xrootdfailurefilelist]
        # move processed logs to a backup directory
        if len(xrootdfailurefilelist) > 0:
            backupdir = f"{self.jobs_dir}/logs/processed"
            os.makedirs(backupdir, exist_ok=True)
            os.system(f"mv {' '.join(xrootdfailurefilelist)} {backupdir}")

        sitemap = get_xrootd_sites_map()
        
        for job in jobs_to_redo:
            # Check if the job is in the list of jobs to recreate
            if job not in jobs_config["jobs_list"]:
                print(f"Job {job} not found in the list of jobs")
                continue

            # If the job failed due to XRootD error, find an alternative site
            if job in xrootdfailurejoblist:
                print(f"Replacing input files in {job} since it failed due to an XRootD error.")
                current_fileset = jobs_config["jobs_list"][job]["filesets"]
                new_fileset = deepcopy(current_fileset)
                for sample, dct in new_fileset.items():
                    newfllist = []
                    for fl in dct['files']:
                        newfl = find_other_file(fl,sitemap)
                        newfllist.append(newfl)
                    new_fileset[sample]['files'] = newfllist
                
                # Update files in the configurator
                config = cloudpickle.load(open(f"{self.jobs_dir}/config_{job}.pkl", "rb"))
                config.set_filesets_manually(new_fileset)
                cloudpickle.dump(config, open(f"{self.jobs_dir}/config_{job}.pkl", "wb"))

            # If the job failed due to timeout, increase the timelimit
            if job in runningjobs:
                update_queue(f"{self.jobs_dir}/{job}.sub",job)

            # Resubmit the job
            dry_run = self.run_options.get("dry-run", False)
            if dry_run:
                print(f"Dry run, not resubmitting  {job}")
            else:
                if job in failedjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.failed")
                elif job in runningjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.running")
                os.system(f"touch {self.jobs_dir}/{job}.idle")
                os.system(f"cd {self.jobs_dir} && condor_submit {job}.sub")
                print(f"Resubmitted {job}")

def find_other_file(filepath,sitemap):
    if filepath.startswith("root:/"):
        rootpref = filepath.split("/store/")[0]
        file = "/store/"+filepath.split("/store/")[1]
    else:
        rootpref = None
        file = filepath
    
    command = f'dasgoclient -query="site file={file}"'    
    sites = os.popen(command,'r').read().split()
    for site in sites:
        if site not in sitemap:
            continue
        sitepath = sitemap[site]
        if not isinstance(sitepath,str):
            continue
        if rootpref:
            if rootpref in sitepath or sitepath in rootpref:
                continue
        return sitepath+file
                        
    print(f"WARNING: No alternative site found for {filepath}. Redoing with the same file!")
    return filepath    

def update_queue(subfile,job):
    with open(subfile, 'r') as fl:
        lines = fl.readlines()
    
    with open(subfile, 'w') as fl:
        for ln in lines:
            if ln.startswith("+JobFlavour"):
                q = ln.split('"')[-2]
                idx = queues.index(q)
                if idx + 1 < len(queues):
                    newq = queues[idx+1]
                    print(f"Bumping {job} from {q} to {newq}")
                else:
                    print(f"{job}: Already at the highest queue! Cannot bump up.")
                    newq = q
                fl.write(f'+JobFlavour = "{newq}"\n')
            else:
                fl.write(ln)

queues = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek"
]
                
def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "condor":
        return ExecutorFactoryCondorCERN(**kwargs)
