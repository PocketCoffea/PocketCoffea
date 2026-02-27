import os
import sys
import socket
import glob
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
        'export MALLOC_TRIM_THRESHOLD_=0',
        ]
    if exec_name == "dask":
        env_worker.append('ulimit -u unlimited')

    if not run_options['ignore-grid-certificate']:
        proxy_basename = os.path.basename(x509_path)
        if exec_name == "dask":
            # On LPC, workers run in apptainer/condor sandboxes where submit-node paths
            # like /uscms/home/... are often not mounted. Keep any Condor-provided proxy
            # path if valid, otherwise fall back to /tmp/x509up_u<uid>.
            env_worker.append(
                'if [ -z "$X509_USER_PROXY" ] || [ ! -f "$X509_USER_PROXY" ]; then '
                'if [ -f "/tmp/x509up_u$(id -u)" ]; then export X509_USER_PROXY="/tmp/x509up_u$(id -u)"; fi; '
                'fi'
            )
        else:
            # Manual-condor jobs may ship the proxy file as a local input; try local basename first.
            env_worker.append(
                f'if [ -f "{proxy_basename}" ]; then export X509_USER_PROXY="$PWD/{proxy_basename}"; '
                f'elif [ -f "{x509_path}" ]; then export X509_USER_PROXY="{x509_path}"; '
                'elif [ -f "/tmp/x509up_u$(id -u)" ]; then export X509_USER_PROXY="/tmp/x509up_u$(id -u)"; '
                'fi'
            )
    
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

    @staticmethod
    def _normalize_memory(value, default):
        if value is None:
            return default
        if isinstance(value, str):
            mem = value.strip()
            if not mem:
                return default
            upper = mem.upper()
            if upper.endswith(("GB", "GIB", "MB", "MIB")):
                return mem
            return f"{mem}GB"
        return f"{value}GB"

    @staticmethod
    def _normalize_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _get_cluster_backends(self):
        try:
            from lpcjobqueue import LPCCondorCluster
        except Exception as exc:
            raise Exception(
                "Executor dask@lpc requires lpcjobqueue and does not fallback to dask-jobqueue. "
                "Install it in the active environment, e.g. "
                "`python -m pip install -U git+https://github.com/PocketCoffea/lpcjobqueue.git@v0.5.0`. "
                f"Import error: {exc}"
            ) from exc
        return [("lpcjobqueue.LPCCondorCluster", LPCCondorCluster)]

    def _instantiate_cluster(self, cluster_cls, cluster_kwargs, job_extra_directives, worker_env, worker_python):
        """
        Different backends/versions use slightly different kwarg names.
        Try a few compatible combinations before failing.
        """
        env_candidates = [
            ("job_script_prologue", list(worker_env)),
            ("env_extra", list(worker_env)),
        ]
        directives_candidates = [
            ("job_extra_directives", dict(job_extra_directives)),
            ("job_extra", dict(job_extra_directives)),
        ]
        python_candidates = [worker_python, None]

        last_type_error = None
        for python_value in python_candidates:
            for directives_key, directives_value in directives_candidates:
                for env_key, env_value in env_candidates:
                    kwargs = dict(cluster_kwargs)
                    kwargs[directives_key] = directives_value
                    kwargs[env_key] = env_value
                    if python_value is not None:
                        kwargs["python"] = python_value
                    try:
                        return cluster_cls(**kwargs)
                    except TypeError as exc:
                        last_type_error = exc
                        continue

        if last_type_error is not None:
            raise last_type_error
        raise RuntimeError(f"Failed to initialize cluster class {cluster_cls.__name__}")

    def _dump_condor_log_tails(self, log_directory, max_lines=40):
        for pattern in ("*.err", "*.out", "*.log"):
            for path in sorted(glob.glob(os.path.join(log_directory, pattern))):
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as handle:
                        lines = handle.readlines()
                except OSError:
                    continue
                print(f">> Tail of {path}:")
                for line in lines[-max_lines:]:
                    print(line.rstrip())
        
    def setup(self):
        ''' Start the DASK cluster here'''
        self.setup_proxyfile()

        # HTCondor tooling relies on HOME/USER to write user config.
        if 'HOME' not in os.environ:
            os.environ['HOME'] = os.path.expanduser('~')
        if 'USER' not in os.environ:
            os.environ['USER'] = os.environ.get('LOGNAME', os.environ.get('USERNAME', 'unknown'))

        # Ensure local condor config path exists before any submit attempt.
        condor_dir = os.path.join(os.environ['HOME'], '.condor')
        os.makedirs(condor_dir, mode=0o755, exist_ok=True)
        user_config = os.path.join(condor_dir, "user_config")
        if not os.path.exists(user_config):
            open(user_config, "a").close()

        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        setup_dask(dask.config)

        # Check if we're on LPC
        hostname = socket.gethostname()
        if "lpc" not in hostname.lower() and "fnal" not in hostname.lower():
            raise Exception("Trying to run with dask/lpc not at LPC! Please try different runner options")

        backends = self._get_cluster_backends()

        # For lpcjobqueue we normally let the backend pick host/port defaults that are LPC-safe.
        # Users can still override with --dask-scheduler-port / --dask-scheduler-host.
        requested_port_raw = self.run_options.get("dask-scheduler-port", None)
        requested_port = None
        if requested_port_raw not in (None, "", "null", "None"):
            requested_port = self._normalize_int(requested_port_raw, 8786)

        if requested_port is not None:
            n_port = requested_port
            if not check_port(n_port):
                print(f">> Port {n_port} is occupied, trying to find an available port...")
                found_port = False
                for port in range(requested_port, requested_port + 100):
                    if check_port(port):
                        n_port = port
                        found_port = True
                        print(f">> Using port {n_port} instead")
                        break
                if not found_port:
                    for port in range(9000, 9100):
                        if check_port(port):
                            n_port = port
                            found_port = True
                            print(f">> Using port {n_port} instead")
                            break
                if not found_port:
                    raise RuntimeError(
                        f"Could not find an available port. Ports {requested_port}-{requested_port+100} and 9000-9100 are all occupied. "
                        f"Please free up a port or specify a different one with 'dask-scheduler-port' in run_options."
                    )
            print(">> Creating dask-lpc cluster transmitting on port:", n_port)
        else:
            n_port = None
            print(">> Creating dask-lpc cluster with backend-managed scheduler port")
        
        # Creating an HTCondor cluster for LPC
        log_folder = "condor_log"
        requested_log_directory = self.run_options.get("dask-log-directory", None)
        if requested_log_directory:
            log_directory = os.path.abspath(requested_log_directory)
        else:
            # lpcjobqueue requires log_directory to be in a host path writable by schedd.
            # Default to $HOME to avoid failing when outputdir is a container-only path (e.g. /srv/...).
            output_tag = os.path.basename(os.path.abspath(self.outputdir.rstrip("/"))) or "run"
            log_directory = os.path.join(
                os.environ.get("HOME", "/tmp"),
                "pocketcoffea_dask_logs",
                output_tag,
                log_folder,
            )
        os.makedirs(log_directory, exist_ok=True)
        print(f">> Using log_directory: {log_directory}")
        
        # Prepare environment variables for workers
        worker_env = get_worker_env(self.run_options, self.x509_path, "dask")
        worker_python = self.run_options.get("worker-python", "python3")
        scheduler_host = self.run_options.get("dask-scheduler-host", None)
        cores_per_worker = self._normalize_int(self.run_options.get("cores-per-worker", 1), 1)
        memory_str = self._normalize_memory(self.run_options.get("mem-per-worker", "2GB"), "2GB")
        disk_str = self._normalize_memory(self.run_options.get("disk-per-worker", "2GB"), "2GB")
        death_timeout = self._normalize_int(self.run_options.get("death-timeout", 3600), 3600)

        job_extra_directives = {
            "log": f"{log_directory}/dask_job_output.log",
            "output": f"{log_directory}/dask_job_output.out",
            "error": f"{log_directory}/dask_job_output.err",
            "should_transfer_files": "Yes",
            "when_to_transfer_output": "ON_EXIT",
            "+JobFlavour": f'"{self.run_options.get("queue", "workday")}"',
            "RequestCpus": str(cores_per_worker),
            "RequestMemory": memory_str,
        }
        worker_image = self.run_options.get("worker-image", None)
        if worker_image:
            # LPC accepts +ApptainerImage; keep Singularity key for compatibility.
            job_extra_directives["+ApptainerImage"] = f'"{worker_image}"'
            job_extra_directives["MY.SingularityImage"] = f'"{worker_image}"'

        user_extra_directives = self.run_options.get("dask-job-extra-directives")
        if isinstance(user_extra_directives, dict):
            job_extra_directives.update(user_extra_directives)

        scheduler_options = {}
        if n_port is not None:
            scheduler_options["port"] = n_port
        if scheduler_host:
            scheduler_options["host"] = scheduler_host

        cluster_kwargs = {
            "cores": cores_per_worker,
            "memory": memory_str,
            "disk": disk_str,
            "death_timeout": death_timeout,
            "log_directory": log_directory,
        }
        if scheduler_options:
            cluster_kwargs["scheduler_options"] = scheduler_options

        self.dask_cluster = None
        last_exception = None
        for backend_name, cluster_cls in backends:
            try:
                self.dask_cluster = self._instantiate_cluster(
                    cluster_cls=cluster_cls,
                    cluster_kwargs=cluster_kwargs,
                    job_extra_directives=job_extra_directives,
                    worker_env=worker_env,
                    worker_python=worker_python,
                )
                print(f">> Using {backend_name} backend")
                break
            except TypeError as exc:
                last_exception = exc
                continue

        if self.dask_cluster is None:
            if last_exception is not None:
                raise RuntimeError(f"Failed to initialize LPC cluster: {last_exception}") from last_exception
            raise RuntimeError("Failed to initialize LPC cluster backend.")

        # Cluster adaptive number of jobs only if requested
        print(">> Sending out jobs")
        scaleout = self._normalize_int(self.run_options.get('scaleout', 1), 1)
        self.dask_cluster.adapt(minimum=1 if self.run_options.get("adaptive", False)
                                else scaleout,
                      maximum=scaleout)
        
        worker_start_timeout = self._normalize_int(
            self.run_options.get("dask-worker-start-timeout", 600),
            600,
        )
        self.dask_client = Client(self.dask_cluster)
        print(">> Waiting for the first job to start...")
        try:
            self.dask_client.wait_for_workers(1, timeout=worker_start_timeout)
        except TimeoutError:
            print(f">> Timeout waiting {worker_start_timeout}s for first worker.")
            self._dump_condor_log_tails(log_directory)
            raise

        dashboard_link = getattr(self.dask_cluster, "dashboard_link", "http://localhost:8787")
        print(f">> You can connect to the Dask viewer at {dashboard_link}")

        
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

class ExecutorFactoryCondorLPC(ExecutorFactoryManualABC):
    def get(self):
        pass

    def prepare_jobs(self, splits):
        config_files = [ ]
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "split_by_category": self.run_options.get("split-by-category", False),
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

        copy_command = "cp"
        eos_prefix = self.run_options.get("eos-prefix", "root://cmseos.fnal.gov//eos/uscms")
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

        # Specify output filename to split-output script ->
        # This will save files such as output_CAT1.coffea, output_CAT2.coffea (remove "_all" from split outputs)...
        if self.run_options.get("split-by-category", False):
            splitcommands = f'''
    cd {abs_output_path}
    split-output output_all.coffea -b category -o output.coffea
    rm output_all.coffea
    for f in *.coffea; do
        run_with_retries "{copy_command} $f {abs_output_path}/${{f%.coffea}}_job_$1.coffea"
    done
'''
        else:
            splitcommands = f'run_with_retries "{copy_command} output/output_all.coffea {abs_output_path}/output_job_$1.coffea"'

        script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}

run_with_retries() {{
    local cmd="$*"
    for i in {{1..10}}; do
        eval "$cmd" && return 0
        sleep 10
    done
    echo "$cmd failed after 10 attempts."
    rm $JOBDIR/job_$1.running
    touch $JOBDIR/job_$1.failed
    exit 1
}}

rm -f $JOBDIR/job_$1.idle

echo "Starting job $1"
touch $JOBDIR/job_$1.running

{runnercmd} --cfg $2 -o output EXECUTOR --chunksize $3
# Do things only if the job is successful
if [ $? -eq 0 ]; then
    echo 'Job successful'
    {splitcommands}
    {columncommand}

    rm $JOBDIR/job_$1.running
    touch $JOBDIR/job_$1.done
else
    echo 'Job failed'
    rm $JOBDIR/job_$1.running
    touch $JOBDIR/job_$1.failed
fi
echo 'Done'
"""
        
        cores_per_worker = self.run_options.get("cores-per-worker", 1)
        chunksize = self.run_options.get("chunksize", 150000)
        if int(cores_per_worker) > 1:
            script = script.replace("EXECUTOR", f"--executor futures --scaleout {cores_per_worker}")
        else:
            script = script.replace("EXECUTOR", "--executor iterative")
            
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Writing the jid file as the htcondor python submission does not work in the singularity
        mem_per_worker = self.run_options.get('mem-per-worker', '2GB')
        # Handle mem-per-worker if it's a string with GB or just a number
        if isinstance(mem_per_worker, str) and 'GB' in mem_per_worker.upper():
            mem_value = mem_per_worker
        elif isinstance(mem_per_worker, str):
            mem_value = f"{mem_per_worker}GB"
        else:
            mem_value = f"{mem_per_worker}GB"
            
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).err",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            '+JobFlavour': f'"{self.run_options.get("queue", "workday")}"',
            'RequestCpus' : str(cores_per_worker),
            'RequestMemory' : mem_value,
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl {chunksize}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_input_files' : f"{abs_jobdir_path}/config_job_$(ProcId).pkl,{self.x509_path},{abs_jobdir_path}/job.sh",
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options.get("retries", 1),
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
        return ExecutorFactoryCondorLPC(**kwargs)
