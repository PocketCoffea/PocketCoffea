import os, getpass
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask

import dask.config

import parsl
from parsl.providers import SlurmProvider
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname

class ParslSlurmExecutorFactory(ExecutorFactoryABC):
    """
    Parsl executor for RWTH CLAIX via SLURM
    """

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        env_worker = [
            'echo "===== Parsl SLURM worker init ====="',
            'echo "Date: $(date)"',
            'echo "Node: $(hostname)"',
            'export XRD_RUNFORKHANDLER=1',
            'export MALLOC_TRIM_THRESHOLD_=0',
            f'export X509_USER_PROXY={self.x509_path}',
            'export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates',
            f'export PYTHONPATH=$PYTHONPATH:{os.getcwd()}',
            'ulimit -s unlimited || true', # aviod failure
            'ulimit -u unlimited || true',
            f'cd {os.getcwd()}',
        ]

        # Conda / Mamba activation 
        if self.run_options.get("conda-env", False):

            # >>> NEW: ensure the env used on the submit node is also used on workers
            if "CONDA_PREFIX" in os.environ:
                env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')

            # if "MAMBA_ROOT_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["MAMBA_ROOT_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'mamba activate {os.environ.get("CONDA_DEFAULT_ENV","base")}')
            # elif "CONDA_ROOT_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["CONDA_ROOT_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'conda activate {os.environ.get("CONDA_DEFAULT_ENV","base")}')
            # elif "CONDA_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["CONDA_PREFIX"]}/etc/profile.d/conda.sh || true')
            #     env_worker.append(f'conda activate {os.environ.get("CONDA_DEFAULT_ENV","base")} || true')
            # elif "MAMBA_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["MAMBA_PREFIX"]}/etc/profile.d/conda.sh || true')
            #     env_worker.append(f'mamba activate {os.environ.get("MAMBA_DEFAULT_ENV","base")} || true')
            # else:
            #     raise Exception("CONDA/Mamba ROOT_PREFIX or PREFIX not found for worker activation.")
            env_worker.append('echo "Conda/Mamba environment activated."')

        # Local virtualenv 
        if self.run_options.get("local-virtualenv", False):
            env_worker.append(f'source {sys.prefix}/bin/activate')

        # Custom user commands appended last
        if self.run_options.get("custom-setup-commands", None):
            env_worker += self.run_options["custom-setup-commands"]

        return env_worker

    def setup(self):
        """Start Parsl HTEX + SLURM with options taken from YAML."""
        self.setup_proxyfile()

        # Top-level options
        scaleout = self.run_options.get("scaleout", 1)
        walltime = self.run_options.get("walltime", "02:00:00")
        max_workers_per_node = self.run_options.get("max-workers-per-node", self.run_options.get("cores-per-worker", 1))
        worker_debug = self.run_options.get("worker-debug", False)
        prefetch_capacity = self.run_options.get("prefetch-capacity", 0)

        # slurm: sub-dict
        slurm_cfg = self.run_options.get("slurm", {}) or {}
        partition = slurm_cfg.get("partition", None)
        account = slurm_cfg.get("account", None)
        scheduler_options = slurm_cfg.get("scheduler-options", "")  # raw multiline #SBATCH string

        run_dir = self.run_options.get("parsl-run-dir",
                    os.path.join(self.outputdir, self.run_options.get("logs-dir", "logs_parsl"), "parsl_runinfo"))
        os.makedirs(run_dir, exist_ok=True)

        # Provider (pass-through from YAML)
        provider = SlurmProvider(
            partition=partition,
            account=account,
            nodes_per_block=self.run_options.get("nodes-per-block", 1),
            init_blocks=scaleout,
            max_blocks=scaleout,
            min_blocks=self.run_options.get("min-blocks", 0),
            walltime=walltime,
            launcher=SrunLauncher(overrides=self.run_options.get("srun-overrides", "")),
            worker_init="\n".join(self.get_worker_env()),
            scheduler_options=scheduler_options,
            exclusive=self.run_options.get("exclusive", False),
        )

        # Executor
        htex = HighThroughputExecutor(
            label="coffea_parsl_slurm",
            address=address_by_hostname(),
            max_workers_per_node=max_workers_per_node,
            worker_debug=worker_debug,
            prefetch_capacity=prefetch_capacity,
            provider=provider,
        )

        slurm_htex = Config(
            executors=[htex],
            retries=self.run_options.get("retries", 0),
            run_dir=run_dir,
        )

        self.slurm_cluster = parsl.load(slurm_htex)
        print("Ready to run with Parsl HTEX on SLURM (CLAIX).")

    def get(self):
        return coffea_processor.parsl_executor(**self.customized_args())

    def customized_args(self):
        return super().customized_args()

    def close(self):
        try:
            parsl.dfk().cleanup()
        finally:
            parsl.clear()

class DaskExecutorFactory(ExecutorFactoryABC):
    '''
    Attempt to setup DASK at RWTH CLAIX cluster via SLURM
    '''

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        env_worker = [
            'export XRD_RUNFORKHANDLER=1',
            'export MALLOC_TRIM_THRESHOLD_=0',
            f'export X509_USER_PROXY={self.x509_path}',
            'export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates',
            'ulimit -u unlimited', # operation not applicable on CLAIX
            ]
        
        # Adding list of custom setup commands from user defined run options
        if self.run_options.get("custom-setup-commands", None):
            env_worker += self.run_options["custom-setup-commands"]

        # Now checking for conda environment  conda-env:true
        if self.run_options.get("conda-env", False):


            if "CONDA_PREFIX" in os.environ:
                env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')

            # # Check if CONDA_ROOT_PREFIX or MAMBA_ROOT_PREFIX is set
            # if "MAMBA_ROOT_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["MAMBA_ROOT_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'mamba activate {os.environ["CONDA_DEFAULT_ENV"]}')
            # elif "CONDA_ROOT_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["CONDA_ROOT_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'conda activate {os.environ["CONDA_DEFAULT_ENV"]}')
            # elif "CONDA_PREFIX" in os.environ: #currently active Conda environment.
            #     env_worker.append(f'source {os.environ["CONDA_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'conda activate {os.environ["CONDA_DEFAULT_ENV"]}')
            # elif "MAMBA_PREFIX" in os.environ:
            #     env_worker.append(f'source {os.environ["MAMBA_PREFIX"]}/etc/profile.d/conda.sh')
            #     env_worker.append(f'mamba activate {os.environ["MAMBA_DEFAULT_ENV"]}')
            # else:
            #     raise Exception("CONDA/Mamba_PREFIX or ROOT_PREFIX not found! Something is wrong with your Conda/Mamba installation.")

    
        # if local-virtual-env: true the dask job is configured to pickup
        # the local virtual environment. 
        if self.run_options.get("local-virtualenv", False):
            env_worker.append(f"source {sys.prefix}/bin/activate")

        return env_worker
    
        
    def setup(self):
        ''' Start the DASK cluster here'''

        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        from dask_jobqueue import SLURMCluster
        setup_dask(dask.config)

        print(">>> Creating a SLURM cluster")
        local_dir = os.environ.get("TMPDIR", f"/tmp/{os.environ['USER']}/slurm_localdir")
        os.makedirs(local_dir, exist_ok=True)

        self.dask_cluster = SLURMCluster(
            queue=self.run_options['queue'],
            cores=self.run_options.get('cores-per-worker', 1),
            processes=self.run_options.get('cores-per-worker', 1),
            memory=self.run_options['mem-per-worker'],
            walltime=self.run_options["walltime"],
            job_script_prologue=self.get_worker_env(),
            local_directory=local_dir,
            log_directory=os.path.join(self.outputdir, "slurm_log"),
        )
        print(self.get_worker_env())
        print(">> Checking environment inside SLURM job")
        print(">> Python version:", os.popen("python --version").read())
        print(">> Conda environment:", os.popen("conda info --envs").read())

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


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif  executor_name == "parsl":
        return ParslSlurmExecutorFactory(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, parsl, dask")