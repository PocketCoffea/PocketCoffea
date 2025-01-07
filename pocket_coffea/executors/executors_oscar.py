import os, getpass
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port

from pocket_coffea.parameters.dask_env import setup_dask
import dask.config
from dask_jobqueue import HTCondorCluster
        
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
            f'export PYTHONPATH={os.getcwd()}',
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
                local_directory=os.path.join("/gpfs/scratch", os.environ["USER"], "slurm_localdir"),
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


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, dask")
