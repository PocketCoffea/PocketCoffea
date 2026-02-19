import os
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
#from .executors_manual_jobs import ExecutorFactoryManualABC
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

        # Spin up a HTCondor cluster for dask using lpcjobqueue
        from lpcjobqueue import LPCCondorCluster

        #if "lpc" not in socket.gethostname():
        #    raise Exception("Trying to run with dask/lpc not at LPC! Please try different runner options")

        n_port = self.run_options.get("dask-scheduler-port", 8786)
        print(">> Creating dask-lpc cluster transmitting on port:", n_port)
        if not check_port(n_port):
            raise RuntimeError(
                f"Port '{n_port}' is already occupied on this node. Change the port or try a different machine."
            )
        # Creating a LPC Cluster, special configuration for dask-on-lpc

        ########################################################################
        # CHECK PARAMETERS FOR LPC CLUSTER
        ########################################################################

        # Define log_directory using absolute path
        log_directory = os.path.abspath(f"{self.outputdir}/condor_log")
        print("log_directory:", log_directory)

        self.dask_cluster = LPCCondorCluster(
                cores=self.run_options['cores-per-worker'],
                memory=self.run_options['mem-per-worker'],
                disk=self.run_options['disk-per-worker'],
                #image_type="singularity",
                #worker_image=self.run_options["worker-image"],
                death_timeout=self.run_options["death-timeout"],
                scheduler_options={"port": n_port, "host": socket.gethostname()},
                log_directory = log_directory,
                # shared_temp_directory="/tmp"
                job_extra_directives={
                    "log": os.path.join(log_directory, "dask_job_output.log"),
                    "output": os.path.join(log_directory, "dask_job_output.out"),
                    "error": os.path.join(log_directory, "dask_job_output.err"),
                    "should_transfer_files": "Yes", #
                    "when_to_transfer_output": "ON_EXIT",
                    "+JobFlavour": f'"{self.run_options["queue"]}"'
                },
                job_script_prologue=get_worker_env(self.run_options,self.x509_path,"dask"),
            )

        ########################################################################
        # 
        ########################################################################

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
        #     self.performance_report_path = os.path.join(self.outputdir, f"{log_directory}/dask-report.html")
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
    elif executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    #elif executor_name == "condor":
    #    return ExecutorFactoryCondorLPC(**kwargs)