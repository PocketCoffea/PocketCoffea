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
        log_folder = "condor_log"

        ########################################################################
        # CHECK PARAMETERS FOR LPC CLUSTER
        ########################################################################

        self.dask_cluster = LPCCondorCluster(
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
    elif executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    #elif executor_name == "condor":
    #    return ExecutorFactoryCondorLPC(**kwargs)