import os, getpass
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port

import parsl
from parsl.providers import CondorProvider
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname

from pocket_coffea.parameters.dask_env import setup_dask
import dask.config
from dask_jobqueue import HTCondorCluster


class ParslCondorExecutorFactory(ExecutorFactoryABC):
    '''
    Parsl executor based on condor for RWTH LX cluster
    '''
    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        env_worker = [
            'echo \"Current date and time: `date`"',
            'echo "Hostname=`hostname`"',
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_USER_PROXY={self.x509_path}',
            f'export PYTHONPATH=$PYTHONPATH:{os.getcwd()}',
            'ulimit -s unlimited',
            f'cd {os.getcwd()}',
            ]

        if self.run_options.get("conda-env", False):
            if "CONDA_PREFIX" in os.environ:
                env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')
            elif "CONDA_ROOT_PREFIX" in os.environ:
                env_worker.append(f"{os.environ['CONDA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
            elif "MAMBA_ROOT_PREFIX" in os.environ:
                env_worker.append(f"{os.environ['MAMBA_EXE']} activate {os.environ['CONDA_DEFAULT_ENV']}")
            else:
                raise Exception("CONDA prefix not found in env! Something is wrong with your conda installation if you want to use conda on the cluster.")
            env_worker.append('echo "Conda has been activated, hopefully... We are ready to roll!"')

        # Adding list of custom setup commands from user defined run options
        if self.run_options.get("custom-setup-commands", None):
            'echo "Executing custom commands below"'
            env_worker += self.run_options["custom-setup-commands"]

        return env_worker


    def setup(self):
        ''' Start Condor cluster here'''
        self.setup_proxyfile()

        condor_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_condor",
                        address=address_by_hostname(),
                        max_workers_per_node=1,
                        worker_debug=self.run_options.get("worker-debug", False),
                        prefetch_capacity=0,
                        provider=CondorProvider(
                            nodes_per_block=1,
                            cores_per_slot=self.run_options.get("cores-per-worker", 1),
                            mem_per_slot=self.run_options.get("mem-per-worker", 4),
                            init_blocks=self.run_options["scaleout"],
                            max_blocks=self.run_options["scaleout"],
                            worker_init="\n".join(self.get_worker_env()),
                            walltime=self.run_options["walltime"],
                            requirements=self.run_options.get("requirements", ""),
                        ),
                    )
                ],
            retries=self.run_options["retries"],
	    run_dir="/tmp/"+getpass.getuser()+"/parsl_runinfo",
            )

        self.condor_cluster = parsl.load(condor_htex)
        print('Ready to run with parsl.')

    def get(self):
        return coffea_processor.parsl_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        # in the futures executor Nworkers == N scaleout
        #args["treereduction"] = self.run_options["tree-reduction"]
        return args

    def close(self):
        parsl.dfk().cleanup()
        parsl.clear()


class DaskExecutorFactory(ExecutorFactoryABC):
    '''
    DASK at RWTH LX cluster via HTCondor
    '''

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options)

    def get_worker_env(self):
        env_worker = [
            'export XRD_RUNFORKHANDLER=1',
            'export MALLOC_TRIM_THRESHOLD_=0',
            f'export X509_USER_PROXY={self.x509_path}',
            f'export PYTHONPATH=$PYTHONPATH:{os.getcwd()}',
            'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/.automount/net_rw/net__data_cms3a-1/andreypz/micromamba/envs/PocketCoffea/lib',
            'ulimit -s unlimited',
            ]

        # Adding list of custom setup commands from user defined run options
        if self.run_options.get("custom-setup-commands", None):
            env_worker += self.run_options["custom-setup-commands"]

        # Now checking for conda environment  conda-env:true
        if self.run_options.get("conda-env", False):
            env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')
            if "CONDA_ROOT_PREFIX" in os.environ:
                env_worker.append(f"{os.environ['CONDA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
            elif "MAMBA_ROOT_PREFIX" in os.environ:
                env_worker.append(f"{os.environ['MAMBA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
            else:
                raise Exception("CONDA prefix not found in env! Something is wrong with your conda installation if you want to use conda in the dask cluster.")

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

        print(">>> Creating an HTCondorCluster Dask cluster")
        self.dask_cluster = HTCondorCluster(
            cores  = self.run_options['cores-per-worker'],
            memory = self.run_options['mem-per-worker'],
            disk = self.run_options.get('disk-per-worker', "2GB"),
            job_script_prologue = self.get_worker_env(),
            log_directory = "/tmp/"+getpass.getuser()+"/dask_log",
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
    elif  executor_name == "parsl":
        return ParslCondorExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, parsl, dask")
