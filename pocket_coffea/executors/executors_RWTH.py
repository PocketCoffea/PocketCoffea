import os, getpass
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask

import parsl
from parsl.providers import CondorProvider, LocalProvider, SlurmProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher, SingleNodeLauncher
from parsl.addresses import address_by_hostname, address_by_query


class ParslCondorExecutorFactory(ExecutorFactoryABC):
    '''
    Parsl executor based on condor for RWTH
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
            'ulimit -u 32768',
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
        print("All run_options:", self.run_options)
        
        ''' Start Condor cluster here'''
        self.setup_proxyfile()

        condor_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_condor",
                        address=address_by_hostname(),
                        max_workers=1,
                        worker_debug=False,
                        prefetch_capacity=0,
                        provider=CondorProvider(
                            nodes_per_block=1,
                            cores_per_slot=self.run_options.get("cores-per-worker", 1),
                            mem_per_slot=self.run_options.get("mem_per_worker_parsl", 2),
                            init_blocks=self.run_options["scaleout"],
                            max_blocks=(self.run_options["scaleout"]) + 5,
                            worker_init="\n".join(self.get_worker_env()),
                            walltime=self.run_options["walltime"],
                            requirements=self.run_options.get("requirements", ""),
                            #transfer_input_files=xfer_files,
                            #scheduler_options=condor_cfg,

                        ),
                    )
                ],
            retries=self.run_options["retries"],
	    run_dir="/tmp/"+getpass.getuser()+"/parsl_runinfo",
            )

        self.condor_cluster = parsl.load(condor_htex)
        print('Ready to run with parsl')

    def get(self):
        return coffea_processor.parsl_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        # in the futures executor Nworkers == N scalout
        #args["treereduction"] = self.run_options["tree-reduction"]
        return args

    def close(self):
        parsl.dfk().cleanup()
        parsl.clear()



def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "parsl-condor":
        return ParslCondorExecutorFactory(**kwargs)
    else:
        print("The executor is not recognized!\n available executors are: iterative, futures, parsl-condor")
