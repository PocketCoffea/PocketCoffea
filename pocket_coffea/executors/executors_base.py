import os
from abc import ABC, abstractmethod
from coffea import processor as coffea_processor
from pocket_coffea.utils.network import get_proxy_path

class ExecutorFactoryABC(ABC):

    def __init__(self, run_options, **kwargs):
        self.run_options = run_options
        self.setup()
        # If handles_submission == False, the executor is not responsible for submitting the jobs
        self.handles_submission = False

    @abstractmethod
    def get(self):
        pass

    def setup(self):
        self.setup_proxyfile()
        self.set_env()

    def setup_proxyfile(self):
        if self.run_options['ignore-grid-certificate']: return
        if vomsproxy:=self.run_options.get('voms-proxy', None) is not None:
             self.x509_path = vomsproxy
        else:
             _x509_localpath = get_proxy_path()
             # Copy the proxy to the home from the /tmp to be used by workers
             self.x509_path = os.environ['HOME'] + f'/{_x509_localpath.split("/")[-1]}'
             if _x509_localpath != self.x509_path:
                 print("Copying proxy file to $HOME.")
                 os.system(f'scp {_x509_localpath} {self.x509_path}')  # scp makes sure older file is overwritten without prompting
             
    def set_env(self):
        # define some environmental variable
        # that are general enought to be always useful
        vars= {
            "XRD_RUNFORKHANDLER": "1",
            "MALLOC_TRIM_THRESHOLD_" : "0",
        }
        if not self.run_options['ignore-grid-certificate']:
            vars["X509_USER_PROXY"] = self.x509_path
        for k,v in vars.items():
            os.environ[k] = v

    def customized_args(self):
        return {}

    def close(self):
        pass
    
class IterativeExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options,  **kwargs):
        super().__init__(run_options, **kwargs)

    def get(self):
        return coffea_processor.iterative_executor(**self.customized_args())


class FuturesExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, **kwargs):
        super().__init__(run_options, **kwargs)

    def get(self):
        return coffea_processor.futures_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        # in the futures executor Nworkers == N scaleout
        args["workers"] = self.run_options["scaleout"]
        return args

        

def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
