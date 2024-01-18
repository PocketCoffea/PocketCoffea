import os
from abc import ABC, abstractmethod
from coffea import processor as coffea_processor
from pocket_coffea.utils.network import get_proxy_path

class ExecutorFactoryABC(ABC):

    def __init__(self, run_options, **kwargs):
        self.run_options = run_options
        self.setup()

    @abstractmethod
    def get(self):
        return None

    def setup(self):
        self.setup_proxyfile()
        self.set_env()

    def setup_proxyfile(self):
        if self.run_options.get('voms-proxy', None) is not None:
             self.x509_path = self.run_options['voms-proxy']
        else:
             _x509_localpath = get_proxy_path()
             # Copy the proxy to the home from the /tmp to be used by workers
             self.x509_path = os.environ['HOME'] + f'/{_x509_localpath.split("/")[-1]}'
             os.system(f'cp {_x509_localpath} {self.x509_path}')
             
    def set_env(self):
        # define some environmental variable
        # that are general enought to be always useful
        vars= {
            "XRD_RUNFORKHANDLER": "1",
            "MALLOC_TRIM_THRESHOLD_" : "0",
            "X509_USER_PROXY": self.x509_path
        }
        for k,v in vars.items():
            os.environ[k] = v

    def customize_args(self, args):
        return args

    def close(self):
        pass
    
class IterativeExecutorFactory(ExecutorFactoryABC):

    def __init__(self):
        super().__init__()

    def get(self):
        return coffea_processor.iterative_executor


class FuturesExecutorFactory(ExecutorFactoryABC):
    def __init__(self):
        super().__init__()

    def get(self):
        return coffea_processor.futures_executor

    
        

def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
