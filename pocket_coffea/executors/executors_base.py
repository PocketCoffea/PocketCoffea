import os
from coffea import processor as coffea_processor

class BaseExecutorFactory():

    def __init__(self):
        self.setup()

    def get(self):
        return None
   
    def setup(self):
        self.set_env()
 
    def set_env(self):
        # define some environmental variable
        # that are general enought to be always useful
        vars= {
            "XRD_RUNFORKHANDLER": "1",
            "MALLOC_TRIM_THRESHOLD_" : "0"
        }
        for k,v in vars.items():
            os.environ[k] = v

    def customize_args(self, args):
        return args

    def close(self):
        pass
    
class IterativeExecutorFactory(BaseExecutorFactory):

    def __init__(self):
        super().__init__()

    def get(self):
        return coffea_processor.iterative_executor


class FuturesExecutorFactory(BaseExecutorFactory):
    def __init__(self):
        super().__init__()

    def get(self):
        return coffea_processor.futures_executor

    
        

def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
