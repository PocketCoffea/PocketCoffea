from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor
    

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        # At coffea-casa we have preconfigured Dask HTCondor cluster for you, please just use it available at tls://localhost:8786
        from distributed import Client
        self.dask_client = Client("tls://localhost:8786")
        # Install pocket_coffea to dask workers via Dask scheduler plugin PipInstall
        # PLEASE CHANGE if you are using fork or different tag / branch
        from dask.distributed import PipInstall
        plugin = PipInstall(packages=["git+https://github.com/PocketCoffea/PocketCoffea.git@main"])
        self.dask_client.register_plugin(plugin)
        # Making sure all workers have uptodate configuration
        self.dask_client.restart()
        
    def customized_args(self):
        args = super().customized_args()
        args["client"] = self.dask_client
        return args
    
    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def close(self):
        self.dask_client.close()

def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
