from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor
from dask.distributed import Client, PipInstall, WorkerPlugin
from distributed.diagnostics.plugin import UploadFile

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, sched_port, proxy_path, outputdir, **kwargs):
        self.outputdir = outputdir
        self.sched_port = sched_port
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        # At coffea-casa we have preconfigured Dask HTCondor cluster for you, please just use it available at tls://localhost:8786
        from distributed import Client
        self.dask_client = Client(address="tcp://127.0.0.1:"+str(self.sched_port))
        self.dask_client.restart()
        self.dask_client.register_worker_plugin(UploadFile(proxy_path))
        
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
