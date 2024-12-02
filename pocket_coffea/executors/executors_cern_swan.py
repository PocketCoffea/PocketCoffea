from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor
from dask.distributed import Client, PipInstall, WorkerPlugin
from distributed.diagnostics.plugin import UploadFile


class PackageChecker(WorkerPlugin):
    '''Pluging to check that PocketCoffea is available.
    Sometimes the worker is not able to find the package,
    so we need to check if it is available and restart the worker if it is not.
    '''
    def __init__(self, package_name):
        self.package_name = package_name

    def setup(self, worker):
        try:
            from importlib.util import find_spec
            spec = find_spec(self.package_name)
            if spec is None:
                raise ImportError(f"Package {self.package_name} not found")
        except ImportError:
            worker.close(timeout=0, executor_wait=False, nanny=True, reason='eos-not-ready')
            

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        if "sched-url" not in run_options or run_options["sched-url"] is None:
            raise Exception("`sched-url` key not provided in the custom run options! Please provide the URL of the Dask scheduler!")
        self.sched_url = run_options["sched-url"]
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        # At INFN AF, the best way to handle DASK clusters is to create them via the Dask labextension and then connect the client to it in your code
        self.dask_client = Client(address=self.sched_url)
        #self.dask_client.register_worker_plugin(PackageChecker("pocket_coffea"))
        
    def customized_args(self):
        args = super().customized_args()
        args["client"] = self.dask_client
        args["treereduction"] = self.run_options["tree-reduction"]
        args["retries"] = self.run_options["retries"]
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
