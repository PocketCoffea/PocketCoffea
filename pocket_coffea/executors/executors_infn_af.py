from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor
from dask.distributed import Client, PipInstall, Worker, WorkerPlugin
from distributed.diagnostics.plugin import UploadFile
import time

class FuturesExecutorFactoryINFN(FuturesExecutorFactory):
    def set_env(self):
        super().set_env()
        os.environ['X509_CERT_DIR']="/cvmfs/grid.cern.ch/etc/grid-security/certificates/"

class IterativeExecutorFactoryINFN(IterativeExecutorFactory):
    def set_env(self):
        super().set_env()
        os.environ['X509_CERT_DIR']="/cvmfs/grid.cern.ch/etc/grid-security/certificates/"

class SetProxyPlugin(WorkerPlugin):

    def __init__(self, proxy_name='proxy'):
        self.proxy_name = proxy_name
        
    async def setup(self, worker: Worker):
        import os
        working_dir = worker.local_directory
        os.environ['X509_USER_PROXY'] = working_dir + '/' + self.proxy_name
        os.environ['X509_CERT_DIR']="/cvmfs/grid.cern.ch/etc/grid-security/certificates/"
        try:
            os.chmod(working_dir + '/' + self.proxy_name, 0o400)
        except:
            print("Unable to modify proxyfile permissions, they may be already modified")

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        if "sched-url" not in run_options or run_options["sched-url"] is None:
            raise Exception("User need to specify `sched-url` in the custom run options! Please provide the URL of the Dask scheduler!")
        self.sched_url = run_options["sched-url"]
        if "voms-proxy" not in run_options or run_options["voms-proxy"] is None:
            raise Exception("User need to specify `voms-proxy` in the custom run options!")
        self.proxy_path = run_options["voms-proxy"]
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        # At INFN AF, the best way to handle DASK clusters is to create them via the Dask labextension and then connect the client to it in your code
        self.dask_client = Client(address=str(self.sched_url))
        self.dask_client.restart()
        try:
            self.dask_client.register_worker_plugin(UploadFile(self.proxy_path))
        except:
            print("Unable to upload proxyfile, it may be already uploaded")
        # get file name from path
        self.dask_client.register_worker_plugin(SetProxyPlugin(proxy_name=self.proxy_path.split("/")[-1]))
        
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
        return IterativeExecutorFactoryINFN(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactoryINFN(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
