from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor
from dask.distributed import Client, PipInstall, WorkerPlugin
from distributed.diagnostics.plugin import UploadFile

def set_proxy(dask_worker):
    import os
    import shutil
    working_dir = dask_worker.local_directory
    print(working_dir)
    os.environ['X509_USER_PROXY'] = working_dir + '/proxy'
    os.environ['X509_CERT_DIR']="/cvmfs/grid.cern.ch/etc/grid-security/certificates/"
    os.chmod(working_dir + '/proxy', 0o400)
    return os.environ.get("X509_USER_PROXY")

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        if "sched-port" not in run_options:
            raise Exception("User need to specify `sched-port` in the custom run options!")
        self.sched_port = run_options["sched-port"]
        if "proxy-path" not in run_options:
            raise Exception("User need to specify `proxy-path` in the custom run options!")
        self.proxy_path = run_options["proxy-path"]
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        # At INFN AF, the best way to handle DASK clusters is to create them via the Dask labextension and then connect the client to it in your code
        self.dask_client = Client(address="tcp://127.0.0.1:"+str(self.sched_port))
        self.dask_client.restart()
        self.dask_client.register_worker_plugin(UploadFile(self.proxy_path))
        self.dask_client.run(set_proxy)
        
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
