import os
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import get_proxy_path

class DaskGatewayExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options, **kwargs)


    def get_worker_env(self):
        env_worker = {}
        propagate_env = [
            "NB_UID",
            "NB_GID",
            "PYTHONPATH",
            "LD_LIBRARY_PATH",
            "X509_CERT_DIR"
        ]
        for var in propagate_env:
            if var in os.environ:
                env_worker[var] = os.environ[var]

        if not self.run_options['ignore-grid-certificate']:
            if (
                (not self.x509_path.startswith("/depot/")) and 
                (not self.x509_path.startswith("/work/"))
            ):
                raise Exception(
                    "X509 certificate must be located under either /depot/ or /work/, "
                    "otherwise the Dask workers will not be able to use it."
                )

            env_worker["X509_USER_PROXY"] = self.x509_path

        return env_worker
    
    def setup_proxyfile(self):
        if self.run_options['ignore-grid-certificate']: return
        if self.run_options.get('voms-proxy', None) is not None:
             self.x509_path = self.run_options['voms-proxy']
        else:
             _x509_localpath = get_proxy_path()
             # Copy the proxy to the home from the /tmp to be used by workers
             self.x509_path = '/work/users/' + os.environ['USER'] + f'/{_x509_localpath.split("/")[-1]}'
             os.system(f'cp {_x509_localpath} {self.x509_path}')
    
    def setup(self):
        ''' Start the DASK cluster here'''
        self.setup_proxyfile()

        # Ensure that we are at Purdue AF
        if "purdue-af" not in socket.gethostname():
            raise Exception(
                "Trying to run with dask@purdue-af not at Purdue AF!"
                "Please try different runner options"
            )

        # Extract the prefix of current Conda environment
        conda_env = sys.executable.split("/bin/")[0]

        # Ensure that the Conda environment is visible to workers
        if (
            (not conda_env.startswith("/depot/")) and 
            (not conda_env.startswith("/work/"))
        ):
            raise Exception(
                "Please make sure that your Conda env is located under either /depot/ or /work/, "
                "otherwise the Dask workers will not be able to use it."
            )

        # Ensure that Dask Gateway is installed
        try:
            import dask_gateway
            from dask_gateway import Gateway
        except ModuleNotFoundError:
            raise Exception(
                "Executor dask@purdue-af requires Dask Gateway: `conda install dask-gateway`."
            )

        # Ensure that XRootD is installed
        try:
            import pyxrootd
        except ModuleNotFoundError:
            raise Exception(
                "Please install xrootd into your environment: `conda install xrootd==5.6.4`."
            )

        # Ensure that monitoring is enabled
        try:
            import prometheus_client
        except ModuleNotFoundError:
            raise Exception(
                "Please install prometheus_client into your environment: `conda install prometheus_client`."
            )

        # Connect to Dask Gateway API server
        gateway = Gateway(
            self.run_options['gateway-address'],
            proxy_address=self.run_options['gateway-proxy-address'],
            public_address=self.run_options['gateway-address']
        )

        print(">> Starting a Dask Gateway cluster")

        # Create the cluster
        self.dask_cluster = gateway.new_cluster(
           conda_env = conda_env,
           worker_cores = self.run_options['cores-per-worker'],
           worker_memory = self.run_options['mem-per-worker'],
           env = self.get_worker_env()
        )

        #Cluster adaptive number of jobs only if requested
        print(">> Sending out jobs")
        self.dask_cluster.adapt(
            minimum=1 if self.run_options["adaptive"] else self.run_options['scaleout'],
            maximum=self.run_options['scaleout']
        )

        print(f">> Click to open dashboard: {self.dask_cluster.dashboard_link}")

        print(">> Connecting to a client")
        self.dask_client = self.dask_cluster.get_client()

        print(">> Waiting for the first job to start...")
        self.dask_client.wait_for_workers(1)


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
    elif  executor_name == "dask":
        return DaskGatewayExecutorFactory(**kwargs)
