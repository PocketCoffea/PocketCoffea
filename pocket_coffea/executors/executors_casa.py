from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from pocket_coffea.executors.executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from coffea import processor as coffea_processor


class DaskExecutorFactory(ExecutorFactoryABC):
    '''Dask executor for coffea-casa (https://coffea-casa.readthedocs.io).

    By default a CoffeaCasaCluster (HTCondor workers) is created with `scaleout`
    workers, or adaptively up to `scaleout` with `adaptive: true`, and closed at
    the end. To use a cluster you already started instead (e.g. from the Dask
    sidebar), pass its address as `scheduler-address` or in
    DASK_SCHEDULER_ADDRESS; it is left running.

    Nothing is installed on the workers and the client is never restarted:
    workers run the image's environment (or the notebook's conda environment,
    which CoffeaCasaCluster ships to them), and restarting a client on
    coffea-casa removes the HTCondor workers instead of restarting them.
    '''

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        self.dask_cluster = None
        super().__init__(run_options, **kwargs)

    def setup(self):
        # Use a grid proxy if there is one: set_env() exports X509_USER_PROXY
        # and CoffeaCasaCluster then ships the proxy to the workers. Without
        # one, read data with coffea-casa's tokens (through XCache).
        if not self.run_options["ignore-grid-certificate"]:
            try:
                self.setup_proxyfile()
            except Exception as e:
                print(f"No usable VOMS proxy ({e}); reading data with "
                      "coffea-casa's tokens instead.")
                self.run_options["ignore-grid-certificate"] = True
        self.set_env()
        import dask
        from distributed import Client
        address = (self.run_options.get("scheduler-address")
                   or dask.config.get("scheduler-address", None))
        if address:
            print(f"Connecting to the Dask cluster at {address}")
            self.dask_client = Client(address)
            return

        from coffea_casa import CoffeaCasaCluster
        self.dask_cluster = CoffeaCasaCluster(
            cores=self.run_options["cores-per-worker"],
            memory=self.run_options["mem-per-worker"],
        )
        if self.run_options["adaptive"]:
            self.dask_cluster.adapt(minimum=1, maximum=self.run_options["scaleout"])
        else:
            self.dask_cluster.scale(self.run_options["scaleout"])
        self.dask_client = Client(self.dask_cluster)
        print(f"Started a CoffeaCasaCluster: {self.dask_client.dashboard_link}")

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
        if self.dask_cluster is not None:
            self.dask_cluster.close()


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
