import os
import logging

import dask.config
from dask_jobqueue import SLURMCluster, HTCondorCluster
from distributed import Client
from dask.distributed import performance_report
from coffea import processor
from coffea.util import save

from pocket_coffea.parameters.dask_env import setup_dask
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.logging import setup_logging
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils import utils


class BaseRunner:
    def __init__(self, architecture, scheduler, cfg, output_dir, loglevel="INFO"):
        self.architecture = architecture
        self.scheduler = scheduler
        self.cfg = cfg
        self.config_dir = os.path.dirname(cfg)
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "output_{}.coffea")
        self.setup_logging(loglevel)
        self.load_config()
        self.load_proxy()
        self.load_env_extra()

    def setup_logging(self, loglevel):
        if (not setup_logging(console_log_output="stdout", console_log_level=loglevel, console_log_color=True,
                        logfile_file="last_run.log", logfile_log_level="info", logfile_log_color=False,
                        log_line_template="%(color_on)s[%(levelname)-8s] %(message)s%(color_off)s")):
            print("Failed to setup logging, aborting.")
            exit(1)

    def load_config(self):
        config_module =  utils.path_import(self.cfg)
        try:
            config = config_module.cfg
            logging.info(config)
            config.save_config(self.output_dir)

        except AttributeError as e:
            print("Error: ", e)
            raise("The provided configuration module does not contain a `cfg` attribute of type Configurator. Please check your configuration!")

        if not isinstance(config, Configurator):
            raise("The configuration module attribute `cfg` is not of type Configurator. Please check yuor configuration!")

        #TODO improve the run options config
        self.cfg = config
        self.filesets = self.cfg.filesets
        self.processor_instance = self.cfg.processor_instance
        self.run_options = config_module.run_options

    def load_proxy(self):
        if self.run_options.get('voms', None) is not None:
            self._x509_path = self.run_options['voms']
        else:
            _x509_localpath = get_proxy_path()
            self._x509_path = os.environ['HOME'] + f'/{_x509_localpath.split("/")[-1]}'
            os.system(f'cp {_x509_localpath} {self._x509_path}')

    def load_env_extra(self):
        self.env_extra = [
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_USER_PROXY={self._x509_path}',
            # f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
            'source /etc/profile.d/conda.sh',
            f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH',
            f'conda activate {os.environ["CONDA_DEFAULT_ENV"]}',
            'ulimit -u 32768',
            'export MALLOC_TRIM_THRESHOLD_=0'
        ]        
        self.env_extra.append(f'export PYTHONPATH={self.config_dir}:$PYTHONPATH')

    def get_executor_args(self):
        executor_args = {
            'skipbadfiles': self.run_options.get('skipbadfiles',False),
            'schema': processor.NanoAODSchema,
            'retries' : self.run_options['retries'],
            'treereduction' : self.run_options.get('treereduction', 20),
            'xrootdtimeout': self.run_options.get('xrootdtimeout', 600),
        }
        if hasattr(self, "client"):
            executor_args.update({'client': self.client})

        return executor_args

    def setup_cluster(self):
        # Here the cluster configuration has to be defined for sub-classes
        pass

    def run_fileset(self, fileset):
        return processor.run_uproot_job(fileset,
                                        treename='Events',
                                        processor_instance=self.processor_instance,
                                        executor=processor.dask_executor,
                                        executor_args=self.get_executor_args(),
                                        chunksize=self.run_options['chunk'],
                                        maxchunks=self.run_options.get('max', None)
                                        )

    def run(self, full=False):
        # This method has to be overridden in the sub-class definition 
        if not hasattr(self, "cluster"):
            raise Exception("The Runner object has no attribute 'cluster'. Please review the cluster definition in the 'setup_cluster()' method.")
        pass

class DaskRunner(BaseRunner):
    def __init__(self, architecture, cfg, output_dir):
        super().__init__(architecture, scheduler="dask", cfg=cfg, output_dir=output_dir)
            
        setup_dask(dask.config)
        self.setup_cluster()

    def setup_cluster(self):
        if self.architecture == 'slurm':
            self.log_folder = "slurm_log"
            self.cluster = SLURMCluster(
                queue=self.run_options['queue'],
                cores=self.run_options['workers'],
                processes=self.run_options['workers'],
                memory=self.run_options['mem_per_worker'],
                walltime=self.run_options["walltime"],
                env_extra=self.env_extra,
                local_directory=os.path.join(self.output_dir, self.log_folder),
            )
        else:
            raise NotImplementedError
        
    def start_client(self):
        logging.info(f">> Starting the Dask client: sending out {self.run_options['scaleout']} jobs.")
        self.cluster.adapt(minimum=1 if self.run_options.get("adapt", False) else self.run_options['scaleout'],
                           maximum=self.run_options['scaleout'])
        self.client = Client(self.cluster)
        logging.info(">> Waiting for the first job to start...")
        self.client.wait_for_workers(1)
        logging.info(">> You can connect to the Dask viewer at http://localhost:8787")

    def run(self, full=False):
        super().run(full=full)
        self.start_client()

        performance_report_path = os.path.join(self.output_dir, f"{self.log_folder}/dask-report.html")
        print(f"Saving performance report to {performance_report_path}")

        with performance_report(filename=performance_report_path):

            if full:
                # Running separately on each dataset
                logging.info(f"Working on samples: {list(self.filesets.keys())}")
                
                output = self.run_fileset(self.filesets)
                print(f"Saving output to {self.output_file.format('all')}")
                save(output, self.output_file.format('all') )
            else:
                # Running separately on each dataset
                for sample, files in self.filesets.items():
                    logging.info(f"Working on sample: {sample}")
                    fileset = {sample:files}

                    output = self.run_fileset(fileset)
                    print(f"Saving output to {self.output_file.format(sample)}")
                    save(output, self.output_file.format(sample))
