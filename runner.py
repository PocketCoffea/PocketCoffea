import os
import sys
import json
import argparse
import time

import numpy as np

import uproot
from coffea import hist
from coffea.nanoevents import NanoEventsFactory
from coffea.util import load, save
from coffea import processor

from utils.Configurator import Configurator

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run analysis on baconbits files using processor coffea files')
    # Inputs
    parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", help='Config file with parameters specific to the current run', required=False)

    args = parser.parse_args()
    config = Configurator(args.cfg)

    if config.executor not in ['futures', 'iterative']:
        # dask/parsl needs to export x509 to read over xrootd
        if config.voms is not None:
            _x509_path = config.voms
        else:
            _x509_localpath = [l for l in os.popen('voms-proxy-info').read().split("\n") if l.startswith('path')][0].split(":")[-1].strip()
            _x509_path = os.environ['HOME'] + f'/.{_x509_localpath.split("/")[-1]}'
            os.system(f'cp {_x509_localpath} {_x509_path}')

        env_extra = [
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_USER_PROXY={_x509_path}',
            f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
            'ulimit -u 32768',
        ]

        wrk_init = [
            f'export X509_USER_PROXY={_x509_path}',
            f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
            'source /etc/profile.d/conda.sh',
            'export PATH=$CONDA_PREFIX/bin:$PATH',
            'conda activate coffea',
            'cd /afs/cern.ch/work/m/mmarcheg/PocketCoffea/',
        ]

        condor_cfg = '''
        getenv      =  True
        +JobFlavour =  "nextweek"
        '''
        #process_worker_pool = os.environ['CONDA_PREFIX'] + "/bin/process_worker_pool.py"

    #########
    # Execute
    if config.executor in ['futures', 'iterative']:
        if config.executor == 'iterative':
            _exec = processor.iterative_executor
        else:
            _exec = processor.futures_executor
        output = processor.run_uproot_job(config.fileset,
                                    treename='Events',
                                    processor_instance=config.processor_instance,
                                    executor=_exec,
                                    executor_args={
                                        'skipbadfiles':config.skipbadfiles,
                                        'schema': processor.NanoAODSchema,
                                        'workers': config.workers},
                                    chunksize=config.chunk, maxchunks=config.max
                                    )
    #elif config.executor == 'parsl/slurm':
    elif 'parsl' in config.executor:
        import parsl
        from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
        from parsl.channels import LocalChannel
        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor
        from parsl.launchers import SrunLauncher, SingleNodeLauncher
        from parsl.addresses import address_by_hostname

        if 'slurm' in config.executor:
            slurm_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_slurm",
                        address=address_by_hostname(),
                        prefetch_capacity=0,
                        provider=SlurmProvider(
                            channel=LocalChannel(script_dir='logs_parsl'),
                            launcher=SrunLauncher(),
                            #launcher=SingleNodeLauncher(),
                            max_blocks=(config.scaleout)+10,
                            init_blocks=config.scaleout,
                            #partition='long',
                            partition='standard',
                            worker_init="\n".join(env_extra) + "\nexport PYTHONPATH=$PYTHONPATH:$PWD",
                            walltime='12:00:00'
                        ),
                    )
                ],
                retries=20,
            )
            dfk = parsl.load(slurm_htex)

            output = processor.run_uproot_job(config.fileset,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.parsl_executor,
                                        executor_args={
                                            'skipbadfiles':True,
                                            'schema': processor.NanoAODSchema,
                                            'config': None,
                                        },
                                        chunksize=config.chunk, maxchunks=config.max
                                        )
        elif 'condor' in config.executor:
            #xfer_files = [process_worker_pool, _x509_path]
            #print(xfer_files)

            condor_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_slurm",
                        #address=address_by_hostname(),
                        worker_ports=(8786,8785),
                        prefetch_capacity=0,
                        provider=CondorProvider(
                            channel=LocalChannel(script_dir='logs_parsl'),
                            launcher=SingleNodeLauncher(),
                            max_blocks=(config.scaleout)+10,
                            init_blocks=config.scaleout,
                            worker_init="\n".join(wrk_init),
                            #transfer_input_files=xfer_files,
                            scheduler_options=condor_cfg,
                            walltime='00:30:00'
                        ),
                    )
                ],
                #retries=20,
            )
            dfk = parsl.load(condor_htex)

            output = processor.run_uproot_job(config.fileset,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.parsl_executor,
                                        executor_args={
                                            'skipbadfiles':True,
                                            'schema': processor.NanoAODSchema,
                                            'config': None,
                                        },
                                        chunksize=config.chunk, maxchunks=config.max
                                        )
    elif 'dask' in config.executor:
        from dask_jobqueue import SLURMCluster, HTCondorCluster
        from distributed import Client
        from dask.distributed import performance_report

        if 'slurm' in config.executor:
            cluster = SLURMCluster(
                queue='all',
                cores=config.workers,
                processes=config.workers,
                memory="200 GB",
                retries=10,
                walltime='00:30:00',
                env_extra=env_extra,
            )
        elif 'condor' in config.executor:
            cluster = HTCondorCluster(
                 cores=config.workers,
                 memory='2GB',
                 disk='2GB',
                 env_extra=env_extra,
            )
        cluster.scale(jobs=config.scaleout)

        client = Client(cluster)
        with performance_report(filename="dask-report.html"):
            output = processor.run_uproot_job(config.fileset,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.dask_executor,
                                        executor_args={
                                            'client': client,
                                            'skipbadfiles':config.skipbadfiles,
                                            'schema': processor.NanoAODSchema,
                                        },
                                        chunksize=config.chunk, maxchunks=config.max
                            )

    save(output, config.output)
    print(output)
    print(f"Saving output to {config.output}")
