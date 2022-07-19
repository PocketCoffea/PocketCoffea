import os
import sys
import json
import argparse
import time
import pickle
import numpy as np

import uproot
from coffea import hist
from coffea.nanoevents import NanoEventsFactory
from coffea.util import load, save
from coffea import processor
from pprint import pprint

from PocketCoffea.utils.Configurator import Configurator

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run analysis on baconbits files using processor coffea files')
    # Inputs
    parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", required=True, type=str,
                        help='Config file with parameters specific to the current run')
    parser.add_argument("-o", "--output-dir", required=False, type=str,
                        help="Overwrite the output folder in the configuration")
    # parser.add_argument("-l", "--limit-files", required=False, type=int,
    #                     help="Overwrite number of files limit")
    args = parser.parse_args()

    if args.cfg[-3:] == ".py":
        config = Configurator(args.cfg, overwrite_output_dir=args.output_dir)
    elif args.cfg[-4:] == ".pkl":
        config = pickle.load(open(args.cfg,"rb"))
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")

    if config.run_options['executor'] not in ['futures', 'iterative']:
        # dask/parsl needs to export x509 to read over xrootd
        if config.run_options['voms'] is not None:
            _x509_path = config.run_options['voms']
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
            'cd /afs/cern.ch/work/m/mmarcheg/PocketCoffea/', # TO BE GENERALIZED
        ]

        condor_cfg = '''
        getenv      =  True
        +JobFlavour =  "nextweek"
        '''
        #process_worker_pool = os.environ['CONDA_PREFIX'] + "/bin/process_worker_pool.py"

    #########
    # Execute
    if config.run_options['executor'] in ['futures', 'iterative']:
        if config.run_options['executor'] == 'iterative':
            _exec = processor.iterative_executor
        else:
            _exec = processor.futures_executor
        output = processor.run_uproot_job(config.fileset,
                                    treename='Events',
                                    processor_instance=config.processor_instance,
                                    executor=_exec,
                                    executor_args={
                                        'skipbadfiles':config.run_options['skipbadfiles'],
                                        'schema': processor.NanoAODSchema,
                                        'workers': config.run_options['scaleout']},
                                    chunksize=config.run_options['chunk'], maxchunks=config.run_options['max']
                                    )
    #elif config.run_options['executor'] == 'parsl/slurm':
    elif 'parsl' in config.run_options['executor']:
        import parsl
        from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
        from parsl.channels import LocalChannel
        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor
        from parsl.launchers import SrunLauncher, SingleNodeLauncher
        from parsl.addresses import address_by_hostname

        if 'slurm' in config.run_options['executor']:
            slurm_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_slurm",
                        address=address_by_hostname(),
                        prefetch_capacity=0,
                        mem_per_worker=config.run_options['mem_per_worker'],
                        provider=SlurmProvider(
                            channel=LocalChannel(script_dir='logs_parsl'),
                            launcher=SrunLauncher(),
                            #launcher=SingleNodeLauncher(),
                            max_blocks=(config.run_options['scaleout'])+10,
                            init_blocks=config.run_options['scaleout'],
                            partition=config.run_options['partition'],
                            worker_init="\n".join(env_extra) + "\nexport PYTHONPATH=$PYTHONPATH:$PWD",
                            walltime=config.run_options['walltime'],
                            exclusive=config.run_options['exclusive'],
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
                                        chunksize=config.run_options['chunk'], maxchunks=config.run_options['max']
                                        )
        elif 'condor' in config.run_options['executor']:
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
                            max_blocks=(config.run_options['scaleout'])+10,
                            init_blocks=config.run_options['scaleout'],
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
                                        chunksize=config.run_options['chunk'], maxchunks=config.run_options['max']
                                        )
    elif 'dask' in config.run_options['executor']:
        from dask_jobqueue import SLURMCluster, HTCondorCluster
        from distributed import Client
        from dask.distributed import performance_report

        if 'slurm' in config.run_options['executor']:
            cluster = SLURMCluster(
                queue='standard',
                cores=config.run_options['workers'],
                processes=config.run_options['workers'],
                memory=config.run_options['mem_per_worker'],
                walltime=config.run_options["walltime"],
                env_extra=env_extra,
                #job_extra=[f'-o {os.path.join(config.output, "slurm-output", "slurm-%j.out")}'],
                local_directory=os.path.join(config.output, "slurm-output"),
            )
        elif 'condor' in config.run_options['executor']:
            cluster = HTCondorCluster(
                 cores=config.run_options['workers'],
                 memory='2GB',
                 disk='2GB',
                 env_extra=env_extra,
            )
        cluster.scale(jobs=config.run_options['scaleout'])

        client = Client(cluster)
        with performance_report(filename=os.path.join(config.output, "dask-report.html")):
            output = processor.run_uproot_job(config.fileset,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.dask_executor,
                                        executor_args={
                                            'client': client,
                                            'skipbadfiles':config.run_options['skipbadfiles'],
                                            'schema': processor.NanoAODSchema,
                                            'retries' : config.run_options['retries'],
                                            'treereduction' : config.run_options.get('treereduction', 20)
                                        },
                                        chunksize=config.run_options['chunk'], maxchunks=config.run_options['max']
                            )
    else:
        print(f"Executor {config.run_options['executor']} not defined!")
        exit(1)
    save(output, config.outfile)
    pprint(output)
    print(f"Saving output to {config.outfile}")
