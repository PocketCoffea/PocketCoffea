import os
import sys
import json
import argparse
import time
import pickle
import socket

import uproot
from coffea.nanoevents import NanoEventsFactory
from coffea.util import load, save
from coffea import processor
from pprint import pprint


print("""
    ____             __        __  ______      ________          
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ / 
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/  
                                                                 
""")

from pocket_coffea.utils.Configurator import Configurator

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run analysis on baconbits files using processor coffea files')
    # Inputs
    parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", required=True, type=str,
                        help='Config file with parameters specific to the current run')
    parser.add_argument("-o", "--outputdir", required=False, type=str,
                        help="Overwrite the output folder in the configuration")
    parser.add_argument("-t", "--test", action="store_true", help="Run with limit 1 interactively")
    parser.add_argument("-lf","--limit-files", type=int, help="Limit number of files")
    parser.add_argument("-lc","--limit-chunks", type=int, help="Limit number of chunks", default=None)
    parser.add_argument("-e","--executor", type=str,
                        help="Overwrite executor from config (to be used only with the --test options)" )
    parser.add_argument("-s","--scaleout", type=int, help="Overwrite scalout config" )
    args = parser.parse_args()

    if args.cfg[-3:] == ".py":
        config = Configurator(args.cfg, overwrite_output_dir=args.outputdir)
    elif args.cfg[-4:] == ".pkl":
        config = pickle.load(open(args.cfg,"rb"))
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")

    if args.test:
        config.run_options["executor"] = args.executor if args.executor else "iterative"
        config.run_options["limit"] = args.limit_files if args.limit_files else 1
        config.run_options["max"] = args.limit_chunks if args.limit_chunks else 2
        config.filter_dataset(config.run_options["limit"])

    if args.limit_files!=None:
        config.run_options["limit"] = args.limit_files
        config.filter_dataset(config.run_options["limit"])

    if args.limit_chunks!=None:
        config.run_options["max"] = args.limit_chunks

    if args.scaleout!=None:
        config.run_options["scaleout"] = args.scaleout

    if args.executor !=None:
        config.run_options["executor"] = args.executor


    
    #### Fixing the environment (assuming this is run in singularity)
    # dask/parsl needs to export x509 to read over xrootd
    if config.run_options['voms'] is not None:
        _x509_path = config.run_options['voms']
    else:
        try:
            _x509_localpath = [l for l in os.popen('voms-proxy-info').read().split("\n") if l.startswith('path')][0].split(":")[-1].strip()
            _x509_path = os.environ['HOME'] + f'/.{_x509_localpath.split("/")[-1]}'
            os.system(f'cp {_x509_localpath} {_x509_path}')
        except:
             raise Exception("VOMS proxy expirend or non-existing: please run `voms-proxy-init -voms cms -rfc --valid 168:0`")
    env_extra = [
        'export XRD_RUNFORKHANDLER=1',
        f'export X509_USER_PROXY={_x509_path}',
        # f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
        'ulimit -u 32768',
    ]

    wrk_init = [
        'export XRD_RUNFORKHANDLER=1',
        f'export X509_USER_PROXY={_x509_path}',
        # f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
        f'source {sys.prefix}/bin/activate',
    ]


    #########
    # Executors
    if config.run_options['executor'] in ['futures', 'iterative']:
        # source the environment variables
        os.environ["XRD_RUNFORKHANDLER"] = "1"
        os.environ["X509_USER_PROXY"] = _x509_path

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
                                    chunksize=config.run_options['chunk'],
                                    maxchunks=config.run_options['max']
                                    )

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
                            partition=config.run_options['queue'],
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

    # DASK runners
    elif 'dask' in config.run_options['executor']:
        from dask_jobqueue import SLURMCluster, HTCondorCluster
        from distributed import Client
        from dask.distributed import performance_report

        if 'slurm' in config.run_options['executor']:
            cluster = SLURMCluster(
                queue=config.run_options['partition'],
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
        elif 'lxplus' in config.run_options["executor"]:
            from PocketCoffea.utils.network import check_port

            if "lxplus" not in socket.gethostname():
                raise Exception("Trying to run with dask/lxplus not at CERN! Please try different runner options")
            from dask_lxplus import CernCluster
            n_port = 8786
            if not check_port(8786):
                raise RuntimeError(
                    "Port '8786' is already occupied on this node. Try another machine."
                )
            os.makedirs(f"{config.output}/condor_job")
            cluster = CernCluster(
                cores=1,
                memory=config.run_options['mem_per_worker'],
                disk=config.run_options.get('disk_per_worker', "20GB"),
                image_type="singularity",
                worker_image="/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest",
                death_timeout="3600",
                scheduler_options={"port": n_port, "host": socket.gethostname()},
                log_directory = f"{config.output}/condor_log",
                # shared_temp_directory="/tmp"
                job_extra={
                    "log": f"{config.output}/condor_log/dask_job_output.log",
                    "output": f"{config.output}/condor_log/dask_job_output.out",
                    "error": f"{config.output}/condor_log/dask_job_output.err",
                    "should_transfer_files": "Yes",
                    "when_to_transfer_output": "ON_EXIT",
                    "+JobFlavour": f'"{config.run_options["queue"]}"'
                },
                env_extra=wrk_init,
            )

        #Cluster adaptive number of jobs only if requested
        cluster.adapt(minimum=1 if config.run_options.get("adapt", False)
                                else config.run_options['scaleout'],
                      maximum=config.run_options['scaleout'])
        client = Client(cluster)
        client.wait_for_workers(1)

        with performance_report(filename=os.path.join(config.output, "condor_log/dask-report.html")):
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
                                        chunksize=config.run_options['chunk'],
                                        maxchunks=config.run_options['max']
                            )
    else:
        print(f"Executor {config.run_options['executor']} not defined!")
        exit(1)
    save(output, config.outfile)
    print(f"Saving output to {config.outfile}")
