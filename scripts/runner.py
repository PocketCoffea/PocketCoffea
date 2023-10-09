#!/usr/bin/env python
print("""
    ____             __        __  ______      ________          
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ / 
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/  
                                                                 
""")

import os
import sys
import argparse
import cloudpickle
import socket
import logging

from coffea.util import save
from coffea import processor

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils import utils
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.logging import setup_logging

def load_run_options(cfg):
    config_module =  utils.path_import(cfg)
    try:
        config = config_module.cfg
        logging.info(config)
        config.save_config(args.outputdir)

    except AttributeError as e:
        print("Error: ", e)
        raise("The provided configuration module does not contain a `cfg` attribute of type Configurator. Please check your configuration!")

    if not isinstance(config, Configurator):
        raise("The configuration module attribute `cfg` is not of type Configurator. Please check yuor configuration!")

    #TODO improve the run options config
    return config_module.run_options


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run analysis on NanoAOD files using PocketCoffea processors')
    # Inputs
    parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", required=True, type=str,
                        help='Config file with parameters specific to the current run')
    parser.add_argument("-o", "--outputdir", required=True, type=str, help="Output folder")
    parser.add_argument("-t", "--test", action="store_true", help="Run with limit 1 interactively")
    parser.add_argument("-lf","--limit-files", type=int, help="Limit number of files")
    parser.add_argument("-lc","--limit-chunks", type=int, help="Limit number of chunks", default=None)
    parser.add_argument("-e","--executor", type=str,
                        help="Overwrite executor from config (to be used only with the --test options)" )
    parser.add_argument("-s","--scaleout", type=int, help="Overwrite scalout config" )
    parser.add_argument("-ll","--loglevel", type=str, help="Logging level", default="INFO" )
    parser.add_argument("-f","--full", action="store_true", help="Process all datasets at the same time", default=False )
    args = parser.parse_args()


    # Setting up the output dir
    os.makedirs(args.outputdir, exist_ok=True)
    outfile = os.path.join(
        args.outputdir, "output_{}.coffea"
    )
    
    # Prepare logging
    if (not setup_logging(console_log_output="stdout", console_log_level=args.loglevel, console_log_color=True,
                        logfile_file="last_run.log", logfile_log_level="info", logfile_log_color=False,
                        log_line_template="%(color_on)s[%(levelname)-8s] %(message)s%(color_off)s")):
        print("Failed to setup logging, aborting.")
        exit(1)

    print("Loading the configuration file...")
    if args.cfg[-3:] == ".py":
        # Load the script
        run_options = load_run_options(args.cfg)
        
    elif args.cfg[-4:] == ".pkl":
        # WARNING: This has to be tested!!
        config = cloudpickle.load(open(args.cfg,"rb"))
        run_options = config.run_options
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")

    if args.test:
        run_options["executor"] = args.executor if args.executor else "iterative"
        run_options["limit"] = args.limit_files if args.limit_files else 1
        run_options["max"] = args.limit_chunks if args.limit_chunks else 2
        config.filter_dataset(run_options["limit"])

    if args.limit_files!=None:
        run_options["limit"] = args.limit_files
        config.filter_dataset(run_options["limit"])

    if args.limit_chunks!=None:
        run_options["max"] = args.limit_chunks

    if args.scaleout!=None:
        run_options["scaleout"] = args.scaleout

    if args.executor !=None:
        run_options["executor"] = args.executor

    #### Fixing the environment (assuming this is run in singularity)
    # dask/parsl needs to export x509 to read over xrootd
    if run_options.get('voms', None) is not None:
        _x509_path = run_options['voms']
    else:
        _x509_localpath = get_proxy_path()
        _x509_path = os.environ['HOME'] + f'/{_x509_localpath.split("/")[-1]}'
        os.system(f'cp {_x509_localpath} {_x509_path}')
        
    if (run_env:=run_options.get("env", "singularity")) == "singularity":
        env_extra = [
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_USER_PROXY={_x509_path}',
            # f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
            f'source {sys.prefix}/bin/activate',
            'ulimit -u 32768',
            'export MALLOC_TRIM_THRESHOLD_=0'
        ]
    elif run_env == "conda":
        env_extra = [
            'export XRD_RUNFORKHANDLER=1',
            f'export X509_USER_PROXY={_x509_path}',
            # f'export X509_CERT_DIR={os.environ["X509_CERT_DIR"]}',
            'source /etc/profile.d/conda.sh',
            f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH',
            f'conda activate {os.environ["CONDA_DEFAULT_ENV"]}',
            'ulimit -u 32768',
            'export MALLOC_TRIM_THRESHOLD_=0'
        ]
        
    env_extra.append(f'export PYTHONPATH={os.path.dirname(args.cfg)}:$PYTHONPATH')
    # condor_extra = [
    #     f"cd {os.getcwd()}",
    #     f'source {os.environ["HOME"]}/.bashrc',
    #     f"source {os.getcwd()}/CondaSetup.sh",
    #     f'conda activate {os.environ["CONDA_PREFIX"]}',
    # ]
    logging.debug(env_extra)


    #########
    # Executors
    if run_options['executor'] in ['futures', 'iterative']:
        # source the environment variables
        os.environ["XRD_RUNFORKHANDLER"] = "1"
        os.environ["X509_USER_PROXY"] = _x509_path

        if run_options['executor'] == 'iterative':
            _exec = processor.iterative_executor
        else:
            _exec = processor.futures_executor
        output = processor.run_uproot_job(config.filesets,
                                    treename='Events',
                                    processor_instance=config.processor_instance,
                                    executor=_exec,
                                    executor_args={
                                        'skipbadfiles':run_options.get('skipbadfiles'),
                                        'schema': processor.NanoAODSchema,
                                        'xrootdtimeout': run_options.get('xrootdtimeout', 600),
                                        'workers': run_options['scaleout']},
                                    chunksize=run_options['chunk'],
                                    maxchunks=run_options.get('max', None)
                                    )
        
        save(output, outfile.format("all"))
        print(f"Saving output to {outfile.format('all')}")


    elif 'parsl' in run_options['executor']:
        import parsl
        from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
        from parsl.channels import LocalChannel
        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor
        from parsl.launchers import SrunLauncher, SingleNodeLauncher
        from parsl.addresses import address_by_hostname, address_by_query

        if 'slurm' in run_options['executor']:
            slurm_htex = Config(
                executors=[
                    HighThroughputExecutor(
                        label="coffea_parsl_slurm",
                        address=address_by_hostname(),
                        prefetch_capacity=0,
                        mem_per_worker=run_options['mem_per_worker'],
                        provider=SlurmProvider(
                            channel=LocalChannel(script_dir='logs_parsl'),
                            launcher=SrunLauncher(),
                            #launcher=SingleNodeLauncher(),
                            max_blocks=(run_options['scaleout'])+10,
                            init_blocks=run_options['scaleout'],
                            partition=run_options['queue'],
                            worker_init="\n".join(env_extra) + "\nexport PYTHONPATH=$PYTHONPATH:$PWD",
                            walltime=run_options['walltime'],
                            exclusive=run_options['exclusive'],
                        ),
                    )
                ],
                retries=20,
            )
            dfk = parsl.load(slurm_htex)

            output = processor.run_uproot_job(config.filesets,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.parsl_executor,
                                        executor_args={
                                            'skipbadfiles':True,
                                            'schema': processor.NanoAODSchema,
                                            'config': None,
                                        },
                                        chunksize=run_options['chunk'], maxchunks=run_options.get('max', None)
                                        )

            save(output, outfile.format("all") )
            print(f"Saving output to {outfile.format('all')}")
    
        elif 'condor' in run_options['executor']:
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
                            max_blocks=(run_options['scaleout'])+10,
                            init_blocks=run_options['scaleout'],
                            worker_init="\n".join(env_extra),
                            #transfer_input_files=xfer_files,
                            scheduler_options=condor_cfg,
                            walltime='00:30:00'
                        ),
                    )
                ],
                retries=run_options["retries"],
            )
            ## Site config for naf-desy
            if "naf-desy" in run_options['executor']:
                condor_htex = Config(
                    executors=[
                        HighThroughputExecutor(
                            label="coffea_parsl_condor",
                            address=address_by_query(),
                            max_workers=1,
                            worker_debug=True,
                            provider=CondorProvider(
                                nodes_per_block=1,
                                cores_per_slot=run_options["workers"],
                                mem_per_slot=run_options["mem_per_worker"],
                                init_blocks=run_options["scaleout"],
                                max_blocks=(run_options["scaleout"]) + 10,
                                worker_init="\n".join(env_extra + condor_extra),
                                walltime=run_options["walltime"],
                            ),
                        )
                    ],
                    retries=run_options["retries"],
                )
            dfk = parsl.load(condor_htex)

            output = processor.run_uproot_job(config.filesets,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.parsl_executor,
                                        executor_args={
                                            'skipbadfiles':True,
                                            'schema': processor.NanoAODSchema,
                                            'config': None,
                                        },
                                        chunksize=run_options['chunk'], maxchunks=run_options.get('max', None)
                                        )
            save(output, outfile.format("all"))
            print(f"Saving output to {outfile.format('all')}")


    # DASK runners
    elif 'dask' in run_options['executor']:
        import dask.config
        from pocket_coffea.parameters.dask_env import setup_dask
        from dask_jobqueue import SLURMCluster, HTCondorCluster
        from distributed import Client
        from dask.distributed import performance_report
        setup_dask(dask.config)
        
        if 'slurm' in run_options['executor']:
            log_folder = "slurm_log"
            cluster = SLURMCluster(
                queue=run_options['queue'],
                cores=run_options['workers'],
                processes=run_options['workers'],
                memory=run_options['mem_per_worker'],
                walltime=run_options["walltime"],
                env_extra=env_extra,
                local_directory=os.path.join(args.outputdir, log_folder),
            )
        elif 'condor' in run_options['executor']:
            log_folder = "condor_log"
            cluster = HTCondorCluster(
                 cores=run_options['workers'],
                 memory=run_options['mem_per_worker'],
                 disk=run_options.get('disk_per_worker', "2GB"),
                 job_script_prologue=env_extra,
            )
        elif 'lxplus' in run_options["executor"]:
            log_folder = "condor_log"
            from pocket_coffea.utils.network import check_port

            if "lxplus" not in socket.gethostname():
                raise Exception("Trying to run with dask/lxplus not at CERN! Please try different runner options")
            logging.info(">> Creating dask-lxplus cluster")
            from dask_lxplus import CernCluster
            n_port = 8786
            if not check_port(8786):
                raise RuntimeError(
                    "Port '8786' is already occupied on this node. Try another machine."
                )
            # Creating a CERN Cluster, special configuration for dask-on-lxplus
            cluster = CernCluster(
                cores=1,
                memory=run_options['mem_per_worker'],
                disk=run_options.get('disk_per_worker', "2GB"),
                image_type="singularity",
                worker_image=run_options["worker_image"],
                death_timeout="3600",
                scheduler_options={"port": n_port, "host": socket.gethostname()},
                log_directory = f"{args.outputdir}/{log_folder}",
                # shared_temp_directory="/tmp"
                job_extra={
                    "log": f"{args.outputdir}/{log_folder}/dask_job_output.log",
                    "output": f"{args.outputdir}/{log_folder}/dask_job_output.out",
                    "error": f"{args.outputdir}/{log_folder}/dask_job_output.err",
                    "should_transfer_files": "Yes",
                    "when_to_transfer_output": "ON_EXIT",
                    "+JobFlavour": f'"{run_options["queue"]}"'
                },
                env_extra=env_extra,
            )

        #Cluster adaptive number of jobs only if requested
        logging.info(">> Sending out jobs")
        cluster.adapt(minimum=1 if run_options.get("adapt", False)
                                else run_options['scaleout'],
                      maximum=run_options['scaleout'])
        client = Client(cluster)
        logging.info(">> Waiting for the first job to start...")
        client.wait_for_workers(1)
        logging.info(">> You can connect to the Dask viewer at http://localhost:8787")
        
        performance_report_path = os.path.join(args.outputdir, f"{log_folder}/dask-report.html")
        print(f"Saving performance report to {performance_report_path}")
        with performance_report(filename=performance_report_path):

            if args.full:
                # Running separately on each dataset
                fileset = config.filesets
                logging.info(f"Working on samples: {list(fileset.keys())}")
                
                output = processor.run_uproot_job(fileset,
                                        treename='Events',
                                        processor_instance=config.processor_instance,
                                        executor=processor.dask_executor,
                                        executor_args={
                                            'client': client,
                                            'skipbadfiles': run_options.get('skipbadfiles',False),
                                            'schema': processor.NanoAODSchema,
                                            'retries' : run_options['retries'],
                                            'treereduction' : run_options.get('treereduction', 20)
                                        },
                                        chunksize=run_options['chunk'],
                                        maxchunks=run_options.get('max', None)
                            )
                print(f"Saving output to {outfile.format('all')}")
                save(output, outfile.format("all") )
            else:
                # Running separately on each dataset
                for sample, files in config.filesets.items():
                    logging.info(f"Working on sample: {sample}")
                    fileset = {sample:files}
                    
                    output = processor.run_uproot_job(fileset,
                                            treename='Events',
                                            processor_instance=config.processor_instance,
                                            executor=processor.dask_executor,
                                            executor_args={
                                                'client': client,
                                                'skipbadfiles': run_options.get('skipbadfiles',False),
                                                'schema': processor.NanoAODSchema,
                                                'retries' : run_options['retries'],
                                                'treereduction' : run_options.get('treereduction', 20)
                                            },
                                            chunksize=run_options['chunk'],
                                            maxchunks=run_options.get('max', None)
                                )
                    print(f"Saving output to {outfile.format(sample)}")
                    save(output, outfile.format(sample))

    else:
        print(f"Executor {run_options['executor']} not defined!")
        exit(1)
