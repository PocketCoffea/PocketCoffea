#!/usr/bin/env python
print("""
    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/

""")
import os, getpass
import sys
import argparse
import cloudpickle
import socket
import logging
import yaml
from yaml import Loader, Dumper

from coffea.util import save
from coffea import processor
from coffea.processor import Runner

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils import utils
from pocket_coffea.utils.logging import setup_logging
from pocket_coffea.parameters import defaults as parameters_utils
from pocket_coffea.executors import executors_base

def load_config(cfg):
    ''' Helper function to load a Configurator instance from a user defined python module'''
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
    
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run analysis on NanoAOD files using PocketCoffea processors')
    # Inputs
    parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", required=True, type=str,
                        help='Config file with parameters specific to the current run')
    parser.add_argument("-ro", "--run-options", type=str, default=None, help="User provided run options .yaml file")
    parser.add_argument("-o", "--outputdir", required=True, type=str, help="Output folder")
    parser.add_argument("-t", "--test", action="store_true", help="Run with limit 1 interactively")
    parser.add_argument("-lf","--limit-files", type=int, help="Limit number of files")
    parser.add_argument("-lc","--limit-chunks", type=int, help="Limit number of chunks", default=None)
    parser.add_argument("-e","--executor", type=str,
                        help="Overwrite executor from config (to be used only with the --test options)", default="iterative")
    parser.add_argument("-s","--scaleout", type=int, help="Overwrite scalout config" )
    parser.add_argument("-c","--chunksize", type=int, help="Overwrite chunksize config" )
    parser.add_argument("-q","--queue", type=str, help="Overwrite queue config" )
    parser.add_argument("-ll","--loglevel", type=str, help="Console logging level", default="INFO" )
    parser.add_argument("-f","--full", action="store_true", help="Process all datasets at the same time", default=False )
    parser.add_argument("--executor-custom-setup", type=str, help="Python module to be loaded as custom executor setup")

    args = parser.parse_args()


    # Setting up the output dir
    os.makedirs(args.outputdir, exist_ok=True)
    outfile = os.path.join(
        args.outputdir, "output_{}.coffea"
    )
    logfile = os.path.join(args.outputdir, "logfile.log")
    # Prepare logging
    if (not setup_logging(console_log_output="stdout", console_log_level=args.loglevel, console_log_color=True,
                        logfile_file=logfile, logfile_log_level="info", logfile_log_color=False,
                        log_line_template="%(color_on)s[%(levelname)-8s] %(message)s%(color_off)s")):
        print("Failed to setup logging, aborting.")
        exit(1)

    print("Loading the configuration file...")
    if args.cfg[-3:] == ".py":
        # Load the script
        config = load_config(args.cfg)

    elif args.cfg[-4:] == ".pkl":
        # WARNING: This has to be tested!!
        config = cloudpickle.load(open(args.cfg,"rb"))
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")


    # Now loading the executor or from the set of predefined ones, or from the
    # user defined script
    if "@" in args.executor:
        # Let's extract the name of the site
        executor_name, site = args.executor.split("@")
    else:
        executor_name = args.executor
        site = None

    # Getting the default run_options
    run_options_defaults = parameters_utils.get_default_run_options()
    run_options = run_options_defaults["general"]
    # If there are default specific default run_options load them
    if site in run_options_defaults:
        run_options.update(run_options_defaults[site])
    if executor_name in run_options_defaults:
        run_options.update(run_options_defaults[executor_name])
    if f"{executor_name}@{site}" in run_options_defaults:
        run_options.update(run_options_defaults[f"{executor_name}@{site}"])

    # Now merge on top the user defined run_options
    if args.run_options:
        run_options = parameters_utils.merge_parameters_from_files(run_options, args.run_options)

    if args.limit_files!=None:
        run_options["limit-files"] = args.limit_files
        config.filter_dataset(run_options["limit-files"])

    if args.limit_chunks!=None:
        run_options["limit-chunks"] = args.limit_chunks

    if args.scaleout!=None:
        run_options["scaleout"] = args.scaleout

    if args.chunksize!=None:
        run_options["chunksize"] = args.chunksize

    if args.queue!=None:
        run_options["queue"] = args.queue

        
    ## Default config for testing: iterative executor, with 2 file and 2 chunks
    if args.test:
        executor = args.executor if args.executor else "iterative"
        run_options["limit-files"] = args.limit_files if args.limit_files else 2
        run_options["limit-chunks"] = args.limit_chunks if args.limit_chunks else 2
        config.filter_dataset(run_options["limit-files"])
   

    # The user can provide a custom executor factory module
    if args.executor_custom_setup:
        # The user is providing a custom python module that acts as an executor factory.
        executors_lib =  utils.path_import(args.executor_custom_setup)
        if "get_executor_factory" not in executors_lib.__dict__.keys():
            print(f"The user defined executor setup module {args.executor_custom_setup}"
                  "does not define a `get_executor_factory` function!")
            exit(1)

    # if site is known we can load the corresponding module
    elif site == "lxplus":
        from pocket_coffea.executors import executors_lxplus as executors_lib
    elif site == "T3_PSI_CH":
        from pocket_coffea.executors import executors_T3_PSI_CH as executors_lib
    elif site == "purdue":
        from pocket_coffea.executors import executors_purdue as executors_lib
    else:
        from pocket_coffea.executors import executors_base as executors_lib

    # Load the executor class from the lib and instantiate it
    executor_factory = executors_lib.get_executor_factory(executor_name, run_options=run_options, outputdir=args.outputdir)
    # Check the type of the executor_factory
    if not isinstance(executor_factory, executors_base.ExecutorFactoryABC):
        print("The user defined executor factory lib is not of type BaseExecturoABC!")
        exit(1)
    
    # Instantiate the executor
    executor = executor_factory.get()
        
    if args.full:
        # Running on all datasets at once
        fileset = config.filesets
        logging.info(f"Working on samples: {list(fileset.keys())}")

        run = Runner(
            executor=executor,
            chunksize=run_options["chunksize"],
            maxchunks=run_options["limit-chunks"],
            skipbadfiles=run_options['skip-bad-files'],
            schema=processor.NanoAODSchema,
            format="root",
        )
        output = run(fileset, treename="Events",
                     processor_instance=config.processor_instance)

        print(f"Saving output to {outfile.format('all')}")
        save(output, outfile.format("all") )
        
    else:
        # Running separately on each dataset
        for sample, files in config.filesets.items():
            logging.info(f"Working on sample: {sample}")
            fileset = {sample:files}

            n_events_tot = int(files["metadata"]["nevents"])
            n_workers_max = n_events_tot / run_options["chunksize"]
            
            # If the number of available workers exceeds the maximum number of workers for a given sample,
            # the chunksize is reduced so that all the workers are used to process the given sample
            if (run_options["scaleout"] > n_workers_max):
                adapted_chunksize = int(n_events_tot / run_options["scaleout"])
                logging.info(f"Reducing chunksize from {run_options['chunk']} to {chunksize} for sample {sample}")
            else:
                adapted_chunksize = run_options["chunksize"]

            run = Runner(
                executor=executor,
                chunksize=adapted_chunksize,
                maxchunks=run_options["limit-chunks"],
                skipbadfiles=run_options['skip-bad-files'],
                schema=processor.NanoAODSchema,
                format="root",
            )
            output = run(fileset, treename="Events",
                         processor_instance=config.processor_instance)
            print(f"Saving output to {outfile.format(sample)}")
            save(output, outfile.format(sample))
         
    # Closing the executor if needed
    executor_factory.close()
