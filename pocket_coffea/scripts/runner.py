import os, getpass
import sys
import argparse
import cloudpickle
import socket
import logging
import yaml
from yaml import Loader, Dumper
import click
import time

from coffea.util import save
from coffea import processor
from coffea.processor import Runner

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.utils import load_config
from pocket_coffea.utils.logging import setup_logging
from pocket_coffea.parameters import defaults as parameters_utils
from pocket_coffea.executors import executors_base
from pocket_coffea.utils.benchmarking import print_processing_stats

@click.command()
@click.option('--cfg', required=True, type=str,
              help='Config file with parameters specific to the current run')
@click.option("-ro", "--custom-run-options", type=str, default=None, help="User provided run options .yaml file")
@click.option("-o", "--outputdir", required=True, type=str, help="Output folder")
@click.option("-t", "--test", is_flag=True, help="Run with limit 1 interactively")
@click.option("-lf","--limit-files", type=int, help="Limit number of files")
@click.option("-lc","--limit-chunks", type=int, help="Limit number of chunks", default=None)
@click.option("-e","--executor", type=str, help="Overwrite executor from config (to be used only with the --test options)", default="iterative")
@click.option("-s","--scaleout", type=int, help="Overwrite scalout config" )
@click.option("-c","--chunksize", type=int, help="Overwrite chunksize config" )
@click.option("-q","--queue", type=str, help="Overwrite queue config" )
@click.option("-ll","--loglevel", type=str, help="Console logging level", default="INFO" )
@click.option("-ps","--process-separately", is_flag=True, help="Process each dataset separately", default=False )
@click.option("--executor-custom-setup", type=str, help="Python module to be loaded as custom executor setup")
@click.option("--filter-years", type=str, help="Filter the data taking period of the datasets to be processed (comma separated list)")

def run(cfg,  custom_run_options, outputdir, test, limit_files,
           limit_chunks, executor, scaleout, chunksize,
           queue, loglevel, process_separately, executor_custom_setup,
           filter_years):
    '''Run an analysis on NanoAOD files using PocketCoffea processors'''

    # Setting up the output dir
    os.makedirs(outputdir, exist_ok=True)
    outfile = os.path.join(
        outputdir, "output_{}.coffea"
    )
    logfile = os.path.join(outputdir, "logfile.log")
    # Prepare logging
    if (not setup_logging(console_log_output="stdout", console_log_level=loglevel, console_log_color=True,
                        logfile_file=logfile, logfile_log_level="info", logfile_log_color=False,
                        log_line_template="%(color_on)s[%(levelname)-8s] %(message)s%(color_off)s")):
        print("Failed to setup logging, aborting.")
        exit(1)

    print("Loading the configuration file...")
    if cfg[-3:] == ".py":
        # Load the script
        config = load_config(cfg, save_config=True, outputdir=outputdir)
        logging.info(config)
    
    elif cfg[-4:] == ".pkl":
        # WARNING: This has to be tested!!
        config = cloudpickle.load(open(cfg,"rb"))
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")
    
    # Now loading the executor or from the set of predefined ones, or from the
    # user defined script
    if "@" in executor:
        # Let's extract the name of the site
        executor_name, site = executor.split("@")
    else:
        executor_name = executor
        site = None
    print("Running with executor", executor_name, site)

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
    if custom_run_options:
        run_options = parameters_utils.merge_parameters_from_files(run_options, custom_run_options)
    
    if limit_files!=None:
        run_options["limit-files"] = limit_files
        config.filter_dataset(run_options["limit-files"])

    if limit_chunks!=None:
        run_options["limit-chunks"] = limit_chunks

    if scaleout!=None:
        run_options["scaleout"] = scaleout

    if chunksize!=None:
        run_options["chunksize"] = chunksize

    if queue!=None:
        run_options["queue"] = queue


    ## Default config for testing: iterative executor, with 2 file and 2 chunks
    if test:
        executor = executor if executor else "iterative"
        run_options["limit-files"] = limit_files if limit_files else 2
        run_options["limit-chunks"] = limit_chunks if limit_chunks else 2
        config.filter_dataset(run_options["limit-files"])

    # The user can provide a custom executor factory module
    if executor_custom_setup:
        # The user is providing a custom python module that acts as an executor factory.
        executors_lib =  utils.path_import(executor_custom_setup)
        if "get_executor_factory" not in executors_lib.__dict__.keys():
            print(f"The user defined executor setup module {executor_custom_setup}"
                  "does not define a `get_executor_factory` function!")
            exit(1)

    # if site is known we can load the corresponding module
    elif site == "lxplus":
        from pocket_coffea.executors import executors_lxplus as executors_lib
    elif site == "swan":
        from pocket_coffea.executors import executors_cern_swan as executors_lib
    elif site == "T3_CH_PSI":
        from pocket_coffea.executors import executors_T3_CH_PSI as executors_lib
    elif site == "purdue-af":
        from pocket_coffea.executors import executors_purdue_af as executors_lib
    elif site == "DESY_NAF":
        from pocket_coffea.executors import executors_DESY_NAF as executors_lib
    elif site == "RWTH":
        from pocket_coffea.executors import executors_RWTH as executors_lib
    elif site == "casa":
        from pocket_coffea.executors import executors_casa as executors_lib
    else:
        from pocket_coffea.executors import executors_base as executors_lib

    if "parsl" in executor_name:
        logging.getLogger().handlers[0].setLevel("ERROR")
        
    # Load the executor class from the lib and instantiate it
    executor_factory = executors_lib.get_executor_factory(executor_name, run_options=run_options, outputdir=outputdir)
    # Check the type of the executor_factory
    if not isinstance(executor_factory, executors_base.ExecutorFactoryABC):
        print("The user defined executor factory lib is not of type BaseExecturoABC!", executor_name, site)
        
        exit(1)

    # Instantiate the executor
    executor = executor_factory.get()
    start_time = time.time()

    # Filter on the fly the fileset to process by datataking period
    filesets_to_run = {}
    filter_years = filter_years.split(",") if filter_years else None
    if filter_years:
        for fileset_name, fileset in config.filesets.items():
            if fileset["metadata"]["year"] in filter_years:
                filesets_to_run[fileset_name] = fileset
    else:
        filesets_to_run = config.filesets

    if len(filesets_to_run) == 0:
        print("No datasets to process, closing")
        exit(1)
        
    if not process_separately:
        # Running on all datasets at once
        logging.info(f"Working on samples: {list(filesets_to_run.keys())}")

        run = Runner(
            executor=executor,
            chunksize=run_options["chunksize"],
            maxchunks=run_options["limit-chunks"],
            skipbadfiles=run_options['skip-bad-files'],
            schema=processor.NanoAODSchema,
            format="root",
        )
        output = run(filesets_to_run, treename="Events",
                     processor_instance=config.processor_instance)
        
        print(f"Saving output to {outfile.format('all')}")
        save(output, outfile.format("all") )
        print_processing_stats(output, start_time, run_options["scaleout"])

    else:
        # Running separately on each dataset
        for sample, files in config.filesets_to_run.items():
            print(f"Working on sample: {sample}")
            logging.info(f"Working on sample: {sample}")
            
            fileset_ = {sample:files}

            n_events_tot = int(files["metadata"]["nevents"])
            n_workers_max = n_events_tot / run_options["chunksize"]

            # If the number of available workers exceeds the maximum number of workers for a given sample,
            # the chunksize is reduced so that all the workers are used to process the given sample
            if (run_options["scaleout"] > n_workers_max):
                adapted_chunksize = int(n_events_tot / run_options["scaleout"])
                logging.info(f"Reducing chunksize from {run_options['chunksize']} to {adapted_chunksize} for sample {sample}")
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
            output = run(fileset_, treename="Events",
                         processor_instance=config.processor_instance)
            print(f"Saving output to {outfile.format(sample)}")
            save(output, outfile.format(sample))
            print_processing_stats(output, start_time, run_options["scaleout"])
    # Closing the executor if needed
    executor_factory.close()



if __name__ == "__main__":
    run()
