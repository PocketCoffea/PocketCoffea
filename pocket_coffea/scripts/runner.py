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
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from coffea.util import save
from coffea import processor
from coffea.processor import Runner

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.utils import load_config, path_import, adapt_chunksize
from pocket_coffea.utils.logging import setup_logging
from pocket_coffea.utils.time import wait_until
from pocket_coffea.parameters import defaults as parameters_utils
from pocket_coffea.executors import executors_base, executors_manual_jobs
from pocket_coffea.utils.benchmarking import print_processing_stats

@click.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.option('--cfg', required=True, type=str,
              help='Config file with parameters specific to the current run')
@click.option("-ro", "--custom-run-options", type=str, default=None, help="User provided run options .yaml file")
@click.option("-o", "--outputdir", required=True, type=str, help="Output folder")
@click.option("-t", "--test", is_flag=True, help="Run with limit 1 interactively")
@click.option("-lf","--limit-files", type=int, help="Limit number of files")
@click.option("-lc","--limit-chunks", type=int, help="Limit number of chunks", default=None)
@click.option("-e","--executor", type=str, help="Overwrite executor from config (to be used only with the --test options)", default="iterative")
@click.option("-s","--scaleout", type=int, help="Overwrite scaleout config" )
@click.option("-c","--chunksize", type=int, help="Overwrite chunksize config" )
@click.option("-q","--queue", type=str, help="Overwrite queue config" )
@click.option("-ll","--loglevel", type=str, help="Console logging level", default="INFO" )
@click.option("-ps","--process-separately", is_flag=True, help="Process each dataset separately", default=False )
@click.option("--executor-custom-setup", type=str, help="Python module to be loaded as custom executor setup")
@click.option("--filter-years", type=str, help="Filter the data taking period of the datasets to be processed (comma separated list)")
@click.option("--filter-samples", type=str, help="Filter the samples to be processed (comma separated list)")
@click.option("--filter-datasets", type=str, help="Filter the datasets to be processed (comma separated list)")

def run(cfg,  custom_run_options, outputdir, test, limit_files,
           limit_chunks, executor, scaleout, chunksize,
           queue, loglevel, process_separately, executor_custom_setup,
           filter_years, filter_samples, filter_datasets):
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

    rprint("[bold]Loading the configuration file...[/]")
    if cfg[-3:] == ".py":
        # Load the script
        config = load_config(cfg, save_config=True, outputdir=outputdir)
    elif cfg[-4:] == ".pkl":
        config = cloudpickle.load(open(cfg,"rb"))
        if not config.loaded:
            config.load()
        config.save_config(outputdir)
        rprint("[italic]The configuration file is saved at {outputdir} [/]")
    else:
        raise sys.exit("Please provide a .py/.pkl configuration file")

    print(config)
    
    # Now loading the executor or from the set of predefined ones, or from the
    # user defined script
    if "@" in executor:
        # Let's extract the name of the site
        executor_name, site = executor.split("@")
    else:
        executor_name = executor
        site = None
    #print("Running with executor:", executor_name, "at", site)

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

    #Parsing additional runoptions from command line in the format --option=value, or --option. 
    ctx = click.get_current_context()
    for arg in ctx.args:
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg.split("=")
                run_options[key[2:]] = value
            else:
                next_arg = ctx.args[ctx.args.index(arg)+1] if ctx.args.index(arg)+1 < len(ctx.args) else None
                if next_arg and not next_arg.startswith("--"):
                    run_options[arg[2:]] = next_arg
                else:
                    run_options[arg[2:]] = True


    ## Default config for testing: iterative executor, with 2 file and 2 chunks
    if test:
        executor = executor if executor else "iterative"
        run_options["limit-files"] = limit_files if limit_files else 2
        run_options["limit-chunks"] = limit_chunks if limit_chunks else 2
        config.filter_dataset(run_options["limit-files"])

    # Run option display
    table = Table(title="Run Configuration")
    table.add_column("Option", style="cyan")
    table.add_column("Value", style="white")

    for key, value in sorted(run_options.items()):
        table.add_row(key, str(value))

    Console().print(table)
    
    # The user can provide a custom executor factory module
    if executor_custom_setup:
        # The user is providing a custom python module that acts as an executor factory.
        executors_lib =  path_import(executor_custom_setup)
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
    elif site == "DESY":
        from pocket_coffea.executors import executors_DESY_NAF as executors_lib
    elif site == "RWTH":
        from pocket_coffea.executors import executors_RWTH as executors_lib
    elif site == "CLAIX":
        from pocket_coffea.executors import executors_CLAIX as executors_lib
    elif site == "brux":
        from pocket_coffea.executors import executors_brux as executors_lib
    elif site == "rubin":
        from pocket_coffea.executors import executors_rubin as executors_lib
    elif site == "oscar":
        from pocket_coffea.executors import executors_oscar as executors_lib
    elif site == "casa":
        from pocket_coffea.executors import executors_casa as executors_lib
    elif site == "infn-af":
        from pocket_coffea.executors import executors_infn_af as executors_lib
    else:
        from pocket_coffea.executors import executors_base as executors_lib

    if "parsl" in executor_name or "dask" in executor_name:
        logging.getLogger().handlers[0].setLevel("ERROR")
        
    # Wait until the starting time, if provided
    if run_options["starting-time"] is not None:
        logging.info(f"Waiting until {run_options['starting-time']} to start processing")
        wait_until(run_options["starting-time"])

    # Load the executor class from the lib and instantiate it
    executor_factory = executors_lib.get_executor_factory(executor_name, run_options=run_options, outputdir=outputdir)
    # Check the type of the executor_factory
    if not (isinstance(executor_factory, executors_base.ExecutorFactoryABC) or
            isinstance(executor_factory, executors_manual_jobs.ExecutorFactoryManualABC)):
        print("The user defined executor factory lib is not of type ExecutorFactoryABC or ExecutorFactoryManualABC!", executor_name, site)
        
        exit(1)

    # Filter on the fly the fileset to process by datataking period
    filesets_to_run = config.filesets
    filter_years = filter_years.split(",") if filter_years else None
    filter_samples = filter_samples.split(",") if filter_samples else None
    filter_datasets = filter_datasets.split(",") if filter_datasets else None
    if filter_years:
        filesets_to_run = {dataset: files for dataset, files in filesets_to_run.items() if files["metadata"]["year"] in filter_years}
    if filter_samples:
        filesets_to_run = {dataset: files for dataset, files in filesets_to_run.items() if files["metadata"]["sample"] in filter_samples}
    if filter_datasets:
        filesets_to_run = {dataset: files for dataset, files in filesets_to_run.items() if dataset in filter_datasets}

    if len(filesets_to_run) == 0:
        print("No datasets to process, closing")
        exit(1)

        
    # Instantiate the executor
    
    # Checking if the executor handles the submission or returns a coffea executor
    if executor_factory.handles_submission:
        # in this case we just send to the executor the config file
        executor = executor_factory.submit(config, filesets_to_run, outputdir)
        exit(0)
    else:
        executor = executor_factory.get()


    start_time = time.time()
        
    if not process_separately:
        # Running on all datasets at once
        logging.info(f"Working on datasets: {list(filesets_to_run.keys())}")

        n_events_tot = sum([int(files["metadata"]["nevents"]) for files in filesets_to_run.values()])
        logging.info("Total number of events: %d", n_events_tot)

        adapted_chunksize = adapt_chunksize(n_events_tot, run_options)
        if adapted_chunksize != run_options["chunksize"]:
            logging.info(f"Reducing chunksize from {run_options['chunksize']} to {adapted_chunksize} for datasets")

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
        if run_options["group-samples"] is not None:
            logging.info(f"Grouping samples during processing")
            logging.info(f"Grouping samples configuration: {run_options['group-samples']}")
            # Group samples together during processing with a list specified in the run_options
            # Once a dataset is grouped, it is removed from the list of datasets to be processed to avoid double processing
            filesets_groups = {}
            filesets_to_group = filesets_to_run.copy()
            for group, samples_to_group in run_options["group-samples"].items():
                fileset_ = {}
                for dataset, files in filesets_to_run.items():
                    if files["metadata"]["sample"] in samples_to_group:
                        fileset_[dataset] = filesets_to_group.pop(dataset)
                if len(fileset_) > 0:
                    filesets_groups[group] = fileset_
            # Adding the remaining datasets that were not grouped
            for dataset, files in filesets_to_group.items():
                filesets_groups[dataset] = {dataset:files}

            print("All datasets to process:", filesets_groups.keys())
        else:
            filesets_groups = {dataset:{dataset:files} for dataset, files in filesets_to_run.items()}

        # Running separately on each dataset
        for group_name, fileset_ in filesets_groups.items():
            dataset_start_time = time.time()
            datasets = list(fileset_.keys())
            if len(datasets) == 1:
                dataset = datasets[0]
                print(f"Working on dataset: {group_name}")
                logging.info(f"Working on dataset: {group_name}")
            else:
                print(f"Working on group of datasets: {group_name} ({len(datasets)} datasets)")
                logging.info(f"Working on group of datasets: {group_name} ({len(datasets)} datasets)")

            n_events_tot = sum([int(files["metadata"]["nevents"]) for files in fileset_.values()])
            logging.info("Total number of events: %d", n_events_tot)

            adapted_chunksize = adapt_chunksize(n_events_tot, run_options)
            if adapted_chunksize != run_options["chunksize"]:
                logging.info(f"Reducing chunksize from {run_options['chunksize']} to {adapted_chunksize} for dataset(s) {group_name}")

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
            print(f"Saving output to {outfile.format(group_name)}")
            save(output, outfile.format(group_name))
            print_processing_stats(output, dataset_start_time, run_options["scaleout"])


    # If the processor has skimmed NanoAOD, we export a dataset_definition file
    if config.save_skimmed_files and config.do_postprocessing:
        from pocket_coffea.utils.skim import save_skimed_dataset_definition
        save_skimed_dataset_definition(output, f"{outputdir}/skimmed_dataset_definition.json", check_initial_events=not test)
        
    # Closing the executor if needed
    executor_factory.close()



if __name__ == "__main__":
    run()
