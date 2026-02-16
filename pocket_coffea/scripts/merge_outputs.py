from coffea.util import load, save
from coffea.processor import accumulate 
import click
import os, sys
from rich import print
from rich.console import Console
from rich.progress import Progress
import cloudpickle
import yaml
from pocket_coffea.utils.filter_output import compare_dict_types
from pocket_coffea.utils.skim import save_skimed_dataset_definition
from itertools import islice
from functools import reduce
import pickle
from glob import glob
import psutil, gc
mem_threshold = 0.5 # ~50% + memory needed to dump files, is the empirical threshold on lxplus


def merge_group_reduction(output_files, N_reduction=5, cachedir="merge_cache", max_mem_gb=8, verbose=False):
    with Progress() as progress:
        task1 = progress.add_task("[red]Merging...", total=len(output_files))
    
        def reduce_in_groups(iterable, group_size):
            result = None
            # Always work with the iterator directly, don't recreate it
            while batch := list(islice(iterable, group_size)):
                if verbose:
                    filesize = sum([os.path.getsize(f) for f in batch])/1024**3
                    print(f"File size (on disk) to load: {filesize:.3f} GB")
                loaded_batch = [load(f) for f in batch]
                batch_acc = accumulate(loaded_batch)
                del loaded_batch
                if result is None:
                    result = batch_acc
                else:
                    result = accumulate([result, batch_acc])                                   
                mem_usage = psutil.Process(os.getpid()).memory_info().rss / 1024**3
                if verbose: 
                    print(f"Current memory usage: {mem_usage:.3f} GB ({mem_usage/max_mem_gb*100:.1f}%)")
                del batch_acc
                progress.update(task1, advance=len(batch))
                if mem_usage > max_mem_gb * mem_threshold:
                    # return the result so-far, and remaining iterator
                    return result, iterable
                
            return result, None

        # Convert to iterator once at the beginning
        itr = iter(list(output_files))
        counter = 0
        while itr:
            result, itr = reduce_in_groups(itr, N_reduction)
            if counter==0 and itr is None:
                # Got full merge in one pass, no need to cache intermediate results
                return result
            else:
                # We are here because memory usage of "result" exceeded max_memory. Cache it on disk and start again.
                os.makedirs(cachedir, exist_ok=True)
                interm_output = f"{cachedir}/merge_{counter}.coffea"
                print(f"[green]Dumping intermediate result to {interm_output} since memory utilization of merged object exceeded {mem_threshold*100:.0f}% of {max_mem_gb:.1f} GB.[/]")
                save(result, interm_output)
                if verbose: 
                    print(f"Intermediate output size on disk: {os.path.getsize(interm_output)/1024**3:.3f} GB")
                print()
                del result
                gc.collect()
                counter += 1

    new_output_files = glob(f"{cachedir}/*.coffea")
    print(f"[green][b]Since outputs were too large to fit in memory, I created {len(new_output_files)} fragmented output files.[/] These may be moved to and merged on a high-memory machine.[/]")
    exit()

def merge_outputs(inputfiles, outputfile, jobs_config=None, force=False, N_reduction=5, max_mem_gb=None, cache_dir=None, verbose=False):
    '''Merge coffea output files'''
    if jobs_config is not None:
        # read the job configuration file
        print(f"Reading job configuration file {jobs_config}")
        with open(jobs_config, 'r') as f:
            job_config = yaml.safe_load(f)
        if "split_by_category" in job_config:   # Ensure back compatibility
            split_by_category = job_config["split_by_category"]
            print("Jobs were split by category, hence will merge per category.")
        else:
            split_by_category = False

    if outputfile is None:
        if jobs_config is None:
            print("[red]Need to specify outputfile (-o) if no job configuration (-jc) is specified![/]")
            exit(1)
        else:
            outputfile = os.path.join(job_config['output_dir'], "output_merged.coffea")
            print(f"Setting {outputfile} as output file.")

    if os.path.exists(outputfile) and not force:
        print(f"[red]Output file {outputfile} already exists. Use -f to overwrite it.")
        exit(1)
    
    if max_mem_gb is None:
        max_mem_gb = psutil.virtual_memory().available / 1024 ** 3
        print(f"Setting max memory usage to {max_mem_gb:.1f} GB. Output will be split into smaller chunks if memory usage exceeds {mem_threshold*100:.0f}%.")
        print("[b]If you still see OOM kills, set a lower max memory with -m.[/]")
                
    # quite different behaviour in case of just files merging, or in case of merging jobs
    if jobs_config is None:
        if len(inputfiles) == 0:
            print("[red]No input files provided[ nor job configuration file/]")
            exit(1)
   
        print(f"[blue]Merging files into {outputfile}[/]")
        print(sorted(inputfiles))
        type_mismatches = []
        f0 = inputfiles[0]
        for f in inputfiles[1:]:
            type_mismatch_found = compare_dict_types(load(f0), load(f))
            type_mismatches.append(type_mismatch_found)
        if any(type_mismatches):
            print("[red]Type mismatch found between the values of the input dictionaries for the following files:")
            for i, f in enumerate(inputfiles):
                if type_mismatches[i]:
                    print(f"    {f}")
            raise TypeError("Type mismatch found between the values of the input dictionaries. Please check the input files.")
        
        if cache_dir is None:
            cache_dir = '/'.join(outputfile.split("/")[:-1])+"/merge_cache"
        total_out =  merge_group_reduction(inputfiles, N_reduction=N_reduction, cachedir=cache_dir, 
                                           max_mem_gb=max_mem_gb, verbose=verbose)
        save(total_out, outputfile)
        print(f"[green]Output saved to {outputfile}")

    else:
        jobs_list = job_config['jobs_list']
        alldone = True
        output_files = []
        output_files_by_category = {}
        # First check that the jobs are done
        with Progress() as progress:
            task_ = progress.add_task("[red]Checking output files from jobs...", total=len(list(jobs_list.keys())))
            for job_name, job in jobs_list.items():
                # Check output
                if split_by_category:
                    # Listing all files with glob is slow   
                    this_job_outputs = glob(job['output_file'].replace("job_","*job_"))
                    if len(this_job_outputs) == 0:
                        print(f"[red]Job {job_name} output is missing[/]")
                        alldone = False
                    else:
                        output_files.extend(this_job_outputs)
                        for this_job_output in this_job_outputs:
                            this_category = "category" + this_job_output.split("category")[1].split("_")[0]
                            if this_category not in output_files_by_category:
                                output_files_by_category[this_category] = []
                            output_files_by_category[this_category].append(this_job_output)
                else:
                    if not os.path.exists(job['output_file']):
                        print(f"[red]Job {job_name} output is missing[/]")
                        alldone = False
                    output_files.append(job['output_file'])
                progress.update(task_, advance=1)

        if not alldone:
            print(f"[red]Not all jobs are done yet[/]")
            exit(1)
        print(f"[green]All jobs are done[/]")

        noutput = len(output_files)
        if noutput < 100:
            print(output_files)
        else:
            print(f"Found {noutput} output files.")

        cloudpickle_config = job_config['config_pkl_total']
        with open(cloudpickle_config, 'rb') as f:
            configurator = cloudpickle.load(f)

        if split_by_category:
            output_file_bunches = list(output_files_by_category.values())
            suffs = list(output_files_by_category.keys())
        else:
            output_file_bunches = [output_files]
            suffs = [None]

        for this_output_files,suff in zip(output_file_bunches,suffs):
            # Output file will be renamed with suffix if this is category-split
            thisoutputfile = outputfile
            if suff:
                thisoutputfile = thisoutputfile.replace(".coffea",f"_{suff}.coffea")
            
            # Check if output exists, again because these might be category-split outputs
            if os.path.exists(thisoutputfile) and not force:
                print(f"[red]Output file {thisoutputfile} already exists. Use -f to overwrite it. Skipping.[/]")
                continue

            if suff:
                print(f"[green]Doing set: {suff}[/]")

            # Do larger files first, so that OOM kills happen early on rather than later
            this_output_files = sorted(this_output_files, key=os.path.getsize, reverse=True)

            # Since it was jobs, there was no postprocessing
            # we do it now after merging all the output
            print(f"Merging output...")
            if cache_dir is None:
                cache_dir = os.path.join(job_config['output_dir'],"merge_cache")
            if suff:
                cache_dir += f"_{suff}"
            total_output = merge_group_reduction(this_output_files, N_reduction=N_reduction, cachedir=cache_dir, max_mem_gb=max_mem_gb, verbose=verbose)
                
            # Apply postprocessing
            print(f"Applying postprocessing...")
            total_output = configurator.processor_instance.postprocess(total_output)

            # In case of skimming jobs save the dataset definition (like in runner)
            if configurator.save_skimmed_files:
                print(f"[blue]Saving skimmed dataset definition[/]")
                save_skimed_dataset_definition(total_output, f"{job_config['output_dir']}/skimmed_dataset_definition.json")

            # Save the output            
            print(f"[green]Saving output to {thisoutputfile}...[/]")
            save(total_output, thisoutputfile)

            del total_output

        print(f"[green]Done![/]")

@click.command()
@click.argument(
    'inputfiles',
    required=False,
    type=str,
    nargs=-1,
)
@click.option(
    "-o",
    "--outputfile",
    required=False,
    type=str,
    help="Output file",
)
@click.option(
    "-jc",
    "--jobs-config",
    required=False,
    type=str,
    help="Job configuration file",
)
@click.option(
    "-n",
    "--reduction",
    required=False,
    type=int,
    default=5,
    help="Number of output files to accumulate at a time",
)
@click.option(
    "-m",
    "--max_mem_gb",
    required=False,
    type=float,
    help="Max memory (in GB) allotted to the accumulated result, after which it is dumped to disk. Use about one-fourth of available RAM, e.g. 8 for a 32GB RAM machine.",
)
@click.option(
    "-c",
    "--cache_dir",
    required=False,
    type=str,
    default=None,
    help="Cache dir for intermediate dumps, if any",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Display memory consumed by objects",
)

# overwrite option
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Overwrite output file if it exists",
)

def main(inputfiles, outputfile, jobs_config, force, reduction, max_mem_gb, cache_dir, verbose):
    '''Merge coffea output files'''
    merge_outputs(inputfiles, outputfile, jobs_config, force, reduction, max_mem_gb, cache_dir, verbose)

if __name__ == "__main__":
    main()
