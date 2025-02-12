from coffea.util import load, save
from coffea.processor import accumulate 
import click
import os
from rich import print
from rich.console import Console
from rich.progress import Progress
import cloudpickle
import yaml
from pocket_coffea.utils.filter_output import compare_dict_types
from pocket_coffea.utils.skim import save_skimed_dataset_definition
from itertools import islice
from functools import reduce


def merge_outputs(inputfiles, outputfile, jobs_config=None, force=False):
    '''Merge coffea output files'''
    if os.path.exists(outputfile) and not force:
        print(f"[red]Output file {outputfile} already exists. Use -f to overwrite it.")
        exit(1)
                
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

        out = accumulate([load(f) for f in inputfiles])
        save(out, outputfile)
        print(f"[green]Output saved to {outputfile}")

    else:
        # read the job configuration file
        print(f"Reading job configuration file {jobs_config}")
        with open(jobs_config, 'r') as f:
            job_config = yaml.safe_load(f)

        jobs_list = job_config['jobs_list']
        alldone = True
        output_files = []
        # First check that the jobs are done
        for job_name, job in jobs_list.items():
            # Check output
            if not os.path.exists(job['output_file']):
                print(f"[red]Job {job_name} output is missing[/]")
                alldone = False
            output_files.append(job['output_file'])
        if not alldone:
            print(f"[red]Not all jobs are done yet[/]")
            exit(1)
        print(f"[green]All jobs are done[/]")
        print(output_files)

        # Loading the configurator
        cloudpickle_config = job_config['config_pkl_total']
        with open(cloudpickle_config, 'rb') as f:
            configurator = cloudpickle.load(f)

        # Since it was jobs, there was no postprocessing
        # we do it now after merging all the output
        print(f"Merging output...")

        with Progress() as progress:

            task1 = progress.add_task("[red]Merging...", total=len(output_files))

            def reduce_in_groups(func, iterable, group_size):
                result = None
                it = iter(iterable)
                while batch := list(islice(it, group_size)):
                    loaded_batch = [load(f) for f in batch]
                    if result is None:
                        result = func(loaded_batch)
                    else:
                        result = func([result, func(loaded_batch)])
                    progress.update(task1, advance=len(batch))
                return result

            # Example usage:
            total_output = reduce_in_groups(accumulate, output_files, 5)
        
        # Apply postprocessing
        print(f"Applying postprocessing...")
        total_output = configurator.processor_instance.postprocess(total_output)

        # In case of skimming jobs save the dataset definition (like in runner)
        if configurator.save_skimmed_files:
            print(f"[blue]Saving skimmed dataset definition[/]")
            save_skimed_dataset_definition(total_output, f"{job_config['output_dir']}/skimmed_dataset_definition.json")

        # Save the output
        print(f"[green]Saving output to {outputfile}...[/]")
        save(total_output, outputfile)
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
    required=True,
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
# overwrite option
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Overwrite output file if it exists",
)

def main(inputfiles, outputfile, jobs_config, force):
    '''Merge coffea output files'''
    merge_outputs(inputfiles, outputfile, jobs_config, force)

if __name__ == "__main__":
    main()
