import os
import sys
import re
import glob

from omegaconf import OmegaConf
from coffea.util import load

from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.parameters import defaults
from coffea.processor import accumulate 
import click
import gc
from rich import print

import concurrent.futures

class MultiValueOption(click.Option):
    """Custom Click option that accepts multiple values in a single occurrence or multiple occurrences.
    
    Supports both:
    - --by-dataset QCD TTToSemiLeptonic
    - --by-dataset QCD --by-dataset TTToSemiLeptonic
    """
    def __init__(self, *args, **kwargs):
        self.save_other_options = kwargs.pop('save_other_options', True)
        nargs = kwargs.pop('nargs', -1)
        assert nargs == -1, 'nargs, if set, must be -1 not {}'.format(nargs)
        super(MultiValueOption, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):
        def parser_process(value, state):
            # Method to hook into the parser.process
            done = False
            value = [value]
            if self.save_other_options:
                # Grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value.append(state.rargs.pop(0))
            else:
                # Grab everything remaining
                value += state.rargs
                state.rargs[:] = []
            value = tuple(value)

            # Call the actual process
            self._previous_parser_process(value, state)

        retval = super(MultiValueOption, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval

@click.command()
@click.option('-inp', '--input-dir', help='Directory with coffea files and parameters', type=str, default=os.getcwd(), required=False)
@click.option('--cfg', help='YAML file with all the analysis parameters', required=False)
@click.option('-op', '--overwrite-parameters', type=str, multiple=True,
              default=None, help='YAML file with plotting parameters to overwrite default parameters', required=False)
@click.option("-o", "--outputdir", type=str, help="Output folder", required=False)
@click.option("-i", "--inputfiles", type=str, multiple=True, help="Input file(s) or patterns", required=False)
@click.option('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting', required=False)
@click.option('-oc', '--only-cat', type=str, multiple=True, help='Filter categories with string', required=False)
@click.option('-oy', '--only-year', type=str, multiple=True, help='Filter datataking years with string', required=False)
@click.option('-os', '--only-syst', type=str, multiple=True, help='Filter systematics with a list of strings', required=False)
@click.option('-e', '--exclude-hist', type=str, multiple=True, default=None, help='Exclude histograms with a list of regular expression strings', required=False)
@click.option('-oh', '--only-hist', type=str, multiple=True, default=None, help='Filter histograms with a list of regular expression strings', required=False)
@click.option('--split-systematics', is_flag=True, help='Split systematic uncertainties in the ratio plot', required=False)
@click.option('--partial-unc-band', is_flag=True, help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`', required=False)
@click.option('-ns','--no-syst', is_flag=True, help='Do not include systematics', required=False, default=False)
@click.option('--overwrite', '--over', is_flag=True, help='Overwrite plots in output folder', required=False)
@click.option('--log-x', is_flag=True, help='Set x-axis scale to log', required=False, default=False)
@click.option('--log-y', is_flag=True, help='Set y-axis scale to log', required=False, default=False)
@click.option('--density', is_flag=True, help='Set density parameter to have a normalized plot', required=False, default=False)
@click.option('-v', '--verbose', type=int, default=1, help='Verbose level for debugging. Higher the number more stuff is printed.', required=False)
@click.option('--format', type=str, default='png', help='File format of the output plots', required=False)
@click.option('--systematics-shifts', is_flag=True, help='Plot the shifts for the systematic uncertainties', required=False, default=False)
@click.option('--no-ratio', is_flag=True, help='Dont plot the ratio', required=False, default=False)
@click.option('--no-systematics-ratio', is_flag=True, help='Plot the ratio of the shifts for the systematic uncertainties', required=False, default=False)
@click.option('--compare', is_flag=True, help='Plot comparison of the samples, instead of data/MC', required=False, default=False)
@click.option('--index-file', type=str, help='Path of the index file to be copied recursively in the plots directory and its subdirectories', required=False, default=None)
@click.option('--no-cache', is_flag=True, help='Do not cache the histograms for faster plotting', required=False, default=False)
@click.option('--split-by-category', is_flag=True, help='If split-by-category was used during running and merging', required=False, default=False)
@click.option('--by-dataset', cls=MultiValueOption, help='Split specified samples by dataset instead of collapsing them. Accepts multiple samples either as: --by-dataset SAMPLE1 SAMPLE2 or --by-dataset SAMPLE1 --by-dataset SAMPLE2', required=False, default=None)

def make_plots(*args, **kwargs):
    return make_plots_core(*args, **kwargs)

def make_plots_core(input_dir, cfg, overwrite_parameters, outputdir, inputfiles,
               workers, only_cat, only_year, only_syst, exclude_hist, only_hist, split_systematics, partial_unc_band, no_syst,
               overwrite, log_x, log_y, density, verbose, format, systematics_shifts, no_ratio, no_systematics_ratio, compare, index_file, no_cache, split_by_category, by_dataset):
    '''Plot histograms produced by PocketCoffea processors'''

    # Normalize by_dataset to a list (handles both tuple from single-flag and list from multiple flags)
    if by_dataset is not None:
        if isinstance(by_dataset, tuple):
            # Flatten tuples from multiple occurrences of the option
            flattened = []
            for item in by_dataset:
                if isinstance(item, tuple):
                    flattened.extend(item)
                else:
                    flattened.append(item)
            by_dataset = flattened
        elif not isinstance(by_dataset, list):
            by_dataset = [by_dataset]
    
    if split_by_category: 
        if not inputfiles:
            all_files = glob.glob(f"{input_dir}/*merged_category*.coffea")
        else:
            all_files = []
            for pattern in inputfiles:
                matched_files = glob.glob(pattern)  # Expand wildcards
                valid_files = [file for file in matched_files if os.path.isfile(file)]
                all_files.extend(valid_files)        

        print("[b]Since we are splitting by category, will handle only one file per pass.[/]")
        for ifl, file in enumerate(all_files):
            make_plots_core(input_dir, cfg, overwrite_parameters, outputdir, [file],
               workers, only_cat, only_year, only_syst, exclude_hist, only_hist, split_systematics, partial_unc_band, no_syst,
               overwrite or ifl > 0, log_x, log_y, density, verbose, format, systematics_shifts, no_ratio, no_systematics_ratio, compare, index_file, no_cache, False, by_dataset)
            gc.collect()

        print("[green]Done making plots for all category-split files![/]")
        exit()

    # Using the `input_dir` argument, read the default config and coffea files (if not set with argparse):
    if cfg==None:
        cfg = os.path.join(input_dir, "parameters_dump.yaml")
    if not inputfiles:
        inputfiles = (os.path.join(input_dir, "output_merged.coffea"),)
    if outputdir==None:
        outputdir = os.path.join(input_dir, "plots")

    

    # Load yaml file with OmegaConf
    if cfg[-5:] == ".yaml":
        parameters_dump = OmegaConf.load(cfg)
    else:
        raise Exception("The input file format is not valid. The config file should be a in .yaml format.")

    # Overwrite plotting parameters
    if not overwrite_parameters:
        parameters = parameters_dump
    else:
        parameters = defaults.merge_parameters_from_files(parameters_dump, *overwrite_parameters, update=True)

    # Resolving the OmegaConf
    try:
        OmegaConf.resolve(parameters)
    except Exception as e:
        print("[red]Error during resolution of OmegaConf parameters magic, please check your parameters files.[/]")
        raise(e)

    style_cfg = parameters['plotting_style']

    # Expand wildcards and filter out invalid files
    all_files = []
    for pattern in inputfiles:
        matched_files = glob.glob(pattern)  # Expand wildcards
        valid_files = [file for file in matched_files if os.path.isfile(file)]
        all_files.extend(valid_files)
    if not all_files: sys.exit("No valid input files found.")

    def load_single_file(file):
        """Helper function to load a single file."""
        print(f"[pink]Loading: {file}[/]")
        return load(file)
    # Use ThreadPoolExecutor to load files concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        files = list(executor.map(load_single_file, all_files))

    if files: accumulator = accumulate(files)

    if not overwrite:
        if os.path.exists(outputdir):
            raise Exception(f"The output folder '{outputdir}' already exists. Please choose another output folder or run with the option `--overwrite`.")

    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    variables = accumulator['variables'].keys()

    if exclude_hist:
        variables_to_exclude = [s for s in variables if any([re.search(p, s) for p in exclude_hist])]
        variables = [s for s in variables if s not in variables_to_exclude]
    if only_hist:
        variables = [s for s in variables if any([re.search(p, s) for p in only_hist])]
    hist_objs = { v : accumulator['variables'][v] for v in variables }

    plotter = PlotManager(
        variables=variables,
        hist_objs=hist_objs,
        datasets_metadata=accumulator['datasets_metadata'],
        plot_dir=outputdir,
        style_cfg=style_cfg,
        only_cat=only_cat,
        only_year=only_year,
        workers=workers,
        log_x=log_x,
        log_y=log_y,
        density=density,
        verbose=verbose,
        save=True,
        index_file=index_file,
        cache=not no_cache,
        split_by_dataset_samples=by_dataset
    )

    print("Started plotting.  Please wait...")

    if compare:
        plotter.plot_comparison_all(ratio=(not no_ratio), format=format)
    else:
        if systematics_shifts:
            plotter.plot_systematic_shifts_all(
                format=format, ratio=(not no_systematics_ratio)
            )
        else:
            plotter.plot_datamc_all(syst=(not no_syst), ratio = (not no_ratio), spliteras=False, format=format)

    print(f"[green]Output plots are saved at: {outputdir}[/]")


if __name__ == "__main__":
    make_plots()
