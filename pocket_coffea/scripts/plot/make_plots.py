import os
import sys
import re
import argparse

from omegaconf import OmegaConf
from coffea.util import load

from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.parameters import defaults
import click

@click.command()
@click.option('-inp', '--input-dir', help='Directory with cofea files and parameters', type=str, default=os.getcwd(), required=False)
@click.option('--cfg', help='YAML file with all the analysis parameters', required=False)
@click.option('-op', '--overwrite-parameters', type=str, multiple=True,
              default=None, help='YAML file with plotting parameters to overwrite default parameters', required=False)
@click.option("-o", "--outputdir", type=str, help="Output folder", required=False)
@click.option("-i", "--inputfile", type=str, help="Input file", required=False)
@click.option('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting', required=False)
@click.option('-oc', '--only-cat', type=str, multiple=True, help='Filter categories with string', required=False)
@click.option('-os', '--only-syst', type=str, multiple=True, help='Filter systematics with a list of strings', required=False)
@click.option('-e', '--exclude-hist', type=str, multiple=True, default=None, help='Exclude histograms with a list of regular expression strings', required=False)
@click.option('-oh', '--only-hist', type=str, multiple=True, default=None, help='Filter histograms with a list of regular expression strings', required=False)
@click.option('--split-systematics', is_flag=True, help='Split systematic uncertainties in the ratio plot', required=False)
@click.option('--partial-unc-band', is_flag=True, help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`', required=False)
@click.option('-ns','--no-syst', is_flag=True, help='Do not include systematics', required=False, default=False)
@click.option('--overwrite', '--over', is_flag=True, help='Overwrite plots in output folder', required=False)
@click.option('--log', is_flag=True, help='Set y-axis scale to log', required=False, default=False)
@click.option('--density', is_flag=True, help='Set density parameter to have a normalized plot', required=False)
@click.option('-v', '--verbose', type=int, default=1, help='Verbose level for debugging. Higher the number more stuff is printed.', required=False)

def make_plots(input_dir, cfg, overwrite_parameters, outputdir, inputfile,
               workers, only_cat, only_syst, exclude_hist, only_hist, split_systematics, partial_unc_band, no_syst, overwrite, log, density, verbose):
    '''Plot histograms produced by PocketCoffea processors'''

    # Using the `input_dir` argument, read the default config and coffea files (if not set with argparse):
    if cfg==None:
        cfg = os.path.join(input_dir, "parameters_dump.yaml")
    if inputfile==None:
        inputfile = os.path.join(input_dir, "output_all.coffea")
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
        print("Error during resolution of OmegaConf parameters magic, please check your parameters files.")
        raise(e)

    style_cfg = parameters['plotting_style']

    if os.path.isfile( inputfile ): accumulator = load(inputfile)
    else: sys.exit(f"Input file '{inputfile}' does not exist")

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
        workers=workers,
        log=log,
        density=density,
        verbose=verbose,
        save=True
    )

    print("Started plotting.  Please wait...")
    plotter.plot_datamc_all(syst=(not no_syst), spliteras=False)

    print("Output plots are saved at: ", outputdir)


if __name__ == "__main__":
    make_plots()
