#!/usr/bin/env python
import os
import sys
import argparse

from omegaconf import OmegaConf
from coffea.util import load

from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.parameters import defaults

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('--cfg', help='YAML file with all the analysis parameters', required=True)
parser.add_argument('-op', '--overwrite_parameters', type=str, nargs="+", default=None, help='YAML file with plotting parameters to overwrite default parameters', required=False)
parser.add_argument("-o", "--outputdir", required=True, type=str, help="Output folder")
parser.add_argument("-i", "--inputfile", required=True, type=str, help="Input file")
parser.add_argument('-j', '--workers', type=int, default=1, help='Number of parallel workers to use for plotting', required=False)
parser.add_argument('-oc', '--only_cat', type=str, nargs="+", help='Filter categories with string', required=False)
parser.add_argument('-os', '--only_syst', type=str, nargs="+", help='Filter systematics with a list of strings', required=False)
parser.add_argument('-e', '--exclude_hist', type=str, nargs="+", default=None, help='Exclude histograms with a list of strings', required=False)
parser.add_argument('--split_systematics', action='store_true', help='Split systematic uncertainties in the ratio plot', required=False)
parser.add_argument('--partial_unc_band', action='store_true', help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`', required=False)
parser.add_argument('--overwrite', action='store_true', help='Overwrite plots in output folder', required=False)
parser.add_argument('--log', action='store_true', help='Set y-axis scale to log', required=False)
parser.add_argument('--density', action='store_true', help='Set density parameter to have a normalized plot', required=False)

args = parser.parse_args()

print("Loading the configuration file...")

# Load yaml file with OmegaConf
if args.cfg[-5:] == ".yaml":
    parameters_dump = OmegaConf.load(args.cfg)
else:
    raise Exception("The input file format is not valid. The config file should be a in .yaml format.")

# Overwrite plotting parameters
if args.overwrite_parameters == parser.get_default("overwrite_parameters"):
    parameters = parameters_dump
else:
    parameters = defaults.merge_parameters_from_files(parameters_dump, *args.overwrite_parameters, update=True)

# Resolving the OmegaConf
try:
    OmegaConf.resolve(parameters)
except Exception as e:
    print("Error during resolution of OmegaConf parameters magic, please check your parameters files.")
    raise(e)

style_cfg = parameters['plotting_style']

if os.path.isfile( args.inputfile ): accumulator = load(args.inputfile)
else: sys.exit(f"Input file '{args.inputfile}' does not exist")

if not args.overwrite:
    if os.path.exists(args.outputdir):
        raise Exception(f"The output folder '{args.outputdir}' already exists. Please choose another output folder or run with the option `--overwrite`.")

if not os.path.exists(args.outputdir):
    os.makedirs(args.outputdir)

variables = accumulator['variables'].keys()
if args.exclude_hist != None:
    variables = list(filter(lambda x : all([s not in x for s in args.exclude_hist]), variables))
hist_objs = { v : accumulator['variables'][v] for v in variables }

plotter = PlotManager(
    variables=variables,
    hist_objs=hist_objs,
    datasets_metadata=accumulator['datasets_metadata'],
    plot_dir=args.outputdir,
    style_cfg=style_cfg,
    only_cat=args.only_cat,
    workers=args.workers,
    log=False,
    density=args.density,
    save=True
)
plotter.plot_datamc_all(syst=True, spliteras=False)

# if Log is also requested, rerun
if args.log:
    plotter = PlotManager(
        variables=variables,
        hist_objs=hist_objs,
        datasets_metadata=accumulator['datasets_metadata'],
        plot_dir=args.outputdir,
        style_cfg=style_cfg,
        only_cat=args.only_cat,
        workers=args.workers,
        log=True,
        density=args.density,
        save=True
    )
    plotter.plot_datamc_all(syst=True, spliteras=False)
