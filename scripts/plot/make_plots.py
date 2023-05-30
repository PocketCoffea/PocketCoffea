import os
import sys
import time
import argparse
import logging

import numpy as np

import matplotlib
matplotlib.use('Agg')

from coffea.util import load

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils import utils
from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.parameters.plotting import style_cfg

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument("-o", "--outputdir", required=True, type=str, help="Output folder")
parser.add_argument("-i", "--inputfile", required=True, type=str, help="Input file")
parser.add_argument('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting')
parser.add_argument('-e', '--exclude', type=str, default=None, help='Exclude categories with string', required=False)
parser.add_argument('-oh', '--only_hist', type=str, default='', help='Filter histograms name with string', required=False)
parser.add_argument('-oc', '--only_cat', type=str, default=None, nargs="+", help='Filter categories with string', required=False)
#parser.add_argument('-os', '--only_syst', type=str, nargs="+", default='', help='Filter systematics with a list of strings', required=False)
#parser.add_argument('--split_systematics', action='store_true', help='Split systematic uncertainties in the ratio plot')
#parser.add_argument('--partial_unc_band', action='store_true', help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`')
parser.add_argument('--overwrite', action='store_true', help='Overwrite plots in output folder')
parser.add_argument('--log', action='store_true', help='Set y-axis scale to log')
parser.add_argument('--density', action='store_true', default=False, help='Set density parameter to have a normalized plot')

args = parser.parse_args()

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( args.inputfile ): accumulator = load(args.inputfile)
else: sys.exit(f"Input file '{args.inputfile}' does not exist")

if not args.overwrite:
    if os.path.exists(args.outputdir):
        raise Exception(f"The output folder '{args.outputdir}' already exists. Please choose another output folder or run with the option `--overwrite`.")

if not os.path.exists(args.outputdir):
    os.makedirs(args.outputdir)

# Filter dictionary of histograms with `args.only`
variables_dict = { k : v for k,v in accumulator['variables'].items() if args.only_hist in k }
if args.exclude:
    variables_dict = { k : v for k,v in variables.items() if not args.exclude in k }
variables = list(variables_dict.keys())

print("Variables to plot:")
print(variables)

plotter = PlotManager(
    variables=variables,
    hist_objs=variables_dict,
    datasets_metadata=accumulator['datasets_metadata'],
    plot_dir=args.outputdir,
    style_cfg=style_cfg,
    only_cat=args.only_cat,
    workers=args.workers,
    log=args.log,
    density=args.density,
    save=True
)
plotter.plot_datamc_all(syst=True, spliteras=False)

end = time.time()
runTime = round(end-start)
NtotHists = len(list(accumulator['variables'].keys()))
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn {NtotHists} plots in {runTime} s")
