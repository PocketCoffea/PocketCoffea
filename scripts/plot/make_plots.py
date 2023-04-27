import os
import sys
import time
import argparse

import numpy as np

import matplotlib
matplotlib.use('Agg')

from coffea.util import load

from multiprocessing import Pool

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.parameters.plotting import style_cfg

def slice_accumulator(accumulator, entrystart, entrystop):
    '''Returns an accumulator containing only a reduced set of histograms, i.e. those between the positions `entrystart` and `entrystop`.'''
    _accumulator = dict([(key, value) for key, value in accumulator.items()])
    _accumulator['variables'] = dict(
        [(key, value) for key, value in accumulator['variables'].items()][
            entrystart:entrystop
        ]
    )
    return _accumulator

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument("-i", "--inputfile", required=True, type=str, help="Input file")
parser.add_argument('--plot_dir', default=None, help='Sub-directory inside the plots folder to save plots', required=False)
parser.add_argument('-v', '--version', type=str, default=None, help='Version of output (e.g. `v01`, `v02`, etc.)')
parser.add_argument('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting')
parser.add_argument('-o', '--only', type=str, default='', help='Filter histograms name with string', required=False)
parser.add_argument('-oc', '--only_cat', type=str, default=[''], nargs="+", help='Filter categories with string', required=False)
parser.add_argument('-os', '--only_syst', type=str, nargs="+", default='', help='Filter systematics with a list of strings', required=False)
parser.add_argument('-e', '--exclude', type=str, default=None, help='Exclude categories with string', required=False)
parser.add_argument('--split_systematics', action='store_true', help='Split systematic uncertainties in the ratio plot')
parser.add_argument('--partial_unc_band', action='store_true', help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`')
parser.add_argument('--overwrite', action='store_true', help='Overwrite plots in output folder')
parser.add_argument('--log', action='store_true', help='Set y-axis scale to log')
parser.add_argument('--density', action='store_true', help='Set density parameter to have a normalized plot')
parser.add_argument('-d', '--data_key', type=str, default='DATA', help='Prefix for data samples', required=False)

args = parser.parse_args()
config = Configurator(args.cfg, plot=True, plot_version=args.version)

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( args.inputfile ): accumulator = load(args.inputfile)
else: sys.exit(f"Input file '{args.inputfile}' does not exist")

if args.plot_dir:
    plot_dir_parent = os.path.dirname(config.plots)
    config.plots = os.path.join(plot_dir_parent, args.plot_dir)

if not args.overwrite:
    if os.path.exists(config.plots):
        raise Exception(f"The output folder '{config.plots}' already exists. Please choose another output folder or run with the option `--overwrite`.")

if not os.path.exists(config.plots):
    os.makedirs(config.plots)


def make_plots(entrystart, entrystop):
    '''Function that instantiates multiple PlotManager objects each managing a different subset of histograms.'''
    _accumulator = slice_accumulator(accumulator, entrystart, entrystop)
    plotter = PlotManager(
        hist_cfg=_accumulator['variables'],
        plot_dir=config.plots,
        style_cfg=style_cfg,
        only_cat=args.only_cat,
        data_key=args.data_key,
        log=args.log,
        density=args.density,
        save=True
    )
    plotter.plot_datamc_all(syst=True, spliteras=False)

# Filter dictionary of histograms with `args.only`
accumulator['variables'] = { k : v for k,v in accumulator['variables'].items() if args.only in k }
if args.exclude:
    accumulator['variables'] = { k : v for k,v in accumulator['variables'].items() if not args.exclude in k }
HistsToPlot = list(accumulator['variables'].keys())

NtotHists = len(HistsToPlot)
NHistsToPlot = len([key for key in HistsToPlot if args.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
print("Histograms to plot:", HistsToPlot)

# Parallelization of plotting
delimiters = np.linspace(0, NtotHists, args.workers + 1).astype(int)
chunks = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]
pool = Pool()
# Parallel calls of make_plots on different subsets of histograms
pool.starmap(make_plots, chunks)
pool.close()

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn {NHistsToPlot} plots in {runTime} s")
