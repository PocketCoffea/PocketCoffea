import os
import sys
import time
import json
import argparse

import numpy as np

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredText
matplotlib.use('Agg')


import matplotlib.pyplot as plt
import math
import mplhep as hep
from coffea.util import load
from coffea.hist import plot
import coffea.hist as hist

from multiprocessing import Pool
from pocket_coffea.parameters.lumi import lumi, femtobarn

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.plot_utils import slice_accumulator, plot_data_mc_hist1D

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument('--plot_dir', default=None, help='Sub-directory inside the plots folder to save plots', required=False)
parser.add_argument('-v', '--version', type=str, default=None, help='Version of output (e.g. `v01`, `v02`, etc.)')
parser.add_argument('--test', default=False, action='store_true', help='Test mode')
parser.add_argument('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting')
parser.add_argument('-o', '--only', type=str, default='', help='Filter histograms name with string', required=False)
parser.add_argument('-oc', '--only_cat', type=str, default='', help='Filter categories with string', required=False)
parser.add_argument('-os', '--only_syst', type=str, nargs="+", default='', help='Filter systematics with a list of strings', required=False)
parser.add_argument('--split_systematics', action='store_true', help='Split systematic uncertainties in the ratio plot')
parser.add_argument('--partial_unc_band', action='store_true', help='Plot only the partial uncertainty band corresponding to the systematics specified as the argument `only_syst`')
parser.add_argument('--overwrite', action='store_true', help='Overwrite plots in output folder')

args = parser.parse_args()
config = Configurator(args.cfg, plot=True, plot_version=args.version)

#finalstate = config.finalstate
#categories_to_sum_over = config.plot_options["sum_over"]
#var = config.plot_options["var"]

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( config.outfile ): accumulator = load(config.outfile)
else: sys.exit(f"Input file '{config.outfile}' does not exist")

if args.plot_dir:
    plot_dir_parent = os.path.dirname(config.plots)
    config.plots = os.path.join(plot_dir_parent, args.plot_dir)

if not args.overwrite:
    if os.path.exists(config.plots):
        raise Exception(f"The output folder '{config.plots}' already exists. Please choose another output folder or run with the option `--overwrite`.")

data_err_opts = {
    'linestyle': 'none',
    'marker': '.',
    'markersize': 10.,
    'color': 'k',
    'elinewidth': 1,
}

mc_opts = {
    #'facecolor': 'None',
    'edgecolor': 'black',
    #'linestyle': '-',
    'linewidth': 1,
}

signal_opts = {
    'facecolor': 'None',
    'edgecolor': ['green', 'red'],
    'linestyle': ['--', '-'],
    'linewidth': 2,
    'alpha': 0.7
}

ggH_opts = {
    'bb' : {
        'facecolor': 'None',
        'edgecolor': 'green',
        'linestyle': '--',
        'linewidth': 2,
        'alpha': 0.7
    },
    'cc': {
        'facecolor': 'None',
        'edgecolor': 'red',
        'linestyle': '--',
        'linewidth': 2,
        'alpha': 0.7
    }
}

selection = {
    'trigger'  : (r'Trigger'),
    'dilepton_SR' : (r'Trigger'+'\n'+
                     r'Dilepton cuts'+'\n'+
                     r'SR'),
    'dilepton_CR' : (r'Trigger'+'\n'+
                     r'Dilepton cuts'+'\n'+
                     r'CR'),
    'semileptonic_SR' : (r'Trigger'+'\n'+
                     r'Semileptonic cuts'+'\n'+
                     r'SR'),
    'semileptonic_CR' : (r'Trigger'+'\n'+
                     r'Semileptonic cuts'+'\n'+
                     r'CR'),
    'semileptonic_triggerSF_Ele32_EleHT_fail' : 'Trigger fail',
    'semileptonic_triggerSF_Ele32_EleHT_pass' : 'Trigger pass',
    'semileptonic_triggerSF_inclusive' : 'Inclusive',
}

plt.style.use([hep.style.ROOT, {'font.size': 16}])
if not os.path.exists(config.plots):
    os.makedirs(config.plots)

def make_plots(entrystart, entrystop):
    _accumulator = slice_accumulator(accumulator, entrystart, entrystop)
    for (histname, h) in _accumulator['variables'].items():
        plot_data_mc_hist1D(
            h,
            histname,
            config,
            plot_dir=args.plot_dir,
            flavorsplit=None,
            only_cat=args.only_cat,
            mcstat=True,
            stat_only=False,
            split_systematics=args.split_systematics,
            only_syst=args.only_syst,
            partial_unc_band=args.partial_unc_band)

# Filter dictionary of histograms with `args.only`
accumulator['variables'] = { k : v for k,v in accumulator['variables'].items() if args.only in k }
HistsToPlot = [k for k in accumulator['variables'].keys()]

NtotHists = len(HistsToPlot)
NHistsToPlot = len([key for key in HistsToPlot if args.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
print("Histograms to plot:", HistsToPlot)

# Parallelization of plotting
delimiters = np.linspace(0, NtotHists, args.workers + 1).astype(int)
chunks = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]
pool = Pool()
pool.starmap(make_plots, chunks)
pool.close()

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn {NHistsToPlot} plots in {runTime} s")
