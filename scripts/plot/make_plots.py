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
parser.add_argument('-v', '--version', type=str, default=None, help='Version of output (e.g. `v01`, `v02`, etc.)')
parser.add_argument('--test', default=False, action='store_true', help='Test mode')

args = parser.parse_args()
config = Configurator(args.cfg, plot=True, plot_version=args.version)

#finalstate = config.finalstate
#categories_to_sum_over = config.plot_options["sum_over"]
#var = config.plot_options["var"]

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( config.outfile ): accumulator = load(config.outfile)
else: sys.exit("Input file does not exist")

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
    'alpha': 1.0
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
        plot_data_mc_hist1D(h, histname, config)

HistsToPlot   = [k for k in accumulator['variables'].keys()]
NtotHists = len(HistsToPlot)
NHistsToPlot = len([key for key in HistsToPlot if config.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
print("Histograms to plot:", HistsToPlot)
delimiters = np.linspace(0, NtotHists, config.run_options['workers'] + 1).astype(int)
chunks = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]
pool = Pool()
pool.starmap(make_plots, chunks)
pool.close()

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn {NHistsToPlot} plots in {runTime} s")
