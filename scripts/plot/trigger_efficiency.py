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

import math
import mplhep as hep
from coffea.util import load
from coffea.hist import plot
import coffea.hist as hist

from multiprocessing import Pool
from PocketCoffea.parameters.allhistograms import histogram_settings

from PocketCoffea.utils.Configurator import Configurator
from PocketCoffea.utils.Plot import plot_efficiency, plot_efficiency_eras, plot_efficiency_ht

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument('-v', '--version', type=str, default=None, help='Version of output (e.g. `v01`, `v02`, etc.)')

args = parser.parse_args()
config = Configurator(args.cfg, plot=True, plot_version=args.version)

finalstate = config.finalstate
fontsize   = config.plot_options["fontsize"]

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( config.outfile ):
    print(f"Opening {config.outfile}")
    accumulator = load(config.outfile)
else: sys.exit("Input file does not exist")

plt.style.use([hep.style.ROOT, {'font.size': 16}])
if not os.path.exists(config.plots):
    os.makedirs(config.plots)
plot_dir = os.path.join(config.plots, "trigger_efficiency")
if not os.path.exists(plot_dir):
    os.makedirs(plot_dir)

print(accumulator.keys())

def _plot_efficiency(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    plot_efficiency(_accumulator, config)

def _plot_efficiency_eras(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    plot_efficiency_eras(_accumulator, config)

def _plot_efficiency_ht(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    plot_efficiency_ht(_accumulator, config)

HistsToPlot   = [k for k in accumulator.keys() if k.startswith('hist')]
NtotHists   = len(HistsToPlot)
NHistsToPlot   = len([key for key in HistsToPlot if config.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
print("histograms to plot:", HistsToPlot)
delimiters = np.linspace(0, NtotHists, config.plot_options['workers'] + 1).astype(int)
chunks     = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]
#print("chunks:", chunks)

if config.split_eras:
    function = _plot_efficiency_eras
elif config.split_ht:
    function = _plot_efficiency_ht
else:
    function = _plot_efficiency

pool = Pool()
pool.starmap(function, chunks)
pool.close()

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn plots in {runTime} s")
