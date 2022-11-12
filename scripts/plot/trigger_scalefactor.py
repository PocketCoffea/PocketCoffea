import os
import sys
import time
import json
import argparse
import itertools
from multiprocessing import Pool

import numpy as np

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredText

matplotlib.use('Agg')

import math
import mplhep as hep
from coffea.util import load
from coffea.hist import plot
import hist

from PocketCoffea.utils.configurator import Configurator
from PocketCoffea.utils.PlotSF import plot_variation_correctionlib

def overwrite_check(outfile):
    path = outfile
    fmt  = '.'+path.split('.')[-1]
    version = 1
    while os.path.exists(path):
        tag = str(version).rjust(2, '0')
        path = outfile.replace(fmt, f'_v{tag}{fmt}')
        version += 1
    #if path != outfile:
    #    print(f"The output will be saved to {path}")
    return path

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument('-v', '--version', type=str, default=None, help='Version of output (e.g. `v01`, `v02`, etc.)')
parser.add_argument('--save_plots', default=False, action='store_true', help='Save efficiency and SF plots')

args = parser.parse_args()
config = Configurator(args.cfg, plot=True, plot_version=args.version)

finalstate = config.finalstate
fontsize   = config.plot_options["fontsize"]

print("Starting ", end='')
print(time.ctime())
start = time.time()

for 

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


end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn plots in {runTime} s")
