import os
import sys
import time
import json
import argparse

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredText

import math
import mplhep as hep
from coffea.util import load
from coffea.hist import plot
import coffea.hist as hist

from multiprocessing import Pool
from PocketCoffea.parameters.allhistograms import histogram_settings
from PocketCoffea.parameters.lumi import lumi, femtobarn

from PocketCoffea.utils.Configurator import Configurator

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
_accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist2d_')] )

for (histname, h) in _accumulator.items():
    if config.only and not (config.only in histname): continue
    if not histname.startswith('hist2d_'): continue
    print(histname)
    variable2d = histname.lstrip('hist2d_') 
    varname_x = variable2d.split('_vs_')[1]
    varname_y = variable2d.split('_vs_')[0]
    #obj_x, field_x = varname_x.split('_')
    #obj_y, field_y = varname_y.split('_')

    datasets = [str(s) for s in h.identifiers('sample')]
    datasets_data = list(filter(lambda x : 'DATA' in x, datasets))
    datasets_mc   = list(filter(lambda x : 'TTTo' in x, datasets))    

    axis_x = h.axes()[-2]
    axis_y = h.axes()[-1]
    binwidth_x = np.ediff1d(axis_x.edges())
    binwidth_y = np.ediff1d(axis_y.edges())
    bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
    bincenter_y = axis_y.edges()[:-1] + 0.5*binwidth_y

    years = [str(s) for s in h.identifiers('year')]
    for year in years:
        h_data = h[(datasets_data, year, )].sum('sample', 'year')
        h_mc   = h[(datasets_mc, year, )].sum('sample', 'year')
        categories = [str(s) for s in h.identifiers('cat')]
        for cat in categories:
            h_data = h[(datasets_data, categories, year )].sum('sample', 'year')
            h_mc   = h[(datasets_mc, categories, year )].sum('sample', 'year')
            eff_mc   = h_mc[cat].sum('cat').values()[()] / h_mc['inclusive'].sum('cat').values()[()]
            eff_data = h_data[cat].sum('cat').values()[()] / h_data['inclusive'].sum('cat').values()[()]
            sf = eff_data/eff_mc
            eff_data = np.nan_to_num(eff_data)
            eff_mc   = np.nan_to_num(eff_mc)
            sf       = np.nan_to_num(np.where(~np.isinf(sf), sf, np.nan))

            for (label, eff) in zip(["eff_mc", "eff_data", "sf"], [eff_mc, eff_data, sf]):
                y, x = np.meshgrid(bincenter_y, bincenter_x)
                assert ((x.shape == eff.shape) & (y.shape == eff.shape)), "The arrays corresponding to the x- and y-axes need to have the same shape as the efficiency map."
                fig, ax = plt.subplots(figsize=[16,10])
                hist, xbins, ybins, im = plt.hist2d(x.flatten(), y.flatten(), weights=eff.flatten(), bins=(axis_x.edges(), axis_y.edges()), cmax=1.2)
                ax.set_xlabel(axis_x.label, fontsize=fontsize)
                ax.set_ylabel(axis_y.label, fontsize=fontsize)
                if varname_x == 'electron_pt':
                    ax.set_xscale('log')
                ax.set_xlim(*config.variables2d[variable2d][varname_x]['xlim'])
                ax.set_ylim(*config.variables2d[variable2d][varname_y]['ylim'])
                #xticks = [20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500]
                #yticks = axis_y.edges()
                if 'xticks' in config.variables2d[variable2d][varname_x].keys():
                    xticks = config.variables2d[variable2d][varname_x]['xticks']
                else:
                    xticks = axis_x.edges()
                if 'yticks' in config.variables2d[variable2d][varname_y].keys():
                    yticks = config.variables2d[variable2d][varname_y]['yticks']
                else:
                    yticks = axis_y.edges()
                ax.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                ax.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                plt.vlines(axis_x.edges(), axis_y.edges()[-1], axis_y.edges()[0], linestyle='--', color='gray')
                plt.hlines(axis_y.edges(), axis_x.edges()[-1], axis_x.edges()[0], linestyle='--', color='gray')

                for (i, x) in enumerate(bincenter_x):
                    for (j, y) in enumerate(bincenter_y):
                        ax.text(x, y, round(eff[i][j], 3), 
                                color="w", ha="center", va="center", fontsize=fontsize, fontweight="bold")

                #plt.title("Electron trigger SF")
                cbar = plt.colorbar()
                if "eff" in label:
                    cbar.set_label("Trigger efficiency", fontsize=fontsize)
                else:
                    cbar.set_label("Trigger SF", fontsize=fontsize)

                filepath = os.path.join(plot_dir, f"{histname}_{label}_{cat}_{year}.png")

                print("Saving", filepath)
                plt.savefig(filepath, dpi=300, format="png")
                plt.close(fig)

