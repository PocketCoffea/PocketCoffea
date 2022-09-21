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

import correctionlib, rich
import correctionlib.convert

from PocketCoffea.utils.Configurator import Configurator
from PocketCoffea.utils.Plot import plot_efficiency_maps, plot_efficiency_maps_splitHT, plot_efficiency_maps_spliteras
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

def update_recursive(dict1, dict2):
    ''' Update two config dictionaries recursively.

    Args:
        dict1 (dict): first dictionary to be updated
        dict2 (dict): second dictionary which entries should be used

    '''
    for k, v in dict2.items():
        if k not in dict1:
            dict1[k] = dict()
        if isinstance(v, dict):
            update_recursive(dict1[k], v)
        else:
            dict1[k] = v

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

if os.path.isfile( config.outfile ):
    print(f"Opening {config.outfile}")
    accumulator = load(config.outfile)
else: sys.exit("Input file does not exist")

plt.style.use([hep.style.ROOT, {'font.size': 16}])
if not os.path.exists(config.plots):
    os.makedirs(config.plots)
plot_dir = os.path.join(config.plots, "trigger_efficiency")
plot_dir_sf = os.path.join(config.plots, "trigger_scalefactor")
for directory in [plot_dir, plot_dir_sf]:
    if not os.path.exists(directory):
        os.makedirs(directory)

print(accumulator.keys())

def _plot_efficiency_maps(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    return plot_efficiency_maps(_accumulator, config, args.save_plots)

def _plot_efficiency_maps_splitHT(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    return plot_efficiency_maps_splitHT(_accumulator, config, args.save_plots)

def _plot_efficiency_maps_spliteras(entrystart, entrystop):
    _accumulator = dict( [(key, value) for key, value in accumulator.items() if key.startswith('hist')][entrystart:entrystop] )
    return plot_efficiency_maps_spliteras(_accumulator, config, args.save_plots)

def save_corrections(corrections):
    if not os.path.exists(config.output_triggerSF):
        os.makedirs(config.output_triggerSF)
    local_folder = os.path.join(config.output, *config.output_triggerSF.split('/')[-2:])
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    for histname, d in corrections.items():
        for cat, correction in d.items():
            year = correction['nominal']['year']
            hist_axis_x = correction['nominal']['hist_axis_x']
            axes = [hist_axis_x]
            if 'hist_axis_y' in correction['nominal'].keys():
                map_name = histname.split("hist2d_")[-1]
                hist_axis_y = correction['nominal']['hist_axis_y']
                axes.append(hist_axis_y)
            else:
                map_name = histname.split("hist_")[-1]
            variations_labels = list(correction.keys())
            ratio_stack = [correction[var]['ratio_stack'] for var in variations_labels]
            print(variations_labels)
            stack = np.stack(ratio_stack)
            axis_variation = hist.axis.StrCategory(variations_labels, name="variation")
            #print("stack", stack.shape)
            sfhist = hist.Hist(axis_variation, *axes, data=stack)
            sfhist.label = "out"
            sfhist.name = f"sf_{cat.split('_pass')[0]}"
            clibcorr = correctionlib.convert.from_histogram(sfhist)
            clibcorr.description = "SF matching the semileptonic trigger efficiency in MC and data."
            cset = correctionlib.schemav2.CorrectionSet(
                schema_version=2,
                description="Semileptonic trigger efficiency SF",
                corrections=[clibcorr],
            )
            rich.print(cset)
            filename = f'sf_trigger_{map_name}_{year}_{cat}.json'
            for outdir in [config.output_triggerSF, local_folder]:
                outfile_triggersf = os.path.join(outdir, filename)
                outfile_triggersf = overwrite_check(outfile_triggersf)
                print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
                with open(outfile_triggersf, "w") as fout:
                    fout.write(cset.json(exclude_unset=True))
                fout.close()
            if 'hist_axis_y' not in correction['nominal'].keys():
                extra_args = {'histname' : histname, 'year' : year, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                plot_variation_correctionlib(outfile_triggersf, hist_axis_x, variations_labels, plot_dir_sf, **extra_args)

HistsToPlot   = [k for k in accumulator.keys() if k.startswith('hist')]
NtotHists   = len(HistsToPlot)
NHistsToPlot   = len([key for key in HistsToPlot if config.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
print("histograms to plot:", HistsToPlot)
delimiters = np.linspace(0, NtotHists, config.plot_options['workers'] + 1).astype(int)
chunks     = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]
#print("chunks:", chunks)

results = {}
for function in [_plot_efficiency_maps, _plot_efficiency_maps_spliteras, _plot_efficiency_maps_splitHT]:
    pool = Pool()
    corrections = pool.starmap(function, chunks)
    for d in corrections:
        update_recursive(results, d)
    #print("***********************\nresults:")
    #print(results['hist2d_electron_etaSC_vs_electron_pt'])
    pool.close()
#print(results['hist2d_electron_etaSC_vs_electron_pt']['Ele32_EleHT_pass']['nominal'])

save_corrections(results)

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn plots in {runTime} s")
