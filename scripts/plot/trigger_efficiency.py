import os
import sys
import time
from copy import deepcopy
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

from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.plot_utils import slice_accumulator
from pocket_coffea.utils.plot_efficiency import plot_efficiency_maps, plot_efficiency_maps_splitHT, plot_efficiency_maps_spliteras
from pocket_coffea.utils.PlotSF import plot_variation_correctionlib

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
parser.add_argument('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting')

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
else:
    try:
        outfile = config.outfile.replace(config.outfile.split('/')[-1], "output_all.coffea")
        print(f"Opening {outfile}")
        accumulator = load(outfile)
    except:
        sys.exit("Input file does not exist")

plt.style.use([hep.style.ROOT, {'font.size': 16}])
if not os.path.exists(config.plots):
    os.makedirs(config.plots)
plot_dir = os.path.join(config.plots, "trigger_efficiency")
plot_dir_sf = os.path.join(config.plots, "trigger_scalefactor")
for directory in [plot_dir, plot_dir_sf]:
    if not os.path.exists(directory):
        os.makedirs(directory)

print(accumulator.keys())
if config.plot_options['only'] != None:
    accumulator_temp = deepcopy(accumulator)
    for k, v in accumulator['variables'].items():
        if not config.plot_options['only'] in k:
            accumulator_temp['variables'].pop(k)
    accumulator = accumulator_temp

HistsToPlot = [k for k in accumulator['variables'].keys()]
NtotHists   = len(HistsToPlot)
print("# tot histograms = ", NtotHists)
print("histograms to plot:", HistsToPlot)
delimiters = np.linspace(0, NtotHists, config.plot_options['workers'] + 1).astype(int)
chunks     = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]

def _plot_efficiency_maps(entrystart, entrystop):
    _accumulator = slice_accumulator(accumulator, entrystart, entrystop)
    return plot_efficiency_maps(_accumulator, config, args.save_plots)

def _plot_efficiency_maps_splitHT(entrystart, entrystop):
    _accumulator = slice_accumulator(accumulator, entrystart, entrystop)
    return plot_efficiency_maps_splitHT(_accumulator, config, args.save_plots)

def _plot_efficiency_maps_spliteras(entrystart, entrystop):
    _accumulator = slice_accumulator(accumulator, entrystart, entrystop)
    return plot_efficiency_maps_spliteras(_accumulator, config, args.save_plots)

def save_corrections(corrections):
    if not os.path.exists(config.workflow_options["output_triggerSF"]):
        os.makedirs(config.workflow_options["output_triggerSF"])
    local_folder = os.path.join(config.output, *config.workflow_options["output_triggerSF"].split('/')[-2:])
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
            sfhist = hist.Hist(axis_variation, *axes, data=stack)
            sfhist.label = "out"
            sfhist.name = f"sf_{cat.split('_pass')[0]}"

            # The correction is built with the option flow='clamp' such that if the variable exceeds the range, the SF corresponding to the closest bin is applied
            clibcorr = correctionlib.convert.from_histogram(sfhist, flow='clamp')
            clibcorr.description = "SF matching the semileptonic trigger efficiency in MC and data."
            cset = correctionlib.schemav2.CorrectionSet(
                schema_version=2,
                description="Semileptonic trigger efficiency SF",
                corrections=[clibcorr],
            )
            rich.print(cset)
            filename = f'sf_trigger_{map_name}_{year}_{cat}.json'
            outdir = local_folder
            outfile_triggersf = os.path.join(outdir, filename)
            outfile_triggersf = overwrite_check(outfile_triggersf)
            print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
            with open(outfile_triggersf, "w") as fout:
                fout.write(cset.json(exclude_unset=True))
            fout.close()
            if 'hist_axis_y' not in correction['nominal'].keys():
                extra_args = {'histname' : histname, 'year' : year, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                plot_variation_correctionlib(outfile_triggersf, hist_axis_x, variations_labels, plot_dir_sf, **extra_args)

results = {}
for function in [_plot_efficiency_maps, _plot_efficiency_maps_spliteras, _plot_efficiency_maps_splitHT]:
    pool = Pool()
    corrections = pool.starmap(function, chunks)
    for d in corrections:
        update_recursive(results, d)
    pool.close()
save_corrections(results)

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn plots in {runTime} s")
