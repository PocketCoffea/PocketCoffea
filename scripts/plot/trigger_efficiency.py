import os
import time
import argparse
from multiprocessing import Pool
from functools import partial

import numpy as np

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use('Agg')

from omegaconf import OmegaConf
from coffea.util import load

import mplhep as hep
import hist

import correctionlib, rich
import correctionlib.convert

from pocket_coffea.utils.plot_utils import PlotManager
from pocket_coffea.utils.plot_efficiency import plot_efficiency_maps, plot_efficiency_maps_splitHT, plot_efficiency_maps_spliteras
from pocket_coffea.utils.plot_sf import plot_variation_correctionlib
from pocket_coffea.parameters import defaults

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
parser.add_argument('--cfg', help='YAML file with all the analysis parameters', required=True)
parser.add_argument('-op', '--overwrite_parameters', type=str, nargs="+", default=None, help='YAML file with plotting parameters to overwrite default parameters', required=False)
parser.add_argument("-o", "--outputdir", required=True, type=str, help="Output folder")
parser.add_argument("-i", "--inputfile", required=True, type=str, help="Input file")
parser.add_argument('-j', '--workers', type=int, default=8, help='Number of parallel workers to use for plotting')
parser.add_argument('-oc', '--only_cat', type=str, nargs="+", help='Filter categories with string', required=False)
parser.add_argument('--overwrite', action='store_true', help='Overwrite plots in output folder')
parser.add_argument('--save_plots', default=False, action='store_true', help='Save efficiency and SF plots')

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

key_plotting = 'plotting_style'
style_cfg = parameters[key_plotting]
fontsize = style_cfg["fontsize"]

key_required = "opts_eff"
if not key_required in style_cfg:
    raise Exception(f"The dictionary '{key_plotting}' in the config is required to have a key '{key_required}' with the efficiency-related parameters.")

print("Starting ", end='')
print(time.ctime())
start = time.time()

if os.path.isfile( args.inputfile ):
    print(f"Opening {args.inputfile}")
    accumulator = load(args.inputfile)
else:
    raise Exception("Input file does not exist")

years = list(accumulator["datasets_metadata"]["by_datataking_period"].keys())
if len(years) > 1:
    raise NotImplemented("The input histograms contain data referring to multiple data-taking years. This script supports one data-taking year at a time.")
else:
    year = years[0]

plt.style.use([hep.style.ROOT, {'font.size': 16}])
if not args.overwrite:
    if os.path.exists(args.outputdir):
        raise Exception(f"The output folder '{args.outputdir}' already exists. Please choose another output folder or run with the option `--overwrite`.")
if not os.path.exists(args.outputdir):
    os.makedirs(args.outputdir)
plot_dir = os.path.join(args.outputdir, "trigger_efficiency")
plot_dir_sf = os.path.join(args.outputdir, "trigger_scalefactor")
maps_dir = os.path.join(args.outputdir, "correction_maps")
for directory in [plot_dir, plot_dir_sf, maps_dir]:
    if not os.path.exists(directory):
        os.makedirs(directory)

print(accumulator.keys())

HistsToPlot = [k for k in accumulator['variables'].keys()]
NtotHists   = len(HistsToPlot)
print("# tot histograms = ", NtotHists)
print("histograms to plot:", HistsToPlot)
delimiters = np.linspace(0, NtotHists, args.workers + 1).astype(int)
chunks     = [(delimiters[i], delimiters[i+1]) for i in range(len(delimiters[:-1]))]

def save_corrections(corrections):
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
            filename = f'sf_trigger_{map_name}_{cat}.json'
            outfile_triggersf = os.path.join(maps_dir, filename)
            outfile_triggersf = overwrite_check(outfile_triggersf)
            print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
            with open(outfile_triggersf, "w") as fout:
                fout.write(cset.json(exclude_unset=True))
            fout.close()
            if 'hist_axis_y' not in correction['nominal'].keys():
                extra_args = {'histname' : histname, 'year' : year, 'config' : style_cfg, 'cat' : cat, 'fontsize' : fontsize}
                plot_variation_correctionlib(outfile_triggersf, hist_axis_x, variations_labels, plot_dir_sf, **extra_args)

variables = accumulator['variables'].keys()
hist_objs = { v : accumulator['variables'][v] for v in variables}

plotter = PlotManager(
    variables=variables,
    hist_objs=hist_objs,
    datasets_metadata=accumulator['datasets_metadata'],
    plot_dir=args.outputdir,
    style_cfg=style_cfg,
    only_cat=args.only_cat,
    workers=args.workers,
    log=False,
    density=False,
    save=False
)

shapes_to_plot = plotter.shape_objects.values()

results = {}
for function in [plot_efficiency_maps, plot_efficiency_maps_spliteras, plot_efficiency_maps_splitHT]:
    if args.workers > 1:
        with Pool(processes=args.workers) as pool:
            # Parallel calls of plot_datamc() on different shape objects
            corrections = pool.map(partial(function, config=style_cfg, year=year, outputdir=plot_dir, save_plots=args.save_plots), shapes_to_plot)
            for d in corrections:
                update_recursive(results, d)
            pool.close()
    else:
        for shape in shapes_to_plot:
            d = function(shape, config=style_cfg, year=year, outputdir=plot_dir, save_plots=args.save_plots)
            update_recursive(results, d)
save_corrections(results)

end = time.time()
runTime = round(end-start)
print("Finishing ", end='')
print(time.ctime())
print(f"Drawn plots in {runTime} s")
