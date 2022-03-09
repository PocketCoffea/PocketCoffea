import os
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
from parameters.allhistograms import histogram_settings
from parameters.lumi import lumi

from utils.config_loader import load_config

parser = argparse.ArgumentParser(description='Plot histograms from coffea file')
parser.add_argument('-i', '--input', type=str, help='Input histogram filename', required=True)
parser.add_argument('-o', '--output', type=str, default='', help='Output directory', required=True)
parser.add_argument('--outputDir', type=str, default=None, help='Output directory')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument('-s', '--scale', type=str, default='linear', help='Plot y-axis scale', required=False)
parser.add_argument('-d', '--dense', action='store_true', default=False, help='Normalized plots')
parser.add_argument('--hist2d', action='store_true', default=False, help='Plot only 2D histograms')
parser.add_argument('--proxy', action='store_true', help='Plot proxy and signal comparison')
parser.add_argument('--only', action='store', default='', help='Plot only one histogram')
parser.add_argument('--test', action='store_true', default=False, help='Test with lower stats.')
parser.add_argument('--data', type=str, default='BTagMu', help='Data sample name')
parser.add_argument('-n', '--normed', action='store_true', default=False, help='Normalized overlayed plots')
parser.add_argument('-j', '--workers', type=int, default=12, help='Number of workers (cores/threads) to use for multi-worker execution (default: %(default)s)')

args = parser.parse_args()

print("Starting ", end='')
print(time.ctime())
start = time.time()
print("Running with options:")
print("    ", args)

if os.path.isfile( args.input ): accumulator = load(args.input)
else:
    files_list = [ifile for ifile in os.listdir(args.input) if ifile != args.output]
    accumulator = load(args.input + files_list[0])
    histograms = accumulator.keys()
    for ifile in files_list[1:]:
        output = load(args.input + ifile)
        for histname in histograms:
            accumulator[histname].add(output[histname])

cfg = load_config(args.cfg).cfg

#scaleXS = {}
#for isam in accumulator[next(iter(accumulator))].identifiers('dataset'):
#   isam = str(isam)
#   scaleXS[isam] = 1 if isam.startswith('BTag') else xsecs[isam]/accumulator['sumw'][isam]

data_err_opts = {
    'linestyle': 'none',
    'marker': '.',
    'markersize': 10.,
    'color': 'k',
    'elinewidth': 1,
}

qcd_opts = {
    'facecolor': 'None',
    'edgecolor': 'blue',
    'linestyle': '-',
    'linewidth': 2,
    'alpha': 0.7
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
    'trigger' : (r'Trigger'),
    'dilepton' : (r'Trigger'+'\n'+
                  r'Dilepton cuts')
}

plt.style.use([hep.style.ROOT, {'font.size': 16}])
plot_dir = args.outputDir if args.outputDir else os.getcwd()+"/plots/" + args.output + "/"
if not os.path.exists(plot_dir):
    os.makedirs(plot_dir)

def make_plots(entrystart, entrystop):
    _accumulator = dict(list(accumulator.items())[entrystart:entrystop])
    for histname in _accumulator:
        if args.only and not (args.only in histname): continue
        if not histname.lstrip('hist_').startswith(tuple(cfg['variables'])): continue
        h = _accumulator[histname]

        for year in [str(s) for s in h.identifiers('year')]:
            # Convert lumi in fb^-1 and round to the first decimal digit
            totalLumi = 'TEST' if args.test else round(lumi[year]/1000, 1)            
            for cut in [str(s) for s in h.identifiers('cut')]:
                selection_text = selection[cut]
                datasets = [str(s) for s in h.identifiers('dataset')]
                varname = h.fields[-1]
                varlabel = h.axis(varname).label
                # This if has to be rewritten!
                #if histname.lstrip('hist_').startswith( tuple(histogram_settings['variables'].keys()) ):
                #    h = h.rebin(varname, hist.Bin(varname, varlabel, **histogram_settings['variables']['_'.join(histname.split('_')[:2])]['binning']))
                #h.scale( scaleXS, axis='dataset' )

                if (not 'hist2d' in histname) & (not args.hist2d):

                    fig, ax = plt.subplots(1, 1, figsize=(12,9))
                    fig.subplots_adjust(hspace=.07)
                    if len(h.axes()) > 4:
                        categories_to_sum_over = ['cut', 'year']
                        collections = [str(s) for s in h.identifiers('collection')]
                        plot.plot1d(h.sum('dataset')[(cut, year)].sum(*categories_to_sum_over), ax=ax, legend_opts={'loc':1})
                        maxY = 1.2 *max( [ max(h.sum('dataset')[(cut, year)].sum(*categories_to_sum_over).values()[(collection,)]) for collection in collections] )
                    else:
                        categories_to_sum_over = ['cut', 'year']
                        plot.plot1d(h[(datasets, cut, year)].sum(*categories_to_sum_over), ax=ax, legend_opts={'loc':1})
                        maxY = 1.2 *max( [ max(h[(dataset, cut, year)].sum(*categories_to_sum_over).values()[(dataset,)]) for dataset in datasets] )

                    hep.cms.text("Preliminary", ax=ax)
                    hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax)
                    ax.legend()
                    at = AnchoredText(selection_text, loc=2, frameon=False)
                    ax.add_artist(at)
                    ax.set_ylim(0,maxY)

                    if histname.lstrip('hist_').startswith( tuple(histogram_settings['variables'].keys()) ):
                        if histname == 'hist_bquark_drMatchedJet':
                            ax.set_xlim(0,1)
                        elif histname == 'hist_bquark_pt':
                            ax.set_xlim(0,200)
                        #ax.set_xlim(**histogram_settings['variables']['_'.join(histname.lstrip('hist_').split('_')[:2])]['xlim'])
                    filepath = f"{plot_dir}{histname}_{cut}_{year}.png"
                    if args.scale != parser.get_default('scale'):
                        if (not args.dense) & (args.scale == "log"):
                            ax.semilogy()
                            exp = 2 + math.floor(math.log(maxY, 10))
                            ax.set_ylim(0.1, 10**exp)
                        #rax.set_ylim(0.1,10)
                        filepath = filepath.replace(".png", "_" + args.scale + ".png")
                    print("Saving", filepath)
                    plt.savefig(filepath, dpi=300, format="png")
                    plt.close(fig)

    return

HistsToPlot = [k for k in accumulator.keys() if 'hist' in k]
NtotHists = len(HistsToPlot)
NHistsToPlot = len([key for key in HistsToPlot if args.only in key])
print("# tot histograms = ", NtotHists)
print("# histograms to plot = ", NHistsToPlot)
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
