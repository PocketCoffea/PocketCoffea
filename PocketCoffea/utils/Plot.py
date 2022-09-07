import os
import sys

import numpy as np
import matplotlib.pyplot as plt

import mplhep as hep
import hist
from coffea import processor
from coffea.hist import Bin, Hist, plot
from coffea.lookup_tools.dense_lookup import dense_lookup
from coffea.util import save, load

import correctionlib, rich
import correctionlib.convert

from PocketCoffea.parameters.lumi import lumi, femtobarn

#color_datamc = {'data' : 'black', 'mc' : 'red'}
opts_data = {
    'linestyle' : 'solid',
    'linewidth' : 0,
    'marker' : '.',
    'markersize' : 1.,
    'color' : 'black',
    'elinewidth' : 1,
    'label' : 'Data'
}

opts_data_eras = {
    'tot' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : 'tot'
    },
    'A' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'green',
        'elinewidth' : 1,
        'label' : 'A'
    },
    'B' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'blue',
        'elinewidth' : 1,
        'label' : 'B'
    },
    'C' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'orange',
        'elinewidth' : 1,
        'label' : 'C'
    },
    'D' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'purple',
        'elinewidth' : 1,
        'label' : 'D'
    },
    'E' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'yellow',
        'elinewidth' : 1,
        'label' : 'E'
    },
    'F' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'cyan',
        'elinewidth' : 1,
        'label' : 'F'
    },
}

opts_data_splitHT = {
    'tot' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'black',
        'elinewidth' : 1,
        'label' : 'tot'
    },
    'lowHT' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'green',
        'elinewidth' : 1,
        'label' : r'$H_T$ < 400 GeV'
    },
    'highHT' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'blue',
        'elinewidth' : 1,
        'label' : r'$H_T$ > 400 GeV'
    },
}

opts_mc = {
    'nominal' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'
    },
    'Up' : {
        'linestyle' : 'dashed',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'
    },
    'Down' : {
        'linestyle' : 'dotted',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'
    },
}

opts_mc_splitHT = {
    'tot' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'black',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'
    },
    'highHT' : {
        'linestyle' : 'dashed',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'green',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'+'\n'+r'$H_T$ < 400 GeV'
    },
    'lowHT' : {
        'linestyle' : 'dotted',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'blue',
        'elinewidth' : 1,
        'label' : r'$t\bar{t}$ DL + SL MC'+'\n'+r'$H_T$ > 400 GeV'
    },
}

opts_sf = {
    'nominal' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : 'SF'
    },
    'Up' : {
        'linestyle' : 'dashed',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : 'SF'
    },
    'Down' : {
        'linestyle' : 'dotted',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : 'SF'
    },
}

opts_sf_eras = {
    'tot' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'red',
        'elinewidth' : 1,
        'label' : 'tot'
    },
    'A' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'green',
        'elinewidth' : 1,
        'label' : 'A'
    },
    'B' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'blue',
        'elinewidth' : 1,
        'label' : 'B'
    },
    'C' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'orange',
        'elinewidth' : 1,
        'label' : 'C'
    },
    'D' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'purple',
        'elinewidth' : 1,
        'label' : 'D'
    },
    'E' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'yellow',
        'elinewidth' : 1,
        'label' : 'E'
    },
    'F' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'cyan',
        'elinewidth' : 1,
        'label' : 'F'
    },
}

opts_sf_splitHT = {
    'tot' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'black',
        'elinewidth' : 1,
        'label' : 'tot'
    },
    'lowHT' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'green',
        'elinewidth' : 1,
        'label' : r'$H_T$ < 400 GeV'
    },
    'highHT' : {
        'linestyle' : 'solid',
        'linewidth' : 0,
        'marker' : '.',
        'markersize' : 1.,
        'color' : 'blue',
        'elinewidth' : 1,
        'label' : r'$H_T$ > 400 GeV'
    },
}

opts_errorbar = {
    'nominal' : {
        'linestyle' : 'solid'
    },
    'Up' : {
        'linestyle' : 'dashed'
    },
    'Down' : {
        'linestyle' : 'dotted'
    },
    'tot' : {
        'linestyle' : 'solid',
        'color' : 'red'
    },
    'A' : {
        'linestyle' : 'solid',
        'color' : 'green'
    },
    'B' : {
        'linestyle' : 'solid',
        'color' : 'blue'
    },
    'C' : {
        'linestyle' : 'solid',
        'color' : 'orange'
    },
    'D' : {
        'linestyle' : 'solid',
        'color' : 'purple'
    },
    'E' : {
        'linestyle' : 'solid',
        'color' : 'yellow'
    },
    'F' : {
        'linestyle' : 'solid',
        'color' : 'cyan'
    },
    'highHT' : {
        'linestyle' : 'dashed'
    },
    'lowHT' : {
        'linestyle' : 'dotted'
    },
}

hatch_density = 4
opts_unc = {
    "step": "post",
    "color": (0, 0, 0, 0.4),
    "facecolor": (0, 0, 0, 0.0),
    "linewidth": 0,
    "hatch": '/'*hatch_density,
    "zorder": 2
}

patch_opts = {
    'data' : {'vmin' : 0.4, 'vmax' : 1.0},
    'mc'   :   {'vmin' : 0.4, 'vmax' : 1.0},
    'sf'   :   {'vmin' : 0.75, 'vmax' : 1.2, 'label' : "Trigger SF"},
    'unc_data' : {'vmax' : 0.05},
    'unc_mc'   : {'vmax' : 0.05},
    'unc_sf'   : {'vmax' : 0.05, 'label' : "Trigger SF unc."},
    'unc_rel_data' : {'vmax' : 0.05},
    'unc_rel_mc'   : {'vmax' : 0.05},
    'unc_rel_sf'   : {'vmax' : 0.05, 'label' : "Trigger SF unc."},
    'ratio_sf' : {'vmin' : 0.95, 'vmax' : 1.05, 'label' : "ratio SF var./nom."},
}

round_opts = {
    'data' : {'round' : 2},
    'mc'   : {'round' : 2},
    'sf'   : {'round' : 2},
    'unc_data' : {'round' : 2},
    'unc_mc'   : {'round' : 2},
    'unc_sf'   : {'round' : 2},
    'unc_rel_data' : {'round' : 2},
    'unc_rel_mc'   : {'round' : 2},
    'unc_rel_sf'   : {'round' : 2},
    'ratio_sf' : {'round' : 3},
}

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

def rearrange_axes(accumulator):
    for (histname, h) in accumulator.items():
        if h.axes()[-1] == h.sparse_axes()[-1]:
            accumulator[histname] = Hist("$N_{events}$", *h.sparse_axes(), *h.dense_axes())

def uncertainty_efficiency(eff, den, sumw2_num=None, sumw2_den=None, mc=False):
    if mc:
        if (sumw2_num is None) | (sumw2_den is None):
            sys.exit("Missing sum of squared weights.")
        return np.sqrt( ((1 - 2*eff) / den**2)*sumw2_num + (eff**2 / den**2)*sumw2_den )
    else:
        if (sumw2_num is not None) | (sumw2_den is not None):
            sys.exit("Sum of squared weights was passed but the type 'mc' was not set to True.")
        return np.sqrt( eff * (1 - eff) / den )

def uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc):
    return (eff_data/eff_mc) * np.sqrt( (unc_eff_data/eff_data)**2 + (unc_eff_mc/eff_mc)**2 )

def plot_variation(x, y, yerr, xerr, xlabel, ylabel, syst, var, opts, ax, data=False, sf=False, **kwargs):
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    hep.cms.text("Preliminary", loc=0, ax=ax)
    hep.cms.lumitext(text=f'{kwargs["totalLumi"]}' + r' fb$^{-1}$, 13 TeV,' + f' {kwargs["year"]}', fontsize=18, ax=ax)
    #plot.plot1d(eff[['mc','data']], ax=ax_eff, error_opts=opts_datamc);
    if data & (not sf) & (var == 'nominal'):
        if not 'era' in var:
            ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts)
            return
    elif data & ( not ((var == 'nominal') | ('era' in var)) ):
        return
    lines = ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts)
    handles, labels = ax.get_legend_handles_labels()
    if kwargs['histname'] in config.plot_options['scalefactor' if sf else 'efficiency'][kwargs['year']][kwargs['cat']].keys():
        ylim = config.plot_options['scalefactor' if sf else 'efficiency'][kwargs['year']][kwargs['cat']][kwargs['histname']]['ylim']
    else:
        ylim = (0.7, 1.3) if sf else (0, 1)

    if (syst != 'nominal') & (syst not in ['era', 'HT']):
        handles[-1].set_label(labels[-1]+f' {var}')
        if var != 'nominal':
            errorbar_x = lines[-1][0]
            errorbar_y = lines[-1][1]
            errorbar_x.set_linestyle(opts_errorbar[var.split(syst)[-1]]['linestyle'])
            errorbar_y.set_linewidth(0)
    ax.legend()
    ax.set_xlabel(xlabel, fontsize=kwargs['fontsize'])
    ax.set_ylabel(ylabel, fontsize=kwargs['fontsize'])
    ax.set_xlim(*config.variables[kwargs['variable']]['xlim'])
    ax.set_ylim(*ylim)

def plot_ratio(x, y, ynom, yerrnom, xerr, edges, xlabel, ylabel, syst, var, opts, ax, data=False, sf=False, **kwargs):
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    if not sf:
        return
    if var == 'nominal':
        return

    ratio = y/ynom
    unc_ratio = (yerrnom/ynom) * ratio
    lines = ax.errorbar(x, ratio, yerr=0, xerr=xerr, **opts)
    lo = 1 - unc_ratio
    hi = 1 + unc_ratio
    unc_band = np.nan_to_num( np.array([lo, hi]), nan=1 )
    print(unc_band)
    ax.fill_between(edges, np.r_[unc_band[0], unc_band[0, -1]], np.r_[unc_band[1], unc_band[1, -1]], **opts_unc)
    if kwargs['histname'] in config.plot_options['ratio'][kwargs['year']][kwargs['cat']].keys():
        ylim = config.plot_options['ratio'][kwargs['year']][kwargs['cat']][kwargs['histname']]['ylim']
    else:
        ylim = (0.7, 1.3)

    if syst != 'nominal':
        if var != 'nominal':
            errorbar_x = lines[-1][0]
            errorbar_y = lines[-1][1]
            if syst == 'HT':
                key = var
            else:
                key = var.split(syst)[-1]
            errorbar_x.set_linestyle(opts_errorbar[key]['linestyle'])
            errorbar_y.set_linestyle(opts_errorbar[key]['linestyle'])
    ax.set_xlabel(xlabel, fontsize=kwargs['fontsize'])
    ax.set_ylabel(ylabel, fontsize=kwargs['fontsize'])
    ax.set_ylim(*ylim)

def plot_residue(x, y, ynom, yerrnom, xerr, edges, xlabel, ylabel, syst, var, opts, ax, data=False, sf=False, **kwargs):
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    if not sf:
        return
    if var == 'nominal':
        return

    residue = (y-ynom) / ynom
    ratio = y/ynom
    unc_residue = (yerrnom/ynom) * ratio
    lines = ax.errorbar(x, residue, yerr=0, xerr=xerr, **opts)
    lo = - unc_residue
    hi = unc_residue
    unc_band = np.nan_to_num( np.array([lo, hi]), nan=1 )
    ax.fill_between(edges, np.r_[unc_band[0], unc_band[0, -1]], np.r_[unc_band[1], unc_band[1, -1]], **opts_unc)
    if kwargs['histname'] in config.plot_options['ratio'][kwargs['year']][kwargs['cat']].keys():
        ylim = config.plot_options['residue'][kwargs['year']][kwargs['cat']][kwargs['histname']]['ylim']
    else:
        ylim = (-0.3, 0.3)

    if syst != 'nominal':
        if var != 'nominal':
            errorbar_x = lines[-1][0]
            errorbar_y = lines[-1][1]
            if syst == 'HT':
                key = var
            else:
                key = var.split(syst)[-1]
            errorbar_x.set_linestyle(opts_errorbar[key]['linestyle'])
            errorbar_y.set_linestyle(opts_errorbar[key]['linestyle'])
    ax.set_xlabel(xlabel, fontsize=kwargs['fontsize'])
    ax.set_ylabel(ylabel, fontsize=kwargs['fontsize'])
    ax.set_ylim(*ylim)

class EfficiencyMap:

    def __init__(self, config, histname, h) -> None:
        self.config = config
        self.plot_dir = os.path.join(self.config.plots, "trigger_efficiency")
        self.fontsize = self.config.plot_options["fontsize"]
        plt.style.use([hep.style.ROOT, {'font.size': self.fontsize}])
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.histname = histname
        self.h = h
        self.datasets = [str(s) for s in h.identifiers('sample')]
        self.datasets_data = list(filter(lambda x : 'DATA' in x, self.datasets))
        self.datasets_mc   = list(filter(lambda x : 'TTTo' in x, self.datasets))
        self.years = [str(s) for s in self.h.identifiers('year')]
        self.categories = [str(s) for s in self.h.identifiers('cat')]
        self.dim = h.dense_dim()

        if self.dim == 1:
            self.variable = self.histname.split('hist_')[1]
            if len(self.variable.split('_')) == 2:
                self.varname_x  = self.variable.split('_')[1]
            elif len(self.variable.split('_')) == 1:
                self.varname_x  = self.variable.split('_')[0]
            self.varnames = [self.varname_x]
            if self.variable in self.config.plot_options["rebin"].keys():
                self.h = self.h.rebin(self.varname_x, Bin(self.varname_x, self.h.axis(self.varname_x).label, **self.config.plot_options["rebin"][variable]["binning"]))
            self.axis_x = self.h.dense_axes()[-1]
            self.binwidth_x = np.ediff1d(self.axis_x.edges())
            self.bincenter_x = self.axis_x.edges()[:-1] + 0.5*self.binwidth_x
            self.fields = {self.varname_x : self.bincenter_x}
        elif self.dim == 2:
            self.variable2d = self.histname.lstrip('hist2d_') 
            self.varname_x = list(self.config.variables2d[self.variable2d].keys())[0]
            self.varname_y = list(self.config.variables2d[self.variable2d].keys())[1]
            self.varnames = [self.varname_x, self.varname_y]
            self.axis_x = self.h.axis(self.varname_x)
            self.axis_y = self.h.axis(self.varname_y)
            self.binwidth_x = np.ediff1d(self.axis_x.edges())
            self.binwidth_y = np.ediff1d(self.axis_y.edges())
            self.bincenter_x = self.axis_x.edges()[:-1] + 0.5*self.binwidth_x
            self.bincenter_y = self.axis_y.edges()[:-1] + 0.5*self.binwidth_y
            self.y, self.x = np.meshgrid(self.bincenter_y, self.bincenter_x)
            self.fields = {self.varname_x : self.x.flatten(), self.varname_y : self.y.flatten()}
            corr_dict = processor.defaultdict_accumulator(float)
        else:
            raise NotImplementedError

    def define_datamc(self, year):
        self.totalLumi = femtobarn(lumi[year]['tot'], digits=1)
        self.h_data = self.h[(self.datasets_data, self.categories, year, )].sum('sample', 'year')
        self.h_mc   = self.h[(self.datasets_mc, self.categories, year, )].sum('sample', 'year')
        self.axis_cat          = self.h_mc.axis('cat')
        self.axis_var          = self.h_mc.axis('var')
        self.axes_electron     = [self.h_mc.axis(varname) for varname in self.varnames]
        # We extract all the variations saved in the histograms and the name of the systematics
        variations = [str(s) for s in self.h.identifiers('var')]
        self.systematics = ['nominal'] + [s.split("Up")[0] for s in variations if 'Up' in s]
        self.corrections = []

    def define_1d_figures(self, year, cat, syst):
        if self.dim == 1:
            self.fig_eff, self.ax_eff = plt.subplots(1,1,figsize=[10,10])
            self.fig_sf1, (self.ax_sf1, self.ax_sf_ratio)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
            self.fig_sf2, (self.ax_sf2, self.ax_sf_residue)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
            self.filepath_eff = os.path.join(self.plot_dir, f"{self.histname}_{year}_eff_datamc_{cat}_{syst}.png")
            self.filepath_sf = os.path.join(self.plot_dir, f"{self.histname}_{year}_sf_{cat}_{syst}.png")
            self.filepath_sf_residue = os.path.join(self.plot_dir, f"{self.histname}_{year}_sf_{cat}_{syst}_residue.png")
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"1D figures cannot be defined for histogram with dimension {self.dim}")

    def define_variations(self, syst):
        self.eff_nominal = None
        self.unc_eff_nominal = None
        self.ratio_sf = None
        self.variations = ['nominal', f'{syst}Down', f'{syst}Up'] if syst != 'nominal' else ['nominal']

    def compute_efficiency(self, cat, var):
        num_data = self.h_data[cat,'nominal',:].sum('cat', 'var').values()[()]
        den_data = self.h_data['inclusive','nominal',:].sum('cat', 'var').values()[()]
        eff_data = np.nan_to_num( num_data / den_data )
        num_mc, sumw2_num_mc   = self.h_mc[cat,var,:].sum('cat', 'var').values(sumw2=True)[()]
        den_mc, sumw2_den_mc   = self.h_mc['inclusive',var,:].sum('cat', 'var').values(sumw2=True)[()]
        eff_mc   = np.nan_to_num( num_mc / den_mc )
        sf       = np.nan_to_num( eff_data / eff_mc )
        sf       = np.where(sf < 100, sf, 100)
        if var == 'nominal':
            self.sf_nominal = sf
        unc_eff_data = uncertainty_efficiency(eff_data, den_data)
        unc_eff_mc   = uncertainty_efficiency(eff_mc, den_mc, sumw2_num_mc, sumw2_den_mc, mc=True)
        unc_sf       = uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc)
        unc_rel_eff_data = np.nan_to_num(unc_eff_data / eff_data)
        unc_rel_eff_mc   = np.nan_to_num(unc_eff_mc / eff_mc)
        unc_rel_sf       = np.nan_to_num(unc_sf / sf)
        self.eff     = Hist("Trigger efficiency", self.axis_cat, *self.axes_electron)
        self.unc_eff = Hist("Trigger eff. unc.", self.axis_cat, *self.axes_electron)
        self.unc_rel_eff = Hist("Trigger eff. relative unc. ", self.axis_cat, *self.axes_electron)

        if self.eff_nominal != None:
            print(cat, var)
            self.ratio = np.nan_to_num(sf / self.sf_nominal)
            self.ratio_sf = Hist("SF ratio", self.axis_cat, *self.axes_electron)
            self.ratio_sf.fill(cat='ratio_sf', **self.fields, weight=self.ratio.flatten())

        self.eff.fill(cat='data', **self.fields, weight=eff_data.flatten())
        self.eff.fill(cat='mc', **self.fields, weight=eff_mc.flatten())
        self.eff.fill(cat='sf', **self.fields, weight=sf.flatten())
        self.unc_eff.fill(cat='unc_data', **self.fields, weight=unc_eff_data.flatten())
        self.unc_eff.fill(cat='unc_mc', **self.fields, weight=unc_eff_mc.flatten())
        self.unc_eff.fill(cat='unc_sf', **self.fields, weight=unc_sf.flatten())
        self.unc_rel_eff.fill(cat='unc_rel_data', **self.fields, weight=unc_rel_eff_data.flatten())
        self.unc_rel_eff.fill(cat='unc_rel_mc', **self.fields, weight=unc_rel_eff_mc.flatten())
        self.unc_rel_eff.fill(cat='unc_rel_sf', **self.fields, weight=unc_rel_sf.flatten())
        if var == 'nominal':
            self.eff_nominal = self.eff
            self.unc_eff_nominal = self.unc_eff

    def plot1d(self, year, cat, syst, var):
        fontsize = self.config.plot_options["fontsize"]
        if self.dim == 1:
            extra_args = {'totalLumi' : self.totalLumi, 'histname' : self.histname, 'year' : year, 'variable' : self.variable, 'config' : self.config, 'cat' : cat, 'fontsize' : fontsize}
            plot_variation(self.bincenter_x, self.eff['data'].sum('cat').values()[()], self.unc_eff['unc_data'].sum('cat').values()[()], 0.5*self.binwidth_x,
                           self.axis_x.label, self.eff.label, syst, var, opts_data, self.ax_eff, data=True, sf=False, **extra_args)
            plot_variation(self.bincenter_x, self.eff['mc'].sum('cat').values()[()], self.unc_eff['unc_mc'].sum('cat').values()[()], 0.5*self.binwidth_x,
                           self.axis_x.label, self.eff.label, syst, var, opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst], self.ax_eff, data=False, sf=False, **extra_args)

            for ax_sf in [self.ax_sf1, self.ax_sf2]:
                plot_variation(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.unc_eff['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x,
                               self.axis_x.label, "Trigger SF", syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], self.ax_sf, data=False, sf=True, **extra_args)
            plot_ratio(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.eff_nominal['sf'].sum('cat').values()[()], self.unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x, self.axis_x.edges(),
                       self.axis_x.label, 'var. / nom.', syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], self.ax_sf_ratio, data=False, sf=True, **extra_args)
            plot_residue(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.eff_nominal['sf'].sum('cat').values()[()], self.unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x, self.axis_x.edges(),
                         self.axis_x.label, '(var - nom.) / nom.', syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], self.ax_sf_residue, data=False, sf=True, **extra_args)
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"Histograms with dimension {self.dim} are not supported")

    def plot2d(self, year, cat, syst, var, save_plots=True):
        fontsize = self.config.plot_options["fontsize"]
        if self.dim == 2:
            if (syst != 'nominal') and (var == 'nominal'): pass
            for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf", "ratio_sf"], [self.eff, self.eff, self.eff, self.unc_eff, self.unc_eff, self.unc_eff, self.unc_rel_eff, self.unc_rel_eff, self.unc_rel_eff, self.ratio_sf]):
                if (var == 'nominal') and (label == "ratio_sf"): continue
                self.map2d = histo[label].sum('cat')

                if save_plots:
                    fig_map, ax_map = plt.subplots(1,1,figsize=[16,10])
                    if label == "sf":
                        self.map2d.label = "Trigger SF"
                    elif label == "unc_sf":
                        self.map2d.label = "Trigger SF unc."
                    elif label == "ratio_sf":
                        self.map2d.label = "SF var./nom."

                    plot.plot2d(self.map2d, ax=ax_map, xaxis=self.eff.axes()[-2], patch_opts=patch_opts[label])
                    hep.cms.text("Preliminary", loc=0, ax=ax_map)
                    hep.cms.lumitext(text=f'{self.totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax_map)
                    ax_map.set_title(var)
                    if self.varname_x == 'pt':
                        ax_map.set_xscale('log')
                    ax_map.set_xlim(*self.config.variables2d[self.variable2d][self.varname_x]['xlim'])
                    ax_map.set_ylim(*self.config.variables2d[self.variable2d][self.varname_y]['ylim'])
                    #xticks = [20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500]
                    #yticks = axis_y.edges()
                    if 'xticks' in self.config.variables2d[self.variable2d][self.varname_x].keys():
                        xticks = self.config.variables2d[self.variable2d][self.varname_x]['xticks']
                    else:
                        xticks = self.axis_x.edges()
                    if 'yticks' in self.config.variables2d[self.variable2d][self.varname_y].keys():
                        yticks = self.config.variables2d[self.variable2d][self.varname_y]['yticks']
                    else:
                        yticks = self.axis_y.edges()
                    ax_map.xaxis.label.set_size(fontsize)   
                    ax_map.yaxis.label.set_size(fontsize)
                    ax_map.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                    ax_map.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                    #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                    plt.vlines(self.axis_x.edges(), self.axis_y.edges()[-1], self.axis_y.edges()[0], linestyle='--', color='gray')
                    plt.hlines(self.axis_y.edges(), self.axis_x.edges()[-1], self.axis_x.edges()[0], linestyle='--', color='gray')

                    for (i, x) in enumerate(self.bincenter_x):
                        if (x < ax_map.get_xlim()[0]) | (x > ax_map.get_xlim()[1]): continue
                        for (j, y) in enumerate(self.bincenter_y):
                            color = "w" if self.map2d.values()[()][i][j] < patch_opts[label]['vmax'] else "k"
                            #color = "w"
                            if self.histname == "hist2d_electron_etaSC_vs_electron_pt":
                                ax_map.text(x, y, f"{round(self.map2d.values()[()][i][j], round_opts[label]['round'])}",
                                        color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")
                            else:
                                ax_map.text(x, y, f"{round(self.map2d.values()[()][i][j], round_opts[label]['round'])}",
                                        color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")
                self.save2d(year, cat, syst, var, label, save_plots)
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"Histograms with dimension {self.dim} are not supported")

    def save1d(self):
        if self.dim == 1:
            print("Saving", self.filepath_eff)
            self.fig_eff.savefig(self.filepath_eff, dpi=self.config.plot_options['dpi'], format="png")
            plt.close(self.fig_eff)
            print("Saving", self.filepath_sf)
            self.fig_sf1.savefig(self.filepath_sf, dpi=self.config.plot_options['dpi'], format="png")
            plt.close(self.fig_sf1)
            print("Saving", self.filepath_sf_residue)
            self.fig_sf2.savefig(self.filepath_sf_residue, dpi=self.config.plot_options['dpi'], format="png")
            plt.close(self.fig_sf2)
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"Histograms with dimension {self.dim} are not supported")

    def save2d(self, year, cat, syst, var, label, save_plots):
        if self.dim == 2:
            if (syst != 'nominal') and (var == 'nominal'): return
            if (var == 'nominal') and (label == "ratio_sf"): return
            filepath = os.path.join(self.plot_dir, f"{self.histname}_{year}_{label}_{cat}_{var}.png")

            if save_plots:
                print("Saving", filepath)
                plt.savefig(filepath, dpi=self.config.plot_options['dpi'], format="png")
            if self.config.output_triggerSF:
                if label in ["sf", "unc_sf"]:
                    A = self.map2d.to_hist()
                    eff_map = self.map2d.values()[()]
                    if (syst == "nominal") and (var == "nominal") and (label == "unc_sf"):
                        ratio = self.eff_nominal["sf"].sum('cat').values()[()]
                        unc = np.array(eff_map)
                        ratios  = [ ratio - unc, ratio + unc ]
                        print(ratios[0])
                        names   = [ f"sf_statDown", f"sf_statUp" ]
                        sfhists = [ hist.Hist(A.axes[0], A.axes[1], data=ratios[i], label='out', name=names[i]) for i in range(2) ]
                        descriptions = [ f"SF matching the semileptonic trigger efficiency in MC and data.\nVariation: {name.split('_')[-1]}" for name in names ]
                    elif label == "sf":
                        ratio = np.where(eff_map != 0, eff_map, 1.)
                        sfhist = hist.Hist(A.axes[0], A.axes[1], data=ratio)
                        sfhist.label = "out"
                        sfhist.name = f"sf_{var}"
                        description = f"SF matching the semileptonic trigger efficiency in MC and data.\nVariation: {var}"
                        sfhists = [sfhist]
                        descriptions = [description]
                    else:
                        return
                    for i in range(len(sfhists)):
                        clibcorr = correctionlib.convert.from_histogram(sfhists[i])
                        clibcorr.description = descriptions[i]
                        self.corrections.append(clibcorr)
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"Histograms with dimension {self.dim} are not supported")

    def save_corrections(self, year, cat):
        if not os.path.exists(self.config.output_triggerSF):
            os.makedirs(self.config.output_triggerSF)
        local_folder = os.path.join(self.config.output, *self.config.output_triggerSF.split('/')[-2:])
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        map_name = self.histname.split("hist2d_")[-1]
        filename = f'sf_trigger_{map_name}_{year}_{cat}.json'
        if self.h.dense_dim() == 2:
            print("***************************")
            for correction in self.corrections:
                print(correction.name)
            print("***************************")
            cset = correctionlib.schemav2.CorrectionSet(
                schema_version=2,
                description="Semileptonic trigger efficiency SF",
                corrections=self.corrections,
            )
            rich.print(cset)
            for outdir in [self.config.output_triggerSF, local_folder]:
                outfile_triggersf = os.path.join(outdir, filename)
                outfile_triggersf = overwrite_check(outfile_triggersf)
                print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
                with open(outfile_triggersf, "w") as fout:
                    fout.write(cset.json(exclude_unset=True))

def plot_efficiency_maps(accumulator, config):
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue        
        print("Histogram:", histname)

        efficiency_map = EfficiencyMap(config, histname, h)

        for year in efficiency_map.years:
            efficiency_map.define_datamc(year)

            for cat in [c for c in efficiency_map.categories if 'pass' in c]:

                for syst in efficiency_map.systematics:
                    efficiency_map.define_1d_figures(year, cat, syst)
                    efficiency_map.define_variations(syst)

                    for var in efficiency_map.variations:
                        efficiency_map.compute_efficiency(cat, var)
                        efficiency_map.plot1d(year, cat, syst, var)
                        efficiency_map.plot2d(year, cat, syst, var, save_plots=False)

                    efficiency_map.save1d()

                efficiency_map.save_corrections(year, cat)

def plot_efficiency(accumulator, config):
    plot_dir = os.path.join(config.plots, "trigger_efficiency")
    fontsize = config.plot_options["fontsize"]
    plt.style.use([hep.style.ROOT, {'font.size': fontsize}])

    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue        
        print(histname)

        datasets = [str(s) for s in h.identifiers('sample')]
        datasets_data = list(filter(lambda x : 'DATA' in x, datasets))
        datasets_mc   = list(filter(lambda x : 'TTTo' in x, datasets))

        if h.dense_dim() == 1:
            variable = histname.split('hist_')[1]
            if len(variable.split('_')) == 2:
                varname_x  = variable.split('_')[1]
            elif len(variable.split('_')) == 1:
                varname_x  = variable.split('_')[0]
            varnames = [varname_x]
            if variable in config.plot_options["rebin"].keys():
                h = h.rebin(varname_x, Bin(varname_x, h.axis(varname_x).label, **config.plot_options["rebin"][variable]["binning"]))
            axis_x = h.dense_axes()[-1]
            binwidth_x = np.ediff1d(axis_x.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            fields = {varname_x : bincenter_x}
        elif h.dense_dim() == 2:
            variable2d = histname.lstrip('hist2d_') 
            varname_x = list(config.variables2d[variable2d].keys())[0]
            varname_y = list(config.variables2d[variable2d].keys())[1]
            varnames = [varname_x, varname_y]
            axis_x = h.axis(varname_x)
            axis_y = h.axis(varname_y)
            binwidth_x = np.ediff1d(axis_x.edges())
            binwidth_y = np.ediff1d(axis_y.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            bincenter_y = axis_y.edges()[:-1] + 0.5*binwidth_y
            y, x = np.meshgrid(bincenter_y, bincenter_x)
            fields = {varname_x : x.flatten(), varname_y : y.flatten()}
            corr_dict = processor.defaultdict_accumulator(float)
        else:
            raise NotImplementedError

        years = [str(s) for s in h.identifiers('year')]
        for year in years:
            totalLumi = femtobarn(lumi[year]['tot'], digits=1)
            categories = [str(s) for s in h.identifiers('cat')]
            h_data = h[(datasets_data, categories, year, )].sum('sample', 'year')
            h_mc   = h[(datasets_mc, categories, year, )].sum('sample', 'year')
            axis_cat          = h_mc.axis('cat')
            axis_var          = h_mc.axis('var')
            axes_electron     = [h_mc.axis(varname) for varname in varnames]
            # We extract all the variations saved in the histograms and the name of the systematics
            variations = [str(s) for s in h.identifiers('var')]
            systematics = ['nominal'] + [s.split("Up")[0] for s in variations if 'Up' in s]
            #for cat in categories:
            for cat in [c for c in categories if 'pass' in c]:
                # Loop over nominal + systematics
                for syst in systematics:
                    if h.dense_dim() == 1:
                        fig_eff, ax_eff = plt.subplots(1,1,figsize=[10,10])
                        fig_sf1, (ax_sf1, ax_sf_ratio)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                        fig_sf2, (ax_sf2, ax_sf_residue)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                        filepath_eff = os.path.join(plot_dir, f"{histname}_{year}_eff_datamc_{cat}_{syst}.png")
                        filepath_sf = os.path.join(plot_dir, f"{histname}_{year}_sf_{cat}_{syst}.png")
                        filepath_sf_residue = os.path.join(plot_dir, f"{histname}_{year}_sf_{cat}_{syst}_residue.png")
                    elif h.dense_dim() == 2:
                        pass
                    # Loop over nominal + up/down variations. If syst is 'nominal', only the nominal variation is plotted.
                    eff_nominal = None
                    unc_eff_nominal = None
                    ratio_sf = None
                    for var in ['nominal', f'{syst}Up', f'{syst}Down'] if syst != 'nominal' else ['nominal']:
                        num_data = h_data[cat,'nominal',:].sum('cat', 'var').values()[()]
                        den_data = h_data['inclusive','nominal',:].sum('cat', 'var').values()[()]
                        eff_data = np.nan_to_num( num_data / den_data )
                        num_mc, sumw2_num_mc   = h_mc[cat,var,:].sum('cat', 'var').values(sumw2=True)[()]
                        den_mc, sumw2_den_mc   = h_mc['inclusive',var,:].sum('cat', 'var').values(sumw2=True)[()]
                        eff_mc   = np.nan_to_num( num_mc / den_mc )
                        sf       = np.nan_to_num( eff_data / eff_mc )
                        sf       = np.where(sf < 100, sf, 100)
                        if var == 'nominal':
                            sf_nominal = sf
                        unc_eff_data = uncertainty_efficiency(eff_data, den_data)
                        unc_eff_mc   = uncertainty_efficiency(eff_mc, den_mc, sumw2_num_mc, sumw2_den_mc, mc=True)
                        unc_sf       = uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc)
                        unc_rel_eff_data = np.nan_to_num(unc_eff_data / eff_data)
                        unc_rel_eff_mc   = np.nan_to_num(unc_eff_mc / eff_mc)
                        unc_rel_sf       = np.nan_to_num(unc_sf / sf)
                        eff     = Hist("Trigger efficiency", axis_cat, *axes_electron)
                        unc_eff = Hist("Trigger eff. unc.", axis_cat, *axes_electron)
                        unc_rel_eff = Hist("Trigger eff. relative unc. ", axis_cat, *axes_electron)

                        if eff_nominal != None:
                            ratio = np.nan_to_num(sf / sf_nominal)
                            ratio_sf = Hist("SF ratio", axis_cat, *axes_electron)
                            ratio_sf.fill(cat='ratio_sf', **fields, weight=ratio.flatten())

                        eff.fill(cat='data', **fields, weight=eff_data.flatten())
                        eff.fill(cat='mc', **fields, weight=eff_mc.flatten())
                        eff.fill(cat='sf', **fields, weight=sf.flatten())
                        unc_eff.fill(cat='unc_data', **fields, weight=unc_eff_data.flatten())
                        unc_eff.fill(cat='unc_mc', **fields, weight=unc_eff_mc.flatten())
                        unc_eff.fill(cat='unc_sf', **fields, weight=unc_sf.flatten())
                        unc_rel_eff.fill(cat='unc_rel_data', **fields, weight=unc_rel_eff_data.flatten())
                        unc_rel_eff.fill(cat='unc_rel_mc', **fields, weight=unc_rel_eff_mc.flatten())
                        unc_rel_eff.fill(cat='unc_rel_sf', **fields, weight=unc_rel_sf.flatten())
                        if var == 'nominal':
                            eff_nominal = eff
                            unc_eff_nominal = unc_eff
                            sf_nominal = sf

                        if h.dense_dim() == 1:
                            extra_args = {'totalLumi' : totalLumi, 'histname' : histname, 'year' : year, 'variable' : variable, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                            plot_variation(bincenter_x, eff['data'].sum('cat').values()[()], unc_eff['unc_data'].sum('cat').values()[()], 0.5*binwidth_x,
                                           axis_x.label, eff.label, syst, var, opts_data, ax_eff, data=True, sf=False, **extra_args)

                            plot_variation(bincenter_x, eff['mc'].sum('cat').values()[()], unc_eff['unc_mc'].sum('cat').values()[()], 0.5*binwidth_x,
                                           axis_x.label, eff.label, syst, var, opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst], ax_eff, data=False, sf=False, **extra_args)

                            for ax_sf in [ax_sf1, ax_sf2]:
                                plot_variation(bincenter_x, eff['sf'].sum('cat').values()[()], unc_eff['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x,
                                               axis_x.label, "Trigger SF", syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], ax_sf, data=False, sf=True, **extra_args)
                            plot_ratio(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                                       axis_x.label, 'var. / nom.', syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], ax_sf_ratio, data=False, sf=True, **extra_args)
                            plot_residue(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                                         axis_x.label, '(var - nom.) / nom.', syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], ax_sf_residue, data=False, sf=True, **extra_args)

                        elif h.dense_dim() == 2:
                            if (syst != 'nominal') and (var == 'nominal'): continue
                            for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf", "ratio_sf"], [eff, eff, eff, unc_eff, unc_eff, unc_eff, unc_rel_eff, unc_rel_eff, unc_rel_eff, ratio_sf]):
                                if (var == 'nominal') and (label == "ratio_sf"): continue
                                fig_map, ax_map = plt.subplots(1,1,figsize=[16,10])
                                efficiency_map = histo[label].sum('cat')
                                if label == "sf":
                                    efficiency_map.label = "Trigger SF"
                                elif label == "unc_sf":
                                    efficiency_map.label = "Trigger SF unc."
                                elif label == "ratio_sf":
                                    efficiency_map.label = "SF var./nom."

                                plot.plot2d(efficiency_map, ax=ax_map, xaxis=eff.axes()[-2], patch_opts=patch_opts[label])
                                hep.cms.text("Preliminary", loc=0, ax=ax_map)
                                hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax_map)
                                ax_map.set_title(var)
                                if varname_x == 'pt':
                                    ax_map.set_xscale('log')
                                ax_map.set_xlim(*config.variables2d[variable2d][varname_x]['xlim'])
                                ax_map.set_ylim(*config.variables2d[variable2d][varname_y]['ylim'])
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
                                ax_map.xaxis.label.set_size(fontsize)   
                                ax_map.yaxis.label.set_size(fontsize)
                                ax_map.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                                ax_map.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                                #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                                plt.vlines(axis_x.edges(), axis_y.edges()[-1], axis_y.edges()[0], linestyle='--', color='gray')
                                plt.hlines(axis_y.edges(), axis_x.edges()[-1], axis_x.edges()[0], linestyle='--', color='gray')

                                for (i, x) in enumerate(bincenter_x):
                                    if (x < ax_map.get_xlim()[0]) | (x > ax_map.get_xlim()[1]): continue
                                    for (j, y) in enumerate(bincenter_y):
                                        color = "w" if efficiency_map.values()[()][i][j] < patch_opts[label]['vmax'] else "k"
                                        #color = "w"
                                        if histname == "hist2d_electron_etaSC_vs_electron_pt":
                                            ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                                    color=color, ha="center", va="center", fontsize=config.plot_options["fontsize_map"], fontweight="bold")
                                        else:
                                            ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                                    color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")

                                #plt.title("Electron trigger SF")
                                #cbar = plt.colorbar()
                                #cbar.set_label(legend, fontsize=fontsize)

                                filepath = os.path.join(plot_dir, f"{histname}_{year}_{label}_{cat}_{var}.png")

                                print("Saving", filepath)
                                plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                                if config.output_triggerSF:
                                    if label in ["sf", "unc_sf"]:
                                        # An extra column filled with ones is added to the efficiency map such that events with 0 electrons
                                        # can be associated with a SF=1. To do that, the None values will have to be filled with -999
                                        col = [efficiency_map.values()[()][0]]
                                        eff_map = np.concatenate( (np.ones_like(col), efficiency_map.values()[()]) )
                                        axis_x_extended = np.concatenate( ([-1000], axis_x.edges()) )
                                        efflookup = dense_lookup(eff_map, [axis_x_extended, axis_y.edges()])
                                        if (syst == "nominal") and (var == "nominal") and (label == "unc_sf"):
                                            corr_dict.update({"unc_sf_stat" : efflookup})
                                            efflookup_identity = dense_lookup(np.ones_like(eff_map), [axis_x_extended, axis_y.edges()])
                                            corr_dict.update({"identity" : efflookup_identity})
                                        elif label == "sf":
                                            #efflookup = dense_lookup(efficiency_map.values()[()], [axis_x_extended, axis_y.edges()])
                                            corr_dict.update({f"sf_{var}" : efflookup})

                    if h.dense_dim() == 1:
                        print("Saving", filepath_eff)
                        fig_eff.savefig(filepath_eff, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_eff)
                        print("Saving", filepath_sf)
                        fig_sf1.savefig(filepath_sf, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_sf1)
                        print("Saving", filepath_sf_residue)
                        fig_sf2.savefig(filepath_sf_residue, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_sf2)

                if not os.path.exists(config.output_triggerSF):
                    os.makedirs(config.output_triggerSF)
                local_folder = os.path.join(config.output, *config.output_triggerSF.split('/')[-2:])
                if not os.path.exists(local_folder):
                    os.makedirs(local_folder)
                map_name = histname.split("hist2d_")[-1]
                filename = f'sf_trigger_{map_name}_{year}_{cat}.coffea'
                if h.dense_dim() == 2:
                    for outdir in [config.output_triggerSF, local_folder]:
                        outfile_triggersf = os.path.join(outdir, filename)
                        outfile_triggersf = overwrite_check(outfile_triggersf)
                        print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
                        save(corr_dict, outfile_triggersf)
                        efflookup_check = load(outfile_triggersf)
                        sf_trigger = efflookup_check[list(efflookup_check.keys())[0]]
                        print(sf_trigger(34, 1.0))

def plot_efficiency_eras(accumulator, config):
    plot_dir = os.path.join(config.plots, "trigger_efficiency")
    fontsize = config.plot_options["fontsize"]
    plt.style.use([hep.style.ROOT, {'font.size': fontsize}])

    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue        

        datasets = [str(s) for s in h.identifiers('sample')]
        datasets_data = list(filter(lambda x : 'DATA' in x, datasets))
        datasets_mc   = list(filter(lambda x : 'TTTo' in x, datasets))

        if h.dense_dim() == 1:
            variable = histname.split('hist_')[1]
            if len(variable.split('_')) == 2:
                varname_x  = variable.split('_')[1]
            elif len(variable.split('_')) == 1:
                varname_x  = variable.split('_')[0]
            varnames = [varname_x]
            if variable in config.plot_options["rebin"].keys():
                h = h.rebin(varname_x, Bin(varname_x, h.axis(varname_x).label, **config.plot_options["rebin"][variable]["binning"]))
            axis_x = h.dense_axes()[-1]
            binwidth_x = np.ediff1d(axis_x.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            fields = {varname_x : bincenter_x}
        elif h.dense_dim() == 2:
            variable2d = histname.lstrip('hist2d_') 
            varname_x = list(config.variables2d[variable2d].keys())[0]
            varname_y = list(config.variables2d[variable2d].keys())[1]
            varnames = [varname_x, varname_y]
            axis_x = h.axis(varname_x)
            axis_y = h.axis(varname_y)
            binwidth_x = np.ediff1d(axis_x.edges())
            binwidth_y = np.ediff1d(axis_y.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            bincenter_y = axis_y.edges()[:-1] + 0.5*binwidth_y
            y, x = np.meshgrid(bincenter_y, bincenter_x)
            fields = {varname_x : x.flatten(), varname_y : y.flatten()}
            corr_dict = processor.defaultdict_accumulator(float)
        else:
            raise NotImplementedError

        years = [str(s) for s in h.identifiers('year')]
        for year in years:
            #totalLumi = femtobarn(lumi[year], digits=1)
            categories = [str(s) for s in h.identifiers('cat')]
            eras = ['tot'] + [str(s) for s in h.identifiers('era') if str(s) != 'MC']
            axis_cat          = h.axis('cat')
            axis_var          = h.axis('var')
            axes_electron     = [h.axis(varname) for varname in varnames]
            # We extract all the variations saved in the histograms and the name of the systematics
            variations = [str(s) for s in h.identifiers('var')]
            systematics = ['nominal']
            #for cat in categories:
            for cat in [c for c in categories if 'pass' in c]:
                # Loop over nominal + systematics
                for syst in systematics:
                    if h.dense_dim() == 1:
                        fig_eff, ax_eff = plt.subplots(1,1,figsize=[10,10])
                        fig_sf1, (ax_sf1, ax_sf_ratio)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                        fig_sf2, (ax_sf2, ax_sf_residue)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                        filepath_eff = os.path.join(plot_dir, f"{histname}_{year}_eff_datamc_{cat}_era.png")
                        filepath_sf = os.path.join(plot_dir, f"{histname}_{year}_sf_{cat}_era.png")
                        filepath_sf_residue = os.path.join(plot_dir, f"{histname}_{year}_sf_{cat}_era_residue.png")
                    elif h.dense_dim() == 2:
                        pass
                    # Loop over nominal + up/down variations. If syst is 'nominal', only the nominal variation is plotted.
                    eff_nominal = None
                    unc_eff_nominal = None
                    ratio_sf = None
                    for var in ['nominal']:
                        for era in eras:
                            if h.dense_dim() == 2:
                                totalLumi = femtobarn(lumi[year][era], digits=1)
                            else:
                                totalLumi = femtobarn(lumi[year]['tot'], digits=1)
                            h_mc   = h[(datasets_mc, categories, year, 'MC', )].sum('sample', 'year', 'era')
                            if era == 'tot':
                                h_data = h[(datasets_data, categories, year, )].sum('sample', 'year', 'era')
                            else:
                                h_data = h[(datasets_data, categories, year, era, )].sum('sample', 'year', 'era')
                                lumi_frac = lumi[year][era] / lumi[year]['tot']
                                h_mc.scale(lumi_frac)
                            num_data = h_data[cat,'nominal',:].sum('cat', 'var').values()[()]
                            den_data = h_data['inclusive','nominal',:].sum('cat', 'var').values()[()]
                            eff_data = np.nan_to_num( num_data / den_data )
                            num_mc, sumw2_num_mc   = h_mc[cat,var,:].sum('cat', 'var').values(sumw2=True)[()]
                            den_mc, sumw2_den_mc   = h_mc['inclusive',var,:].sum('cat', 'var').values(sumw2=True)[()]
                            eff_mc   = np.nan_to_num( num_mc / den_mc )
                            sf       = np.nan_to_num( eff_data / eff_mc )
                            sf       = np.where(sf < 100, sf, 100)
                            if var == 'nominal':
                                sf_nominal = sf
                            unc_eff_data = uncertainty_efficiency(eff_data, den_data)
                            unc_eff_mc   = uncertainty_efficiency(eff_mc, den_mc, sumw2_num_mc, sumw2_den_mc, mc=True)
                            unc_sf       = uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc)
                            unc_rel_eff_data = np.nan_to_num(unc_eff_data / eff_data)
                            unc_rel_eff_mc   = np.nan_to_num(unc_eff_mc / eff_mc)
                            unc_rel_sf       = np.nan_to_num(unc_sf / sf)
                            eff     = Hist("Trigger efficiency", axis_cat, *axes_electron)
                            unc_eff = Hist("Trigger eff. unc.", axis_cat, *axes_electron)
                            unc_rel_eff = Hist("Trigger eff. relative unc. ", axis_cat, *axes_electron)

                            if eff_nominal != None:
                                ratio = np.nan_to_num(sf / sf_nominal)
                                ratio_sf = Hist("SF ratio", axis_cat, *axes_electron)
                                ratio_sf.fill(cat='ratio_sf', **fields, weight=ratio.flatten())

                            eff.fill(cat='data', **fields, weight=eff_data.flatten())
                            eff.fill(cat='mc', **fields, weight=eff_mc.flatten())
                            eff.fill(cat='sf', **fields, weight=sf.flatten())
                            unc_eff.fill(cat='unc_data', **fields, weight=unc_eff_data.flatten())
                            unc_eff.fill(cat='unc_mc', **fields, weight=unc_eff_mc.flatten())
                            unc_eff.fill(cat='unc_sf', **fields, weight=unc_sf.flatten())
                            unc_rel_eff.fill(cat='unc_rel_data', **fields, weight=unc_rel_eff_data.flatten())
                            unc_rel_eff.fill(cat='unc_rel_mc', **fields, weight=unc_rel_eff_mc.flatten())
                            unc_rel_eff.fill(cat='unc_rel_sf', **fields, weight=unc_rel_sf.flatten())
                            if era == 'tot':
                                eff_nominal = eff
                                unc_eff_nominal = unc_eff
                                sf_nominal = sf

                            if h.dense_dim() == 1:
                                extra_args = {'totalLumi' : totalLumi, 'histname' : histname, 'year' : year, 'variable' : variable, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                                plot_variation(bincenter_x, eff['data'].sum('cat').values()[()], unc_eff['unc_data'].sum('cat').values()[()], 0.5*binwidth_x,
                                               axis_x.label, eff.label, 'era', f'era{era}', opts_data_eras[era], ax_eff, data=True, sf=False, **extra_args)

                                # We don't plot MC, but only the efficiency in each data taking era
                                #plot_variation(bincenter_x, eff['mc'].sum('cat').values()[()], unc_eff['unc_mc'].sum('cat').values()[()], 0.5*binwidth_x,
                                #               axis_x.label, eff.label, 'era', f'era{era}', opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst], ax_eff, data=False, sf=False, **extra_args)

                                for ax_sf in [ax_sf1, ax_sf2]:
                                    plot_variation(bincenter_x, eff['sf'].sum('cat').values()[()], unc_eff['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x,
                                                   axis_x.label, "Trigger SF", 'era', f'era{era}', opts_sf_eras[era], ax_sf, data=False, sf=True, **extra_args)
                                plot_ratio(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                                           axis_x.label, 'var. / nom.', 'era', f'era{era}', opts_sf_eras[era], ax_sf_ratio, data=False, sf=True, **extra_args)
                                plot_residue(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                                             axis_x.label, '(var - nom.) / nom.', 'era', f'era{era}', opts_sf_eras[era], ax_sf_residue, data=False, sf=True, **extra_args)

                            elif h.dense_dim() == 2:
                                if (syst != 'nominal') and (var == 'nominal'): continue
                                for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf", "ratio_sf"], [eff, eff, eff, unc_eff, unc_eff, unc_eff, unc_rel_eff, unc_rel_eff, unc_rel_eff, ratio_sf]):
                                    if (var == 'nominal') and (label == "ratio_sf"): continue
                                    fig_map, ax_map = plt.subplots(1,1,figsize=[16,10])
                                    efficiency_map = histo[label].sum('cat')
                                    if label == "sf":
                                        efficiency_map.label = "Trigger SF"
                                    elif label == "unc_sf":
                                        efficiency_map.label = "Trigger SF unc."
                                    elif label == "ratio_sf":
                                        efficiency_map.label = "SF var./nom."

                                    plot.plot2d(efficiency_map, ax=ax_map, xaxis=eff.axes()[-2], patch_opts=patch_opts[label])
                                    hep.cms.text("Preliminary", loc=0, ax=ax_map)
                                    hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax_map)
                                    ax_map.set_title(var)
                                    if varname_x == 'pt':
                                        ax_map.set_xscale('log')
                                    ax_map.set_xlim(*config.variables2d[variable2d][varname_x]['xlim'])
                                    ax_map.set_ylim(*config.variables2d[variable2d][varname_y]['ylim'])
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
                                    ax_map.xaxis.label.set_size(fontsize)   
                                    ax_map.yaxis.label.set_size(fontsize)
                                    ax_map.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                                    ax_map.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                                    #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                                    plt.vlines(axis_x.edges(), axis_y.edges()[-1], axis_y.edges()[0], linestyle='--', color='gray')
                                    plt.hlines(axis_y.edges(), axis_x.edges()[-1], axis_x.edges()[0], linestyle='--', color='gray')

                                    for (i, x) in enumerate(bincenter_x):
                                        if (x < ax_map.get_xlim()[0]) | (x > ax_map.get_xlim()[1]): continue
                                        for (j, y) in enumerate(bincenter_y):
                                            color = "w" if efficiency_map.values()[()][i][j] < patch_opts[label]['vmax'] else "k"
                                            #color = "w"
                                            if histname == "hist2d_electron_etaSC_vs_electron_pt":
                                                ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                                        color=color, ha="center", va="center", fontsize=config.plot_options["fontsize_map"], fontweight="bold")
                                            else:
                                                ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                                        color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")

                                    #plt.title("Electron trigger SF")
                                    #cbar = plt.colorbar()
                                    #cbar.set_label(legend, fontsize=fontsize)

                                    filepath = os.path.join(plot_dir, f"{histname}_{year}_{label}_{cat}_{year}{era}.png")

                                    print("Saving", filepath)
                                    plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                                    if config.output_triggerSF:
                                        if label in ["sf", "unc_sf"]:
                                            # An extra column filled with ones is added to the efficiency map such that events with 0 electrons
                                            # can be associated with a SF=1. To do that, the None values will have to be filled with -999
                                            col = [efficiency_map.values()[()][0]]
                                            eff_map = np.concatenate( (np.ones_like(col), efficiency_map.values()[()]) )
                                            axis_x_extended = np.concatenate( ([-1000], axis_x.edges()) )
                                            efflookup = dense_lookup(eff_map, [axis_x_extended, axis_y.edges()])
                                            if (syst == "nominal") and (var == "nominal") and (label == "unc_sf"):
                                                corr_dict.update({f"unc_sf_stat_{era}" : efflookup})
                                                efflookup_identity = dense_lookup(np.ones_like(eff_map), [axis_x_extended, axis_y.edges()])
                                                corr_dict.update({"identity" : efflookup_identity})
                                            elif label == "sf":
                                                corr_dict.update({f"sf_{era}" : efflookup})

                    if h.dense_dim() == 1:
                        print("Saving", filepath_eff)
                        fig_eff.savefig(filepath_eff, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_eff)
                        print("Saving", filepath_sf)
                        fig_sf1.savefig(filepath_sf, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_sf1)
                        print("Saving", filepath_sf_residue)
                        fig_sf2.savefig(filepath_sf_residue, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_sf2)

                if not os.path.exists(config.output_triggerSF):
                    os.makedirs(config.output_triggerSF)
                local_folder = os.path.join(config.output, *config.output_triggerSF.split('/')[-2:])
                if not os.path.exists(local_folder):
                    os.makedirs(local_folder)
                map_name = histname.split("hist2d_")[-1]
                filename = f'sf_trigger_{map_name}_{year}_{cat}.coffea'
                if h.dense_dim() == 2:
                    for outdir in [config.output_triggerSF, local_folder]:
                        outfile_triggersf = os.path.join(outdir, filename)
                        outfile_triggersf = overwrite_check(outfile_triggersf)
                        print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
                        save(corr_dict, outfile_triggersf)
                        efflookup_check = load(outfile_triggersf)
                        sf_trigger = efflookup_check[list(efflookup_check.keys())[0]]
                        print(sf_trigger(34, 1.0))

def plot_efficiency_ht(accumulator, config):
    plot_dir = os.path.join(config.plots, "trigger_efficiency")
    fontsize = config.plot_options["fontsize"]
    plt.style.use([hep.style.ROOT, {'font.size': fontsize}])

    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue        
        print(histname)

        datasets = [str(s) for s in h.identifiers('sample')]
        datasets_data = list(filter(lambda x : 'DATA' in x, datasets))
        datasets_mc   = list(filter(lambda x : 'TTTo' in x, datasets))

        if h.dense_dim() == 1:
            variable = histname.split('hist_')[1]
            if len(variable.split('_')) == 2:
                varname_x  = variable.split('_')[1]
            elif len(variable.split('_')) == 1:
                varname_x  = variable.split('_')[0]
            varnames = [varname_x]
            if variable in config.plot_options["rebin"].keys():
                h = h.rebin(varname_x, Bin(varname_x, h.axis(varname_x).label, **config.plot_options["rebin"][variable]["binning"]))
            axis_x = h.dense_axes()[-1]
            binwidth_x = np.ediff1d(axis_x.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            fields = {varname_x : bincenter_x}
        elif h.dense_dim() == 2:
            variable2d = histname.lstrip('hist2d_') 
            varname_x = list(config.variables2d[variable2d].keys())[0]
            varname_y = list(config.variables2d[variable2d].keys())[1]
            varnames = [varname_x, varname_y]
            axis_x = h.axis(varname_x)
            axis_y = h.axis(varname_y)
            binwidth_x = np.ediff1d(axis_x.edges())
            binwidth_y = np.ediff1d(axis_y.edges())
            bincenter_x = axis_x.edges()[:-1] + 0.5*binwidth_x
            bincenter_y = axis_y.edges()[:-1] + 0.5*binwidth_y
            y, x = np.meshgrid(bincenter_y, bincenter_x)
            fields = {varname_x : x.flatten(), varname_y : y.flatten()}
        else:
            raise NotImplementedError

        years = [str(s) for s in h.identifiers('year')]
        for year in years:
            if h.dense_dim() == 2:
                corr_dict = processor.defaultdict_accumulator(float)
                corrections = []
            totalLumi = femtobarn(lumi[year]['tot'], digits=1)
            categories = [str(s) for s in h.identifiers('cat')]
            h_data = h[(datasets_data, categories, year, )].sum('sample', 'year')
            h_mc   = h[(datasets_mc, categories, year, )].sum('sample', 'year')
            axis_cat          = h_mc.axis('cat')
            axis_var          = h_mc.axis('var')
            axes_electron     = [h_mc.axis(varname) for varname in varnames]
            # We extract all the variations saved in the histograms and the name of the systematics
            variations = [str(s) for s in h.identifiers('var')]
            syst = 'nominal'
            var  = 'nominal'
            if h.dense_dim() == 1:
                fig_eff, ax_eff = plt.subplots(1,1,figsize=[10,10])
                fig_sf1, (ax_sf1, ax_sf_ratio)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                fig_sf2, (ax_sf2, ax_sf_residue)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                
            elif h.dense_dim() == 2:
                pass

            eff_nominal = None
            unc_eff_nominal = None
            ratio_sf = None
            for cat in [c for c in categories if c.endswith(tuple(['pass', 'pass_lowHT', 'pass_highHT']))]:
                if h.dense_dim() == 1:
                    filepath_eff = os.path.join(plot_dir, f"{histname}_{year}_eff_datamc_{'_'.join(cat.split('_')[:-1])}_splitHT.png")
                    filepath_sf = os.path.join(plot_dir, f"{histname}_{year}_sf_{'_'.join(cat.split('_')[:-1])}_splitHT.png")
                    filepath_sf_residue = os.path.join(plot_dir, f"{histname}_{year}_sf_{'_'.join(cat.split('_')[:-1])}_splitHT_residue.png")

                if cat.endswith(tuple(['pass_lowHT'])):
                    cat_inclusive = [c for c in categories if c.endswith(tuple(['pass_lowHT', 'fail_lowHT']))]
                elif cat.endswith(tuple(['pass_highHT'])):
                    cat_inclusive = [c for c in categories if c.endswith(tuple(['pass_highHT', 'fail_highHT']))]
                elif cat.endswith('pass'):
                    cat_inclusive = 'inclusive'
                num_data = h_data[cat,'nominal',:].sum('cat', 'var').values()[()]
                den_data = h_data[cat_inclusive,'nominal',:].sum('cat', 'var').values()[()]
                eff_data = np.nan_to_num( num_data / den_data )
                num_mc, sumw2_num_mc   = h_mc[cat,var,:].sum('cat', 'var').values(sumw2=True)[()]
                den_mc, sumw2_den_mc   = h_mc[cat_inclusive,var,:].sum('cat', 'var').values(sumw2=True)[()]
                eff_mc   = np.nan_to_num( num_mc / den_mc )
                sf       = np.nan_to_num( eff_data / eff_mc )
                sf       = np.where(sf < 100, sf, 100)
                if var == 'nominal':
                    sf_nominal = sf
                unc_eff_data = uncertainty_efficiency(eff_data, den_data)
                unc_eff_mc   = uncertainty_efficiency(eff_mc, den_mc, sumw2_num_mc, sumw2_den_mc, mc=True)
                unc_sf       = uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc)
                unc_rel_eff_data = np.nan_to_num(unc_eff_data / eff_data)
                unc_rel_eff_mc   = np.nan_to_num(unc_eff_mc / eff_mc)
                unc_rel_sf       = np.nan_to_num(unc_sf / sf)
                eff     = Hist("Trigger efficiency", axis_cat, *axes_electron)
                unc_eff = Hist("Trigger eff. unc.", axis_cat, *axes_electron)
                unc_rel_eff = Hist("Trigger eff. relative unc. ", axis_cat, *axes_electron)

                if eff_nominal != None:
                    ratio = np.nan_to_num(sf / sf_nominal)
                    ratio_sf = Hist("SF ratio", axis_cat, *axes_electron)
                    ratio_sf.fill(cat='ratio_sf', **fields, weight=ratio.flatten())

                eff.fill(cat='data', **fields, weight=eff_data.flatten())
                eff.fill(cat='mc', **fields, weight=eff_mc.flatten())
                eff.fill(cat='sf', **fields, weight=sf.flatten())
                unc_eff.fill(cat='unc_data', **fields, weight=unc_eff_data.flatten())
                unc_eff.fill(cat='unc_mc', **fields, weight=unc_eff_mc.flatten())
                unc_eff.fill(cat='unc_sf', **fields, weight=unc_sf.flatten())
                unc_rel_eff.fill(cat='unc_rel_data', **fields, weight=unc_rel_eff_data.flatten())
                unc_rel_eff.fill(cat='unc_rel_mc', **fields, weight=unc_rel_eff_mc.flatten())
                unc_rel_eff.fill(cat='unc_rel_sf', **fields, weight=unc_rel_sf.flatten())
                if cat.endswith('pass'):
                    eff_nominal = eff
                    unc_eff_nominal = unc_eff
                    sf_nominal = sf

                if h.dense_dim() == 1:
                    cat_ht = cat.split('_')[-1]
                    if cat_ht == 'pass':
                        cat_ht = 'tot'
                    extra_args = {'totalLumi' : totalLumi, 'histname' : histname, 'year' : year, 'variable' : variable, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                    plot_variation(bincenter_x, eff['data'].sum('cat').values()[()], unc_eff['unc_data'].sum('cat').values()[()], 0.5*binwidth_x,
                                   axis_x.label, eff.label, 'HT', f'{cat_ht}', opts_data_splitHT[cat_ht], ax_eff, data=True, sf=False, **extra_args)

                    plot_variation(bincenter_x, eff['mc'].sum('cat').values()[()], unc_eff['unc_mc'].sum('cat').values()[()], 0.5*binwidth_x,
                                   axis_x.label, eff.label, 'HT', f'{cat_ht}', opts_mc_splitHT[cat_ht], ax_eff, data=False, sf=False, **extra_args)

                    for ax_sf in [ax_sf1, ax_sf2]:
                        plot_variation(bincenter_x, eff['sf'].sum('cat').values()[()], unc_eff['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x,
                                       axis_x.label, "Trigger SF", 'HT', f'{cat_ht}', opts_sf_splitHT[cat_ht], ax_sf, data=False, sf=True, **extra_args)
                    if cat.endswith('pass'):
                        continue
                    plot_ratio(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                               axis_x.label, 'var. / nom.', 'HT', f'{cat_ht}', opts_sf_splitHT[cat_ht], ax_sf_ratio, data=False, sf=True, **extra_args)
                    plot_residue(bincenter_x, eff['sf'].sum('cat').values()[()], eff_nominal['sf'].sum('cat').values()[()], unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x, axis_x.edges(),
                                 axis_x.label, '(var - nom.) / nom.', 'HT', f'{cat_ht}', opts_sf_splitHT[cat_ht], ax_sf_residue, data=False, sf=True, **extra_args)

                elif h.dense_dim() == 2:
                    if (syst != 'nominal') and (var == 'nominal'): continue
                    for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf", "ratio_sf"], [eff, eff, eff, unc_eff, unc_eff, unc_eff, unc_rel_eff, unc_rel_eff, unc_rel_eff, ratio_sf]):
                        if (var == 'nominal') and (label == "ratio_sf"): continue
                        fig_map, ax_map = plt.subplots(1,1,figsize=[16,10])
                        efficiency_map = histo[label].sum('cat')
                        if label == "sf":
                            efficiency_map.label = "Trigger SF"
                        elif label == "unc_sf":
                            efficiency_map.label = "Trigger SF unc."
                        elif label == "ratio_sf":
                            efficiency_map.label = "SF var./nom."

                        plot.plot2d(efficiency_map, ax=ax_map, xaxis=eff.axes()[-2], patch_opts=patch_opts[label])
                        hep.cms.text("Preliminary", loc=0, ax=ax_map)
                        hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax_map)
                        ax_map.set_title(var)
                        if varname_x == 'pt':
                            ax_map.set_xscale('log')
                        ax_map.set_xlim(*config.variables2d[variable2d][varname_x]['xlim'])
                        ax_map.set_ylim(*config.variables2d[variable2d][varname_y]['ylim'])
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
                        ax_map.xaxis.label.set_size(fontsize)   
                        ax_map.yaxis.label.set_size(fontsize)
                        ax_map.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                        ax_map.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                        #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                        plt.vlines(axis_x.edges(), axis_y.edges()[-1], axis_y.edges()[0], linestyle='--', color='gray')
                        plt.hlines(axis_y.edges(), axis_x.edges()[-1], axis_x.edges()[0], linestyle='--', color='gray')

                        for (i, x) in enumerate(bincenter_x):
                            if (x < ax_map.get_xlim()[0]) | (x > ax_map.get_xlim()[1]): continue
                            for (j, y) in enumerate(bincenter_y):
                                color = "w" if efficiency_map.values()[()][i][j] < patch_opts[label]['vmax'] else "k"
                                #color = "w"
                                if histname == "hist2d_electron_etaSC_vs_electron_pt":
                                    ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                            color=color, ha="center", va="center", fontsize=config.plot_options["fontsize_map"], fontweight="bold")
                                else:
                                    ax_map.text(x, y, f"{round(efficiency_map.values()[()][i][j], round_opts[label]['round'])}",
                                            color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")

                        filepath = os.path.join(plot_dir, f"{histname}_{year}_{label}_{cat}_{var}.png")

                        print("Saving", filepath)
                        plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                        if config.output_triggerSF:
                            if label in ["sf", "unc_sf"]:
                                # An extra column filled with ones is added to the efficiency map such that events with 0 electrons
                                # can be associated with a SF=1. To do that, the None values will have to be filled with -999
                                #col = [efficiency_map.values()[()][0]]
                                #eff_map = np.concatenate( (np.ones_like(col), efficiency_map.values()[()]) )
                                #axis_x_extended = np.concatenate( ([-1000], axis_x.edges()) )
                                #efflookup = dense_lookup(eff_map, [axis_x_extended, axis_y.edges()])
                                A = efficiency_map.to_hist()
                                eff_map = efficiency_map.values()[()]
                                ratio = np.where(eff_map != 0, eff_map, 1.)
                                sfhist = hist.Hist(A.axes[0], A.axes[1], data=ratio)
                                sfhist.label = "out"
                                if (syst == "nominal") and (var == "nominal") and (cat.endswith("pass")) and (label == "unc_sf"):
                                    #corr_dict.update({"unc_sf_stat" : efflookup})
                                    #efflookup_identity = dense_lookup(np.ones_like(eff_map), [axis_x_extended, axis_y.edges()])
                                    #corr_dict.update({"identity" : efflookup_identity})
                                    sfhist.name = f"{label}_stat"
                                elif label == "sf":    
                                    #efflookup = dense_lookup(efficiency_map.values()[()], [axis_x_extended, axis_y.edges()])
                                    #corr_dict.update({f"sf_{var}" : efflookup})
                                    sfhist.name = f"sf_{cat}"
                                else:
                                    continue
                                #print("******************************")
                                #print(sfhist.name)
                                #print(A.view())
                                clibcorr = correctionlib.convert.from_histogram(sfhist)
                                clibcorr.description = "SF to match the semileptonic trigger efficiency in MC and data"
                                corrections.append(clibcorr)

            if h.dense_dim() == 1:
                print("Saving", filepath_eff)
                fig_eff.savefig(filepath_eff, dpi=config.plot_options['dpi'], format="png")
                plt.close(fig_eff)
                print("Saving", filepath_sf)
                fig_sf1.savefig(filepath_sf, dpi=config.plot_options['dpi'], format="png")
                plt.close(fig_sf1)
                print("Saving", filepath_sf_residue)
                fig_sf2.savefig(filepath_sf_residue, dpi=config.plot_options['dpi'], format="png")
                plt.close(fig_sf2)

            if not os.path.exists(config.output_triggerSF):
                os.makedirs(config.output_triggerSF)
            local_folder = os.path.join(config.output, *config.output_triggerSF.split('/')[-2:])
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)
            map_name = histname.split("hist2d_")[-1]
            filename = f'sf_trigger_{map_name}_{year}_splitHT.json'
            if h.dense_dim() == 2:
                cset = correctionlib.schemav2.CorrectionSet(
                    schema_version=2,
                    description="Semileptonic trigger efficiency SF",
                    corrections=corrections,
                )
                rich.print(cset)
                for outdir in [config.output_triggerSF, local_folder]:
                    outfile_triggersf = os.path.join(outdir, filename)
                    outfile_triggersf = overwrite_check(outfile_triggersf)
                    print(f"Saving semileptonic trigger scale factors in {outfile_triggersf}")
                    with open(outfile_triggersf, "w") as fout:
                        fout.write(cset.json(exclude_unset=True))
                    #save(corr_dict, outfile_triggersf)
                    #efflookup_check = load(outfile_triggersf)
                    #sf_trigger = efflookup_check[list(efflookup_check.keys())[0]]
                    #print(sf_trigger(34, 1.0))
