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
    'pass' : {
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
    'pass' : {
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
    'pass' : {
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
    'pass' : {
        'linestyle' : 'solid',
        'color' : 'red'
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
    #print(unc_band)
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
            elif syst == 'era':
                key = var.split(syst)[-1]
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
            elif syst == 'era':
                key = var.split(syst)[-1]
            else:
                key = var.split(syst)[-1]
            errorbar_x.set_linestyle(opts_errorbar[key]['linestyle'])
            errorbar_y.set_linestyle(opts_errorbar[key]['linestyle'])
    ax.set_xlabel(xlabel, fontsize=kwargs['fontsize'])
    ax.set_ylabel(ylabel, fontsize=kwargs['fontsize'])
    ax.set_ylim(*ylim)

class EfficiencyMap:

    def __init__(self, config, histname, h, year, mode="standard") -> None:
        self.config = config
        self.mode = mode
        self.plot_dir = os.path.join(self.config.plots, "trigger_efficiency")
        self.fontsize = self.config.plot_options["fontsize"]
        plt.style.use([hep.style.ROOT, {'font.size': self.fontsize}])
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.histname = histname
        self.h = h
        self.year = year
        self.datasets = [str(s) for s in h.identifiers('sample')]
        self.datasets_data = list(filter(lambda x : 'DATA' in x, self.datasets))
        self.datasets_mc   = list(filter(lambda x : 'TTTo' in x, self.datasets))
        self.years = [str(s) for s in self.h.identifiers('year')]
        self.categories = [str(s) for s in self.h.identifiers('cat')]
        self.dim = h.dense_dim()
        if self.mode == "spliteras":
            self.eras = ['tot'] + [str(s) for s in h.identifiers('era') if str(s) != 'MC']

        if self.dim == 1:
            self.variable = self.histname.split('hist_')[1]
            if len(self.variable.split('_')) == 2:
                self.varname_x  = self.variable.split('_')[1]
            elif len(self.variable.split('_')) == 1:
                self.varname_x  = self.variable.split('_')[0]
            self.varnames = [self.varname_x]
            if self.variable in self.config.plot_options["rebin"].keys():
                self.h = self.h.rebin(self.varname_x, Bin(self.varname_x, self.h.axis(self.varname_x).label, **self.config.plot_options["rebin"][self.variable]["binning"]))
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
            self.hist_axis_x = None
            self.hist_axis_y = None
        else:
            raise NotImplementedError

    def define_systematics(self):
        # We extract all the variations saved in the histograms and the name of the systematics
        variations = [str(s) for s in self.h.identifiers('var')]
        self.systematics = ['nominal'] + [s.split("Up")[0] for s in variations if 'Up' in s]
        self.lumi_fractions = {}

    def initialize_stack(self):
        self.ratio_stack = []
        self.variations_labels = []
        self.corrections = {}

    def define_datamc(self, era=None):
        if (self.mode == "spliteras") and (self.dim == 2):
            self.totalLumi = femtobarn(lumi[self.year][era], digits=1)
        else:
            self.totalLumi = femtobarn(lumi[self.year]['tot'], digits=1)
        if (self.mode == "spliteras"):
            self.h_mc   = self.h[(self.datasets_mc, self.categories, self.year, 'MC', )].sum('sample', 'year', 'era')
            if era == 'tot':
                self.h_data = self.h[(self.datasets_data, self.categories, self.year, )].sum('sample', 'year', 'era')
            else:
                self.h_data = self.h[(self.datasets_data, self.categories, self.year, era)].sum('sample', 'year', 'era')
                self.lumi_frac = lumi[self.year][era] / lumi[self.year]['tot']
                self.lumi_fractions[era] = self.lumi_frac
                self.h_mc.scale(self.lumi_frac)
        else:
            self.h_data = self.h[(self.datasets_data, self.categories, self.year, )].sum('sample', 'year', 'era')
            self.h_mc   = self.h[(self.datasets_mc, self.categories, self.year, 'MC', )].sum('sample', 'year', 'era')
        self.axis_cat          = self.h_mc.axis('cat')
        self.axis_var          = self.h_mc.axis('var')
        self.axes_electron     = [self.h_mc.axis(varname) for varname in self.varnames]

    def define_1d_figures(self, cat, syst, save_plots=True):
        if self.dim == 1:
            if save_plots:
                self.fig_eff, self.ax_eff = plt.subplots(1,1,figsize=[10,10])
                self.fig_sf1, (self.ax_sf1, self.ax_sf_ratio)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                self.fig_sf2, (self.ax_sf2, self.ax_sf_residue)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                self.filepath_eff = os.path.join(self.plot_dir, f"{self.histname}_{self.year}_eff_datamc_{cat}_{syst}.png")
                self.filepath_sf = os.path.join(self.plot_dir, f"{self.histname}_{self.year}_sf_{cat}_{syst}.png")
                self.filepath_sf_residue = os.path.join(self.plot_dir, f"{self.histname}_{self.year}_sf_{cat}_{syst}_residue.png")
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"define_1d_figures: 1D figures cannot be defined for histogram with dimension {self.dim}")

    def define_variations(self, syst):
        self.eff_nominal = None
        self.unc_eff_nominal = None
        self.ratio_sf = None
        if syst in ["splitHT", "spliteras"]:
            pass
        else:
            self.variations = ['nominal', f'{syst}Down', f'{syst}Up'] if syst != 'nominal' else ['nominal']

    def compute_efficiency(self, cat, var, era=None):
        if cat.endswith(tuple(['pass_lowHT'])):
            cat_inclusive = [c for c in self.categories if c.endswith(tuple(['pass_lowHT', 'fail_lowHT']))]
        elif cat.endswith(tuple(['pass_highHT'])):
            cat_inclusive = [c for c in self.categories if c.endswith(tuple(['pass_highHT', 'fail_highHT']))]
        elif cat.endswith('pass'):
            cat_inclusive = 'inclusive'
        else:
            raise NotImplementedError
        num_data = self.h_data[cat,'nominal',:].sum('cat', 'var').values()[()]
        den_data = self.h_data[cat_inclusive,'nominal',:].sum('cat', 'var').values()[()]
        eff_data = np.nan_to_num( num_data / den_data )
        num_mc, sumw2_num_mc   = self.h_mc[cat,var,:].sum('cat', 'var').values(sumw2=True)[()]
        den_mc, sumw2_den_mc   = self.h_mc[cat_inclusive,var,:].sum('cat', 'var').values(sumw2=True)[()]
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
            #print(cat, var)
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
        if self.mode == "standard":
            if var == 'nominal':
                self.eff_nominal = self.eff
                self.unc_eff_nominal = self.unc_eff
        elif self.mode == "splitHT":
            if cat.endswith('pass'):
                self.eff_nominal = self.eff
                self.unc_eff_nominal = self.unc_eff
        elif self.mode == "spliteras":
            if era == "tot":
                self.eff_nominal = self.eff
                self.unc_eff_nominal = self.unc_eff

    def plot1d(self, cat, syst, var, save_plots=True, era=None):
        fontsize = self.config.plot_options["fontsize"]
        if self.mode == "standard":
            _opts_mc   = opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst]
            _opts_data = opts_data
            _opts_sf   = opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst]
        elif self.mode == "splitHT":
            _opts_data = opts_data_splitHT
            _opts_mc   = opts_mc_splitHT[var]
            _opts_sf   = opts_sf_splitHT[var]
        elif self.mode == "spliteras":
            _opts_data = opts_data_eras[era]
            _opts_sf   = opts_sf_eras[era]
        if self.dim == 1:
            if save_plots:
                extra_args = {'totalLumi' : self.totalLumi, 'histname' : self.histname, 'year' : self.year, 'variable' : self.variable, 'config' : self.config, 'cat' : cat, 'fontsize' : fontsize}
                plot_variation(self.bincenter_x, self.eff['data'].sum('cat').values()[()], self.unc_eff['unc_data'].sum('cat').values()[()], 0.5*self.binwidth_x,
                               self.axis_x.label, self.eff.label, syst, var, _opts_data, self.ax_eff, data=True, sf=False, **extra_args)
                if self.mode != "spliteras":
                    plot_variation(self.bincenter_x, self.eff['mc'].sum('cat').values()[()], self.unc_eff['unc_mc'].sum('cat').values()[()], 0.5*self.binwidth_x,
                                   self.axis_x.label, self.eff.label, syst, var, _opts_mc, self.ax_eff, data=False, sf=False, **extra_args)

                for ax_sf in [self.ax_sf1, self.ax_sf2]:
                    plot_variation(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.unc_eff['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x,
                                   self.axis_x.label, "Trigger SF", syst, var, _opts_sf, ax_sf, data=False, sf=True, **extra_args)
                plot_ratio(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.eff_nominal['sf'].sum('cat').values()[()], self.unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x, self.axis_x.edges(),
                           self.axis_x.label, 'var. / nom.', syst, var, _opts_sf, self.ax_sf_ratio, data=False, sf=True, **extra_args)
                plot_residue(self.bincenter_x, self.eff['sf'].sum('cat').values()[()], self.eff_nominal['sf'].sum('cat').values()[()], self.unc_eff_nominal['unc_sf'].sum('cat').values()[()], 0.5*self.binwidth_x, self.axis_x.edges(),
                             self.axis_x.label, '(var - nom.) / nom.', syst, var, _opts_sf, self.ax_sf_residue, data=False, sf=True, **extra_args)
            if self.config.output_triggerSF:
                #sf     = self.eff['sf'].sum('cat').values()[()].to_hist()
                A = self.eff['sf'].sum('cat').to_hist()
                self.hist_axis_x = A.axes[0]

                if (syst == "nominal") and (var == "nominal"):
                    if self.mode in ["splitHT", "spliteras"]: return
                    ratio = self.eff_nominal["sf"].sum('cat').values()[()]
                    ratio = np.where(ratio != 0, ratio, np.nan)
                    unc = np.array(self.unc_eff_nominal['unc_sf'].sum('cat').values()[()])
                    print("************************")
                    print(ratio)
                    print(unc)
                    ratios  = [ ratio, ratio - unc, ratio + unc ]
                    labels = [ "nominal", "statDown", "statUp" ]
                    #print(ratios[0])
                elif (syst != "nominal"):
                    eff_map = self.eff['sf'].sum('cat').values()[()]
                    ratios = [ np.where(eff_map != 0, eff_map, np.nan) ]
                    if self.mode == "standard":
                        labels = [ var ]
                    elif self.mode == "splitHT":
                        labels = [ cat ]
                    elif self.mode == "spliteras":
                        if era == 'tot':
                            labels = [ "tot" ]
                        else:
                            labels = [ era ]
                    else:
                        raise NotImplementedError
                else:
                    return
                self.ratio_stack += ratios
                self.variations_labels += labels
                if (self.mode == "splitHT") and (len(self.ratio_stack) == 3):
                    i_nominal = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass')][0])
                    i_lowHT = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass_lowHT')][0])
                    i_highHT = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass_highHT')][0])
                    ratio = self.ratio_stack[i_nominal]
                    unc = 0.5*abs(self.ratio_stack[i_highHT] - self.ratio_stack[i_lowHT])
                    ratios  = [ ratio - unc, ratio + unc ]
                    labels = [ "htDown", "htUp" ]
                    self.ratio_stack = ratios
                    self.variations_labels = labels
                elif (self.mode == "spliteras") and (len(self.ratio_stack) == len(self.eras)):
                    i_nominal = self.variations_labels.index('tot')
                    ratio = self.ratio_stack[i_nominal]
                    sf_eras = {}
                    lumiweights = {}
                    for i, era in enumerate(self.variations_labels):
                        if era == 'tot': continue
                        sf_eras[era]     = self.ratio_stack[i]
                        lumiweights[era] = self.lumi_fractions[era]*np.ones_like(self.ratio_stack[i])
                    print("*******************************")
                    print(self.lumi_fractions)
                    sf_lumiweighted = np.average(list(sf_eras.values()), axis=0, weights=list(lumiweights.values()))
                    unc = abs(sf_lumiweighted - ratio)
                    ratios  = [ ratio - unc, ratio + unc ]
                    labels = [ "eraDown", "eraUp" ]
                    self.ratio_stack = ratios
                    self.variations_labels = labels
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"plot1d: Histograms with dimension {self.dim} are not supported")

    def plot2d(self, cat, syst, var, save_plots=True, era=None):
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
                    hep.cms.lumitext(text=f'{self.totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {self.year}', fontsize=18, ax=ax_map)
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
                self.save2d(cat, syst, var, label, save_plots, era)
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"plot2d: Histograms with dimension {self.dim} are not supported")

    def save1d(self, save_plots):
        if self.dim == 1:
            if save_plots:
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
            sys.exit(f"save1d: Histograms with dimension {self.dim} are not supported")

    def save2d(self, cat, syst, var, label, save_plots, era=None):
        if self.dim == 2:
            if (syst != 'nominal') and (var == 'nominal'): return
            if (var == 'nominal') and (label == "ratio_sf"): return
            if self.mode in ["standard", "splitHT"]:
                filepath = os.path.join(self.plot_dir, f"{self.histname}_{self.year}_{label}_{cat}_{var}.png")
            elif self.mode == "spliteras":
                filepath = os.path.join(self.plot_dir, f"{self.histname}_{self.year}_{label}_{cat}_{self.year}{era}.png")

            if save_plots:
                print("Saving", filepath)
                plt.savefig(filepath, dpi=self.config.plot_options['dpi'], format="png")
            if self.config.output_triggerSF:
                if label in ["sf", "unc_sf"]:
                    A = self.map2d.to_hist()
                    self.hist_axis_x = A.axes[0]
                    self.hist_axis_y = A.axes[1]
                    eff_map = self.map2d.values()[()]
                    if (syst == "nominal") and (var == "nominal") and (label == "unc_sf"):
                        if self.mode in ["splitHT", "spliteras"]: return
                        ratio = self.eff_nominal["sf"].sum('cat').values()[()]
                        ratio = np.where(ratio != 0, ratio, np.nan)
                        unc = np.array(eff_map)
                        ratios  = [ ratio - unc, ratio + unc ]
                        labels = [ "statDown", "statUp" ]
                        #print(ratios[0])
                    elif label == "sf":
                        ratios = [ np.where(eff_map != 0, eff_map, np.nan) ]
                        if self.mode == "standard":
                            labels = [ var ]
                        elif self.mode == "splitHT":
                            labels = [ cat ]
                        elif self.mode == "spliteras":
                            if era == 'tot':
                                labels = [ "tot" ]
                            else:
                                labels = [ era ]
                        else:
                            raise NotImplementedError
                    else:
                        return
                    self.ratio_stack += ratios
                    self.variations_labels += labels
                    if (self.mode == "splitHT") and (len(self.ratio_stack) == 3):
                        i_nominal = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass')][0])
                        i_lowHT = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass_lowHT')][0])
                        i_highHT = self.variations_labels.index([c for c in self.variations_labels if c.endswith('pass_highHT')][0])
                        ratio = self.ratio_stack[i_nominal]
                        unc = 0.5*abs(self.ratio_stack[i_highHT] - self.ratio_stack[i_lowHT])
                        ratios  = [ ratio - unc, ratio + unc ]
                        labels = [ "htDown", "htUp" ]
                        self.ratio_stack = ratios
                        self.variations_labels = labels
                    elif (self.mode == "spliteras") and (len(self.ratio_stack) == len(self.eras)):
                        i_nominal = self.variations_labels.index('tot')
                        ratio = self.ratio_stack[i_nominal]
                        sf_eras = {}
                        lumiweights = {}
                        for i, era in enumerate(self.variations_labels):
                            if era == 'tot': continue
                            sf_eras[era]     = self.ratio_stack[i]
                            lumiweights[era] = self.lumi_fractions[era]*np.ones_like(self.ratio_stack[i])
                        sf_lumiweighted = np.average(list(sf_eras.values()), axis=0, weights=list(lumiweights.values()))
                        unc = abs(sf_lumiweighted - ratio)
                        ratios  = [ ratio - unc, ratio + unc ]
                        labels = [ "eraDown", "eraUp" ]
                        self.ratio_stack = ratios
                        self.variations_labels = labels
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"save2d: Histograms with dimension {self.dim} are not supported")

    def save_corrections(self):
        if self.dim == 2:
            assert len(self.ratio_stack) == len(self.variations_labels), "'ratio_stack' and 'variations_labels' have different length"
            for label, ratio in zip(self.variations_labels, self.ratio_stack):
                self.corrections[label] = {'ratio_stack' : ratio, 'year' : self.year, 'hist_axis_x' : self.hist_axis_x, 'hist_axis_y' : self.hist_axis_y}
        elif self.dim == 1:
            assert len(self.ratio_stack) == len(self.variations_labels), "'ratio_stack' and 'variations_labels' have different length"
            for label, ratio in zip(self.variations_labels, self.ratio_stack):
                self.corrections[label] = {'ratio_stack' : ratio, 'year' : self.year, 'hist_axis_x' : self.hist_axis_x}

def plot_efficiency_maps(accumulator, config, save_plots=False):

    corrections = {}
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue
        print("Histogram:", histname)
        corrections[histname] = {}

        for year in config.dataset["filter"]["year"]:
            efficiency_map = EfficiencyMap(config, histname, h, year, mode="standard")
            efficiency_map.define_systematics()
            efficiency_map.define_datamc()
            categories = [c for c in efficiency_map.categories if c.endswith(tuple(['pass']))]

            for cat in categories:
                efficiency_map.initialize_stack()

                for syst in efficiency_map.systematics:
                    efficiency_map.define_1d_figures(cat, syst, save_plots=save_plots)
                    efficiency_map.define_variations(syst)

                    for var in efficiency_map.variations:
                        efficiency_map.compute_efficiency(cat, var)
                        efficiency_map.plot1d(cat, syst, var, save_plots=save_plots)
                        efficiency_map.plot2d(cat, syst, var, save_plots=save_plots)

                    efficiency_map.save1d(save_plots=save_plots)

                efficiency_map.save_corrections()
                corrections[histname][cat] = efficiency_map.corrections
                #corrections[histname][cat].update({'year' : year, 'hist_axis_x' : efficiency_map.hist_axis_x, 'hist_axis_y' : efficiency_map.hist_axis_y})
    return corrections

def plot_efficiency_maps_splitHT(accumulator, config, save_plots=False):

    corrections = {}
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue
        print("Histogram:", histname)
        corrections[histname] = {}

        for year in config.dataset["filter"]["year"]:
            efficiency_map = EfficiencyMap(config, histname, h, year, mode="splitHT")
            efficiency_map.define_systematics()
            efficiency_map.define_datamc()
            categories = [c for c in efficiency_map.categories if c.endswith(tuple(['pass', 'pass_lowHT', 'pass_highHT']))]
            efficiency_map.define_1d_figures(categories[0], "splitHT", save_plots=save_plots)
            efficiency_map.define_variations("splitHT")
            efficiency_map.initialize_stack()

            for cat in categories:
                efficiency_map.compute_efficiency(cat, "nominal")
                efficiency_map.plot1d(cat, "HT", cat.split('_')[-1], save_plots=save_plots)
                efficiency_map.plot2d(cat, "nominal", "nominal", save_plots=save_plots)

            efficiency_map.save1d(save_plots=save_plots)
            efficiency_map.save_corrections()
            corrections[histname][[c for c in categories if c.endswith('pass')][0]] = efficiency_map.corrections
    return corrections

def plot_efficiency_maps_spliteras(accumulator, config, save_plots=False):

    corrections = {}
    for (histname, h) in accumulator.items():
        if config.plot_options["only"] and not (config.plot_options["only"] in histname): continue
        if not histname.startswith('hist'): continue
        print("Histogram:", histname)
        corrections[histname] = {}

        for year in config.dataset["filter"]["year"]:
            efficiency_map = EfficiencyMap(config, histname, h, year, mode="spliteras")
            categories = [c for c in efficiency_map.categories if c.endswith(tuple(['pass']))]

            for cat in categories:
                efficiency_map.define_systematics()
                efficiency_map.define_1d_figures(cat, "spliteras", save_plots=save_plots)
                efficiency_map.define_variations("spliteras")
                efficiency_map.initialize_stack()

                for era in efficiency_map.eras:
                    efficiency_map.define_datamc(era)
                    efficiency_map.compute_efficiency(cat, "nominal", era=era)
                    efficiency_map.plot1d(cat, "era", f"era{era}", save_plots=save_plots, era=era)
                    efficiency_map.plot2d(cat, "nominal", "nominal", save_plots=save_plots, era=era)

                efficiency_map.save1d(save_plots=save_plots)
                efficiency_map.save_corrections()
                corrections[histname][cat] = efficiency_map.corrections
    return corrections
