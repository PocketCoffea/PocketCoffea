import os
import sys

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

import mplhep as hep
import hist
from hist import Hist

import correctionlib
import correctionlib.convert

from pocket_coffea.parameters.lumi import lumi, femtobarn

# color_datamc = {'data' : 'black', 'mc' : 'red'}
opts_data = {
    'linestyle': 'solid',
    'linewidth': 0,
    'marker': '.',
    'markersize': 1.0,
    'color': 'black',
    'elinewidth': 1,
    'label': 'Data',
}

opts_data_eras = {
    'tot': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': 'tot',
    },
    'A': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': 'A',
    },
    'B': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': 'B',
    },
    'C': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'orange',
        'elinewidth': 1,
        'label': 'C',
    },
    'D': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'purple',
        'elinewidth': 1,
        'label': 'D',
    },
    'E': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'yellow',
        'elinewidth': 1,
        'label': 'E',
    },
    'F': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': 'F',
    },
    'G': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': 'G',
    },
    'H': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'orange',
        'elinewidth': 1,
        'label': 'H',
    },
}

opts_data_splitHT = {
    'pass': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'black',
        'elinewidth': 1,
        'label': 'tot',
    },
    'lowHT': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': r'$H_T$ < 400 GeV',
    },
    'highHT': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': r'$H_T$ > 400 GeV',
    },
}

opts_mc = {
    'nominal': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC',
    },
    'Up': {
        'linestyle': 'dashed',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC',
    },
    'Down': {
        'linestyle': 'dotted',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC',
    },
}

opts_mc_splitHT = {
    'pass': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'black',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC',
    },
    'highHT': {
        'linestyle': 'dashed',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC' + '\n' + r'$H_T$ < 400 GeV',
    },
    'lowHT': {
        'linestyle': 'dotted',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': r'$t\bar{t}$ DL + SL MC' + '\n' + r'$H_T$ > 400 GeV',
    },
}

opts_sf = {
    'nominal': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': 'SF',
    },
    'Up': {
        'linestyle': 'dashed',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': 'SF',
    },
    'Down': {
        'linestyle': 'dotted',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': 'SF',
    },
}

opts_sf_eras = {
    'tot': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
        'label': 'tot',
    },
    'A': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': 'A',
    },
    'B': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': 'B',
    },
    'C': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'orange',
        'elinewidth': 1,
        'label': 'C',
    },
    'D': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'purple',
        'elinewidth': 1,
        'label': 'D',
    },
    'E': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'yellow',
        'elinewidth': 1,
        'label': 'E',
    },
    'F': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': 'F',
    },
    'G': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': 'G',
    },
    'H': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'orange',
        'elinewidth': 1,
        'label': 'H',
    },
}

opts_sf_splitHT = {
    'pass': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'black',
        'elinewidth': 1,
        'label': 'tot',
    },
    'lowHT': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'green',
        'elinewidth': 1,
        'label': r'$H_T$ < 400 GeV',
    },
    'highHT': {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 1.0,
        'color': 'blue',
        'elinewidth': 1,
        'label': r'$H_T$ > 400 GeV',
    },
}

opts_errorbar = {
    'nominal': {'linestyle': 'solid'},
    'Up': {'linestyle': 'dashed'},
    'Down': {'linestyle': 'dotted'},
    'tot': {'linestyle': 'solid', 'color': 'red'},
    'A': {'linestyle': 'solid', 'color': 'green'},
    'B': {'linestyle': 'solid', 'color': 'blue'},
    'C': {'linestyle': 'solid', 'color': 'orange'},
    'D': {'linestyle': 'solid', 'color': 'purple'},
    'E': {'linestyle': 'solid', 'color': 'yellow'},
    'F': {'linestyle': 'solid', 'color': 'green'},
    'G': {'linestyle': 'solid', 'color': 'blue'},
    'H': {'linestyle': 'solid', 'color': 'orange'},
    'pass': {'linestyle': 'solid', 'color': 'red'},
    'highHT': {'linestyle': 'dashed'},
    'lowHT': {'linestyle': 'dotted'},
}

hatch_density = 4
opts_unc = {
    "step": "post",
    "color": (0, 0, 0, 0.4),
    "facecolor": (0, 0, 0, 0.0),
    "linewidth": 0,
    "hatch": '/' * hatch_density,
    "zorder": 2,
}

opts_gap = {
    "step": "post",
    # "color": (0, 0, 0, 0.4),
    "color": "white",
    "facecolor": "white",
    # "linewidth": 0,
    "hatch": '/' * 100,
    "zorder": 2,
}

round_opts = {
    'data': {'round': 2},
    'mc': {'round': 2},
    'sf': {'round': 2},
    'unc_data': {'round': 2},
    'unc_mc': {'round': 2},
    'unc_sf': {'round': 2},
    'unc_rel_data': {'round': 2},
    'unc_rel_mc': {'round': 2},
    'unc_rel_sf': {'round': 2},
    'ratio_sf': {'round': 3},
}

def stack_sum(stack):
    '''Returns the sum histogram of a stack (`hist.stack.Stack`) of histograms.'''
    if len(stack) == 1:
        return stack[0]
    else:
        htot =hist.Hist(stack[0])
        for h in stack[1:]:
            htot = htot + h
        return htot

def uncertainty_efficiency(eff, den, sumw2_num=None, sumw2_den=None, mc=False):
    '''Returns the uncertainty on an efficiency `eff=num/den` given the efficiency `eff`, the denominator `den`.
    For MC efficiency also the sum of the squared weights of numerator and denominator (`sumw2_num`, `sumw2_den`) have to be passed as argument and the flag `mc` has to be set to `True`.'''
    if mc:
        if (sumw2_num is None) | (sumw2_den is None):
            sys.exit("Missing sum of squared weights.")
        return np.sqrt(
            ((1 - 2 * eff) / den**2) * sumw2_num + (eff**2 / den**2) * sumw2_den
        )
    else:
        if (sumw2_num is not None) | (sumw2_den is not None):
            sys.exit(
                "Sum of squared weights was passed but the type 'mc' was not set to True."
            )
        return np.sqrt(eff * (1 - eff) / den)


def uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc):
    '''Returns the uncertainty on a scale factor given the data and MC efficiency (`eff_data`, `eff_mc`) and the corresponding uncertainties (`unc_eff_data`, `unc_eff_mc`).'''
    return (eff_data / eff_mc) * np.sqrt(
        (unc_eff_data / eff_data) ** 2 + (unc_eff_mc / eff_mc) ** 2
    )


def plot_variation(
    x,
    y,
    yerr,
    xerr,
    xlabel,
    ylabel,
    syst,
    var,
    opts,
    ax,
    data=False,
    sf=False,
    **kwargs,
):
    '''Function to plot a variation of an efficiency or scale factor on an axis `ax`.
    To plot the data efficiency variation, the flag `data` has to be set to `True`. To plot the scale factor variation, the flag `sf` has to be set to `True`.'''
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    hep.cms.text("Preliminary", loc=0, ax=ax)
    hep.cms.lumitext(
        text=f'{kwargs["totalLumi"]}' + r' fb$^{-1}$, 13 TeV,' + f' {kwargs["year"]}',
        fontsize=18,
        ax=ax,
    )
    # plot.plot1d(eff[['mc','data']], ax=ax_eff, error_opts=opts_datamc);
    if data & (not sf) & (var == 'nominal'):
        if not 'era' in var:
            ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts, fmt='none')
            return
    elif data & (not ((var == 'nominal') | ('era' in var))):
        return
    lines = ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts, fmt='none')
    handles, labels = ax.get_legend_handles_labels()
    if (
        kwargs['histname']
        in config['parameters_sf' if sf else 'parameters_eff'][kwargs['year']][
            kwargs['cat']
        ].keys()
    ):
        ylim = config['parameters_sf' if sf else 'parameters_eff'][
            kwargs['year']
        ][kwargs['cat']][kwargs['histname']]['ylim']
    else:
        ylim = (0.7, 1.3) if sf else (0, 1)

    if (syst != 'nominal') & (syst not in ['era', 'HT']):
        handles[-1].set_label(labels[-1] + f' {var}')
        if var != 'nominal':
            errorbar_x = lines[-1][0]
            errorbar_y = lines[-1][1]
            errorbar_x.set_linestyle(opts_errorbar[var.split(syst)[-1]]['linestyle'])
            errorbar_y.set_linewidth(0)
    ax.legend()
    ax.set_xlabel(xlabel, fontsize=kwargs['fontsize'])
    ax.set_ylabel(ylabel, fontsize=kwargs['fontsize'])
    ax.set_xlim(*kwargs['xlim'])
    ax.set_ylim(*ylim)


def plot_ratio(
    x,
    y,
    ynom,
    yerrnom,
    xerr,
    edges,
    xlabel,
    ylabel,
    syst,
    var,
    opts,
    ax,
    data=False,
    sf=False,
    **kwargs,
):
    '''Function to plot the uncertainty band corresponding to the variation of an efficiency or scale factor in the ratio plot on an axis `ax`.
    To plot the data efficiency variation, the flag `data` has to be set to `True`. To plot the scale factor variation, the flag `sf` has to be set to `True`.'''
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    if not sf:
        return
    if var == 'nominal':
        return

    ratio = y / ynom
    unc_ratio = (yerrnom / ynom) * ratio
    lines = ax.errorbar(x, ratio, yerr=0, xerr=xerr, **opts, fmt='none')
    lo = 1 - unc_ratio
    hi = 1 + unc_ratio
    unc_band = np.nan_to_num(np.array([lo, hi]), nan=1)
    # print(unc_band)
    ax.fill_between(
        edges,
        np.r_[unc_band[0], unc_band[0, -1]],
        np.r_[unc_band[1], unc_band[1, -1]],
        **opts_unc,
    )
    if (
        kwargs['histname']
        in config['parameters_ratio'][kwargs['year']][kwargs['cat']].keys()
    ):
        ylim = config['parameters_ratio'][kwargs['year']][kwargs['cat']][
            kwargs['histname']
        ]['ylim']
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


def plot_residue(
    x,
    y,
    ynom,
    yerrnom,
    xerr,
    edges,
    xlabel,
    ylabel,
    syst,
    var,
    opts,
    ax,
    data=False,
    sf=False,
    **kwargs,
):
    '''Function to plot the uncertainty band corresponding to the variation of an efficiency or scale factor in the residue plot on an axis `ax`.
    To plot the data efficiency variation, the flag `data` has to be set to `True`. To plot the scale factor variation, the flag `sf` has to be set to `True`.'''
    config = kwargs['config']
    if data & sf:
        sys.exit("'data' and 'sf' cannot be True at the same time.")
    if not sf:
        return
    if var == 'nominal':
        return

    residue = (y - ynom) / ynom
    ratio = y / ynom
    unc_residue = (yerrnom / ynom) * ratio
    lines = ax.errorbar(x, residue, yerr=0, xerr=xerr, **opts, fmt='none')
    lo = -unc_residue
    hi = unc_residue
    unc_band = np.nan_to_num(np.array([lo, hi]), nan=1)
    ax.fill_between(
        edges,
        np.r_[unc_band[0], unc_band[0, -1]],
        np.r_[unc_band[1], unc_band[1, -1]],
        **opts_unc,
    )
    if (
        kwargs['histname']
        in config['parameters_ratio'][kwargs['year']][kwargs['cat']].keys()
    ):
        ylim = config['parameters_residue'][kwargs['year']][kwargs['cat']][
            kwargs['histname']
        ]['ylim']
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
    def __init__(self, shape, config, year, outputdir, mode="standard") -> None:
        '''h is a dictionary of `hist` histograms, one for each sample.'''
        self.config = config
        self.year = year
        self.mode = mode
        self.plot_dir = outputdir
        self.fontsize = self.config["fontsize"]
        plt.style.use([hep.style.ROOT, {'font.size': self.fontsize}])
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)
        self.histname = shape.name
        self.h = shape.h_dict
        self.datasets = self.h.keys()
        self.datasets_data = list(filter(lambda x: 'DATA' in x, self.datasets))
        self.datasets_mc = list(filter(lambda x: 'DATA' not in x, self.datasets))
        self.axis_cat = self.h[self.datasets_data[0]].axes['cat']
        self.axis_var = self.h[self.datasets_mc[0]].axes['variation']
        self.categories = list(self.axis_cat.value(range(self.axis_cat.size)))
        self.variations = list(self.axis_var.value(range(self.axis_var.size)))
        self.dim = shape.dense_dim
        self.dense_axes = shape.dense_axes
        if self.mode == "spliteras":
            self.axis_era = self.h[self.datasets_data[0]].axes['era']
            self.eras = ['tot'] + list(self.axis_era.value(range(self.axis_era.size)))

        if self.dim == 1:
            self.variable = self.dense_axes[0].name
            self.varname_x = self.variable
            self.varnames = [self.varname_x]
            self.axis_x = self.dense_axes[-1]
            self.xlim = (self.axis_x.edges[0], self.axis_x.edges[-1])
            self.binwidth_x = np.ediff1d(self.axis_x.edges)
            self.bincenter_x = self.axis_x.edges[:-1] + 0.5 * self.binwidth_x
            self.fields = {self.varname_x: self.bincenter_x}
        elif self.dim == 2:
            self.varname_x = self.dense_axes[0].name
            self.varname_y = self.dense_axes[1].name
            self.varnames = [self.varname_x, self.varname_y]
            self.axis_x = self.dense_axes[0]
            self.axis_y = self.dense_axes[1]
            self.xlim = (self.axis_x.edges[0], self.axis_x.edges[-1])
            self.ylim = (self.axis_y.edges[0], self.axis_y.edges[-1])
            self.binwidth_x = np.ediff1d(self.axis_x.edges)
            self.binwidth_y = np.ediff1d(self.axis_y.edges)
            self.bincenter_x = self.axis_x.edges[:-1] + 0.5 * self.binwidth_x
            self.bincenter_y = self.axis_y.edges[:-1] + 0.5 * self.binwidth_y
            self.y, self.x = np.meshgrid(self.bincenter_y, self.bincenter_x)
            self.fields = {
                self.varname_x: self.x.flatten(),
                self.varname_y: self.y.flatten(),
            }
            self.hist_axis_x = None
            self.hist_axis_y = None
        else:
            raise NotImplementedError

        dict_data = {d: self.h[d] for d in self.datasets_data}
        dict_mc = {d: self.h[d] for d in self.datasets_mc}
        self.h_data = stack_sum(hist.Stack.from_dict(dict_data))
        self.h_mc = stack_sum(hist.Stack.from_dict(dict_mc))

    def define_systematics(self):
        '''Define the list of systematics, given the variations.'''
        # We extract all the variations saved in the histograms and the name of the systematics
        self.systematics = ['nominal'] + [
            s.split("Up")[0] for s in self.variations if 'Up' in s
        ]
        self.lumi_fractions = {}

    def initialize_stack(self):
        '''Initialize the lists and dictionaries to save the scale factor corrections in a stack.'''
        self.ratio_stack = []
        self.variations_labels = []
        self.corrections = {}

    def define_datamc(self, cat, var, era=None):
        '''Define the data and MC dictionaries used for slicing the histograms `self.h_data` and `self.h_mc`, for .'''
        if (self.mode == "spliteras") and (self.dim == 2):
            self.totalLumi = femtobarn(lumi[self.year][era], digits=1)
        else:
            self.totalLumi = femtobarn(lumi[self.year]['tot'], digits=1)
        if self.mode == "spliteras":
            self.slicing_mc = {'cat': cat, 'variation': var}
            if era == 'tot':
                self.slicing_data = {'cat': cat}
            else:
                self.slicing_data = {'cat': cat, 'era': era}
                self.lumi_frac = lumi[self.year][era] / lumi[self.year]['tot']
                self.lumi_fractions[era] = self.lumi_frac
                self.h_mc = self.lumi_frac * self.h_mc
        else:
            self.slicing_data = {'cat': cat}
            self.slicing_mc = {'cat': cat, 'variation': var}

    def define_1d_figures(self, cat, syst, save_plots=True):
        '''Define the figures of the 1D histogram plots.'''
        if self.dim == 1:
            if save_plots:
                self.fig_eff, self.ax_eff = plt.subplots(1, 1, figsize=[10, 10])
                self.fig_sf1, (self.ax_sf1, self.ax_sf_ratio) = plt.subplots(
                    2,
                    1,
                    figsize=[10, 10],
                    gridspec_kw={"height_ratios": (3, 1)},
                    sharex=True,
                )
                self.fig_sf2, (self.ax_sf2, self.ax_sf_residue) = plt.subplots(
                    2,
                    1,
                    figsize=[10, 10],
                    gridspec_kw={"height_ratios": (3, 1)},
                    sharex=True,
                )
                self.filepath_eff = os.path.join(
                    self.plot_dir,
                    f"{self.histname}_eff_datamc_{cat}_{syst}.png",
                )
                self.filepath_sf = os.path.join(
                    self.plot_dir, f"{self.histname}_sf_{cat}_{syst}.png"
                )
                self.filepath_sf_residue = os.path.join(
                    self.plot_dir,
                    f"{self.histname}_sf_{cat}_{syst}_residue.png",
                )
        elif self.dim == 2:
            pass
        else:
            sys.exit(
                f"define_1d_figures: 1D figures cannot be defined for histogram with dimension {self.dim}"
            )

    def define_variations(self, syst):
        '''Define the variations, given a systematic uncertainty `syst`.'''
        self.eff_nominal = None
        self.unc_eff_nominal = None
        self.ratio_sf = None
        if syst in ["splitHT", "spliteras"]:
            pass
        else:
            self.variations = (
                ['nominal', f'{syst}Down', f'{syst}Up']
                if syst != 'nominal'
                else ['nominal']
            )

    def compute_efficiency(self, cat, var, era=None):
        '''Compute the data and MC efficiency, the scale factor and the corresponding uncertainties for a given category `cat` and a variation `var`.
        If the computation has to be performed for a specific data-taking era, also the argument `era` has to be specified.'''
        if cat.endswith(tuple(['pass_lowHT'])):
            cat_inclusive = [
                c
                for c in self.categories
                if c.endswith(tuple(['pass_lowHT', 'fail_lowHT']))
            ]
        elif cat.endswith(tuple(['pass_highHT'])):
            cat_inclusive = [
                c
                for c in self.categories
                if c.endswith(tuple(['pass_highHT', 'fail_highHT']))
            ]
        elif cat.endswith('pass'):
            cat_inclusive = 'inclusive'
        else:
            raise NotImplementedError
        self.slicing_data_inclusive = {
            k: value for k, value in self.slicing_data.items()
        }
        self.slicing_data_inclusive['cat'] = cat_inclusive
        self.slicing_mc_inclusive = {k: value for k, value in self.slicing_mc.items()}
        self.slicing_mc_inclusive['cat'] = cat_inclusive

        num_data = self.h_data[self.slicing_data].project(*self.varnames).values()
        den_data = (
            self.h_data[self.slicing_data_inclusive].project(*self.varnames).values()
        )
        eff_data = np.nan_to_num(num_data / den_data)
        num_mc = self.h_mc[self.slicing_mc].project(*self.varnames).values()
        sumw2_num_mc = self.h_mc[self.slicing_mc].project(*self.varnames).variances()
        den_mc = self.h_mc[self.slicing_mc_inclusive].project(*self.varnames).values()
        sumw2_den_mc = (
            self.h_mc[self.slicing_mc_inclusive].project(*self.varnames).variances()
        )
        eff_mc = np.nan_to_num(num_mc / den_mc)
        sf = np.nan_to_num(eff_data / eff_mc)
        sf = np.where(sf < 100, sf, 100)
        if var == 'nominal':
            self.sf_nominal = sf
        unc_eff_data = uncertainty_efficiency(eff_data, den_data)
        unc_eff_mc = uncertainty_efficiency(
            eff_mc, den_mc, sumw2_num_mc, sumw2_den_mc, mc=True
        )
        unc_sf = uncertainty_sf(eff_data, eff_mc, unc_eff_data, unc_eff_mc)
        unc_rel_eff_data = np.nan_to_num(unc_eff_data / eff_data)
        unc_rel_eff_mc = np.nan_to_num(unc_eff_mc / eff_mc)
        unc_rel_sf = np.nan_to_num(unc_sf / sf)
        axis_map_eff = hist.axis.StrCategory(["data", "mc", "sf"], name="map")
        axis_map_unc_eff = hist.axis.StrCategory(
            ["unc_data", "unc_mc", "unc_sf"], name="map"
        )
        axis_map_unc_rel_eff = hist.axis.StrCategory(
            ["unc_rel_data", "unc_rel_mc", "unc_rel_sf"], name="map"
        )
        axis_map_ratio_sf = hist.axis.StrCategory(["ratio_sf"], name="map")
        self.eff = Hist(axis_map_eff, *self.dense_axes)
        self.unc_eff = Hist(axis_map_unc_eff, *self.dense_axes)
        self.unc_rel_eff = Hist(axis_map_unc_rel_eff, *self.dense_axes)

        if self.eff_nominal != None:
            self.ratio = np.nan_to_num(sf / self.sf_nominal)
            self.ratio_sf = Hist(axis_map_ratio_sf, *self.dense_axes)
            self.ratio_sf.fill(
                map='ratio_sf', **self.fields, weight=self.ratio.flatten()
            )

        self.eff.fill(map='data', **self.fields, weight=eff_data.flatten())
        self.eff.fill(map='mc', **self.fields, weight=eff_mc.flatten())
        self.eff.fill(map='sf', **self.fields, weight=sf.flatten())
        self.unc_eff.fill(map='unc_data', **self.fields, weight=unc_eff_data.flatten())
        self.unc_eff.fill(map='unc_mc', **self.fields, weight=unc_eff_mc.flatten())
        self.unc_eff.fill(map='unc_sf', **self.fields, weight=unc_sf.flatten())
        self.unc_rel_eff.fill(
            map='unc_rel_data', **self.fields, weight=unc_rel_eff_data.flatten()
        )
        self.unc_rel_eff.fill(
            map='unc_rel_mc', **self.fields, weight=unc_rel_eff_mc.flatten()
        )
        self.unc_rel_eff.fill(
            map='unc_rel_sf', **self.fields, weight=unc_rel_sf.flatten()
        )
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
        '''Function to plot a 1D efficiency or scale factor for a given category `cat` and a variation `var`.
        To save the output plots, the flag `save_plots` has to be set to `True`.
        If the computation has to be performed for a specific data-taking era, also the argument `era` has to be specified.'''
        fontsize = self.config["fontsize"]
        if self.mode == "standard":
            _opts_mc = opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst]
            _opts_data = opts_data
            _opts_sf = opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst]
        elif self.mode == "splitHT":
            _opts_data = opts_data_splitHT
            _opts_mc = opts_mc_splitHT[var]
            _opts_sf = opts_sf_splitHT[var]
        elif self.mode == "spliteras":
            _opts_data = opts_data_eras[era]
            _opts_sf = opts_sf_eras[era]
        if self.dim == 1:
            if save_plots:
                extra_args = {
                    'totalLumi': self.totalLumi,
                    'histname': self.histname,
                    'year': self.year,
                    'variable': self.variable,
                    'config': self.config,
                    'cat': cat,
                    'fontsize': fontsize,
                    'xlim': self.xlim
                }
                plot_variation(
                    self.bincenter_x,
                    self.eff[{'map': 'data'}].values(),
                    self.unc_eff[{'map': 'unc_data'}].values(),
                    0.5 * self.binwidth_x,
                    self.axis_x.label,
                    self.eff.label,
                    syst,
                    var,
                    _opts_data,
                    self.ax_eff,
                    data=True,
                    sf=False,
                    **extra_args,
                )
                if self.mode != "spliteras":
                    plot_variation(
                        self.bincenter_x,
                        self.eff[{'map': 'mc'}].values(),
                        self.unc_eff[{'map': 'unc_mc'}].values(),
                        0.5 * self.binwidth_x,
                        self.axis_x.label,
                        self.eff.label,
                        syst,
                        var,
                        _opts_mc,
                        self.ax_eff,
                        data=False,
                        sf=False,
                        **extra_args,
                    )

                for ax_sf in [self.ax_sf1, self.ax_sf2]:
                    plot_variation(
                        self.bincenter_x,
                        self.eff[{'map': 'sf'}].values(),
                        self.unc_eff[{'map': 'unc_sf'}].values(),
                        0.5 * self.binwidth_x,
                        self.axis_x.label,
                        "Trigger SF",
                        syst,
                        var,
                        _opts_sf,
                        ax_sf,
                        data=False,
                        sf=True,
                        **extra_args,
                    )
                plot_ratio(
                    self.bincenter_x,
                    self.eff[{'map': 'sf'}].values(),
                    self.eff_nominal[{'map': 'sf'}].values(),
                    self.unc_eff_nominal[{'map': 'unc_sf'}].values(),
                    0.5 * self.binwidth_x,
                    self.axis_x.edges,
                    self.axis_x.label,
                    'var. / nom.',
                    syst,
                    var,
                    _opts_sf,
                    self.ax_sf_ratio,
                    data=False,
                    sf=True,
                    **extra_args,
                )
                plot_residue(
                    self.bincenter_x,
                    self.eff[{'map': 'sf'}].values(),
                    self.eff_nominal[{'map': 'sf'}].values(),
                    self.unc_eff_nominal[{'map': 'unc_sf'}].values(),
                    0.5 * self.binwidth_x,
                    self.axis_x.edges,
                    self.axis_x.label,
                    '(var - nom.) / nom.',
                    syst,
                    var,
                    _opts_sf,
                    self.ax_sf_residue,
                    data=False,
                    sf=True,
                    **extra_args,
                )
            A = self.eff[{'map': 'sf'}]
            self.hist_axis_x = A.axes[0]

            if (syst == "nominal") and (var == "nominal"):
                if self.mode in ["splitHT", "spliteras"]:
                    return
                ratio = self.eff_nominal[{'map': 'sf'}].values()
                # In the bins in which the SF is 0, which are those where the trigger efficiency on data is 0,
                # the SF is set to 1 so that MC is not rescaled in that bin.
                # ratio = np.where(ratio != 0, ratio, 1.0)
                unc = np.array(self.unc_eff_nominal[{'map': 'unc_sf'}].values())
                ratios = [ratio, ratio - unc, ratio + unc]
                labels = ["nominal", "statDown", "statUp"]
                # print(ratios[0])
            elif syst != "nominal":
                eff_map = self.eff[{'map': 'sf'}].values()
                # In the bins in which the SF is 0, which are those where the trigger efficiency on data is 0,
                # the SF is set to 1 so that MC is not rescaled in that bin.
                # ratios = [np.where(eff_map != 0, eff_map, 1.0)]
                ratios = [eff_map]
                if self.mode == "standard":
                    labels = [var]
                elif self.mode == "splitHT":
                    labels = [cat]
                elif self.mode == "spliteras":
                    if era == 'tot':
                        labels = ["tot"]
                    else:
                        labels = [era]
                else:
                    raise NotImplementedError
            else:
                return
            self.ratio_stack += ratios
            self.variations_labels += labels
            if (self.mode == "splitHT") and (len(self.ratio_stack) == 3):
                i_nominal = self.variations_labels.index(
                    [c for c in self.variations_labels if c.endswith('pass')][0]
                )
                i_lowHT = self.variations_labels.index(
                    [c for c in self.variations_labels if c.endswith('pass_lowHT')][
                        0
                    ]
                )
                i_highHT = self.variations_labels.index(
                    [
                        c
                        for c in self.variations_labels
                        if c.endswith('pass_highHT')
                    ][0]
                )
                ratio = self.ratio_stack[i_nominal]
                unc = 0.5 * abs(
                    self.ratio_stack[i_highHT] - self.ratio_stack[i_lowHT]
                )
                ratios = [ratio - unc, ratio + unc]
                labels = ["htDown", "htUp"]
                self.ratio_stack = ratios
                self.variations_labels = labels
            elif (self.mode == "spliteras") and (
                len(self.ratio_stack) == len(self.eras)
            ):
                i_nominal = self.variations_labels.index('tot')
                ratio = self.ratio_stack[i_nominal]
                sf_eras = {}
                lumiweights = {}
                for i, era in enumerate(self.variations_labels):
                    if era == 'tot':
                        continue
                    sf_eras[era] = self.ratio_stack[i]
                    lumiweights[era] = self.lumi_fractions[era] * np.ones_like(
                        self.ratio_stack[i]
                    )
                sf_lumiweighed = np.average(
                    list(sf_eras.values()),
                    axis=0,
                    weights=list(lumiweights.values()),
                )
                unc = abs(sf_lumiweighed - ratio)
                ratios = [ratio - unc, ratio + unc]
                labels = ["eraDown", "eraUp"]
                self.ratio_stack = ratios
                self.variations_labels = labels
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"plot1d: Histograms with dimension {self.dim} are not supported")

    def plot2d(self, cat, syst, var, save_plots=True, era=None):
        '''Function to plot a 2D efficiency or scale factor for a given category `cat` and a variation `var`.
        To save the output plots, the flag `save_plots` has to be set to `True`.
        If the computation has to be performed for a specific data-taking era, also the argument `era` has to be specified.'''
        fontsize = self.config["fontsize"]
        fontsize_map = self.config["opts_eff"]["figure"]["fontsize_map"]
        if self.dim == 2:
            if (syst != 'nominal') and (var == 'nominal'):
                pass
            for (label, histo) in zip(
                [
                    "data",
                    "mc",
                    "sf",
                    "unc_data",
                    "unc_mc",
                    "unc_sf",
                    "unc_rel_data",
                    "unc_rel_mc",
                    "unc_rel_sf",
                    "ratio_sf",
                ],
                [
                    self.eff,
                    self.eff,
                    self.eff,
                    self.unc_eff,
                    self.unc_eff,
                    self.unc_eff,
                    self.unc_rel_eff,
                    self.unc_rel_eff,
                    self.unc_rel_eff,
                    self.ratio_sf,
                ],
            ):
                if (var == 'nominal') and (label == "ratio_sf"):
                    continue
                self.map2d = histo[{'map': label}]
                if label == "sf":
                    self.map2d.values()[np.where(self.map2d.values() == 0)] = 1.0

                if save_plots:
                    fig_map, ax_map = plt.subplots(1, 1, figsize=[16, 10])
                    self.map2d.label = self.config['opts_map']['patch_opts'][label]['label']
                    self.map2d.plot2d(
                        ax=ax_map,
                        norm=mpl.colors.Normalize(vmin=self.config['opts_map']['patch_opts'][label]['vmin'],
                                                  vmax=self.config['opts_map']['patch_opts'][label]['vmax'])
                    )

                    hep.cms.text("Preliminary", loc=0, ax=ax_map)
                    hep.cms.lumitext(
                        text=f'{self.totalLumi}'
                        + r' fb$^{-1}$, 13 TeV,'
                        + f' {self.year}',
                        fontsize=18,
                        ax=ax_map,
                    )
                    ax_map.set_title(self.map2d.label)
                    if self.varname_x == 'ElectronGood.pt':
                        ax_map.set_xscale('log')

                    xticks = self.axis_x.edges
                    yticks = self.axis_y.edges
                    ax_map.xaxis.label.set_size(fontsize)
                    ax_map.yaxis.label.set_size(fontsize)
                    ax_map.set_xticks(
                        xticks, [str(int(t)) for t in xticks], fontsize=fontsize
                    )
                    ax_map.tick_params(
                        axis='x',  # changes apply to the x-axis
                        which='minor',  # both major and minor ticks are affected
                        bottom=False,  # ticks along the bottom edge are off
                        top=False,  # ticks along the top edge are off
                        labelbottom=True,
                    )  # labels along the bottom edge are off

                    ax_map.set_yticks(
                        yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize
                    )
                    ax_map.tick_params(
                        axis='y',  # changes apply to the y-axis
                        which='minor',  # both major and minor ticks are affected
                        bottom=False,  # ticks along the bottom edge are off
                        top=False,  # ticks along the top edge are off
                        labelbottom=True,
                    )  # labels along the bottom edge are off
                    ax_map.set_xlim(*self.xlim)
                    ax_map.set_ylim(*self.ylim)
                    plt.vlines(
                        self.axis_x.edges,
                        self.axis_y.edges[-1],
                        self.axis_y.edges[0],
                        linestyle='--',
                        color='gray',
                    )
                    plt.hlines(
                        self.axis_y.edges,
                        self.axis_x.edges[-1],
                        self.axis_x.edges[0],
                        linestyle='--',
                        color='gray',
                    )
                    if self.varname_y == 'ElectronGood.etaSC':
                        for gap in [(-1.5660, -1.4442), (1.4442, 1.5660)]:
                            ax_map.fill_between(
                                [xticks[0], xticks[-1]],
                                gap[0],
                                gap[1],
                                **opts_gap,
                                # label="ECAL gap",
                            )

                    for (i, x) in enumerate(self.bincenter_x):
                        if (x < ax_map.get_xlim()[0]) | (x > ax_map.get_xlim()[1]):
                            continue
                        for (j, y) in enumerate(self.bincenter_y):
                            color = (
                                "w"
                                if self.map2d.values()[()][i][j]
                                < self.config['opts_map']['patch_opts'][label]['vmax']
                                else "k"
                            )
                            # color = "w"
                            if self.varname_y == "ElectronGood.etaSC":
                                if (abs(y) > 1.4442) & (abs(y) < 1.5660):
                                    continue
                                else:
                                    ax_map.text(
                                        x,
                                        y,
                                        f"{round(self.map2d.values()[()][i][j], round_opts[label]['round'])}",
                                        color=color,
                                        ha="center",
                                        va="center",
                                        fontsize=fontsize_map,
                                        fontweight="bold",
                                    )
                            else:
                                ax_map.text(
                                    x,
                                    y,
                                    f"{round(self.map2d.values()[()][i][j], round_opts[label]['round'])}",
                                    color=color,
                                    ha="center",
                                    va="center",
                                    fontsize=fontsize_map,
                                    fontweight="bold",
                                )
                self.save2d(cat, syst, var, label, save_plots, era)
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"plot2d: Histograms with dimension {self.dim} are not supported")

    def save1d(self, save_plots):
        '''Function to save the 1D plots as `png` files if `save_plots` is set to `True`.'''
        if self.dim == 1:
            if save_plots:
                print("Saving", self.filepath_eff)
                self.fig_eff.savefig(
                    self.filepath_eff, dpi=self.config['opts_eff']['figure']['dpi'], format="png"
                )
                plt.close(self.fig_eff)
                print("Saving", self.filepath_sf)
                self.fig_sf1.savefig(
                    self.filepath_sf, dpi=self.config['opts_eff']['figure']['dpi'], format="png"
                )
                plt.close(self.fig_sf1)
                print("Saving", self.filepath_sf_residue)
                self.fig_sf2.savefig(
                    self.filepath_sf_residue,
                    dpi=self.config['opts_eff']['figure']['dpi'],
                    format="png",
                )
                plt.close(self.fig_sf2)
        elif self.dim == 2:
            pass
        else:
            sys.exit(f"save1d: Histograms with dimension {self.dim} are not supported")

    def save2d(self, cat, syst, var, label, save_plots, era=None):
        '''Function that saves the 2D plots as `png` files if `save_plots` is set to `True`.
        The category `cat`, the systematic uncertainty `syst` and the variation `var` have to be specified.
        The argument `label` is required to specify the map that needs to be plotted.'''
        if self.dim == 2:
            if (syst != 'nominal') and (var == 'nominal'):
                return
            if (var == 'nominal') and (label == "ratio_sf"):
                return
            if self.mode in ["standard", "splitHT"]:
                filepath = os.path.join(
                    self.plot_dir,
                    f"{self.histname}_{label}_{cat}_{var}.png",
                )
            elif self.mode == "spliteras":
                filepath = os.path.join(
                    self.plot_dir,
                    f"{self.histname}_{label}_{cat}_{self.year}{era}.png",
                )

            if save_plots:
                print("Saving", filepath)
                plt.savefig(filepath, dpi=self.config['opts_map']['figure']['dpi'], format="png")
            if label in ["sf", "unc_sf"]:
                A = self.map2d
                self.hist_axis_x = A.axes[0]
                self.hist_axis_y = A.axes[1]
                eff_map = self.map2d.values()[()]
                if (
                    (syst == "nominal")
                    and (var == "nominal")
                    and (label == "unc_sf")
                ):
                    if self.mode in ["splitHT", "spliteras"]:
                        return
                    ratio = self.eff_nominal[{'map': 'sf'}].values()
                    # In the bins in which the SF is 0, which are those where the trigger efficiency on data is 0,
                    # the SF is set to 1 so that MC is not rescaled in that bin.
                    # ratio = np.where(ratio != 0, ratio, 1.0)
                    unc = np.array(eff_map)
                    ratios = [ratio - unc, ratio + unc]
                    labels = ["statDown", "statUp"]
                    # print(ratios[0])
                elif label == "sf":
                    # In the bins in which the SF is 0, which are those where the trigger efficiency on data is 0,
                    # the SF is set to 1 so that MC is not rescaled in that bin.
                    ratios = [eff_map]
                    if self.mode == "standard":
                        labels = [var]
                    elif self.mode == "splitHT":
                        labels = [cat]
                    elif self.mode == "spliteras":
                        if era == 'tot':
                            labels = ["tot"]
                        else:
                            labels = [era]
                    else:
                        raise NotImplementedError
                else:
                    return
                self.ratio_stack += ratios
                self.variations_labels += labels
                if (self.mode == "splitHT") and (len(self.ratio_stack) == 3):
                    i_nominal = self.variations_labels.index(
                        [c for c in self.variations_labels if c.endswith('pass')][0]
                    )
                    i_lowHT = self.variations_labels.index(
                        [
                            c
                            for c in self.variations_labels
                            if c.endswith('pass_lowHT')
                        ][0]
                    )
                    i_highHT = self.variations_labels.index(
                        [
                            c
                            for c in self.variations_labels
                            if c.endswith('pass_highHT')
                        ][0]
                    )
                    ratio = self.ratio_stack[i_nominal]
                    unc = 0.5 * abs(
                        self.ratio_stack[i_highHT] - self.ratio_stack[i_lowHT]
                    )
                    ratios = [ratio - unc, ratio + unc]
                    labels = ["htDown", "htUp"]
                    self.ratio_stack = ratios
                    self.variations_labels = labels
                elif (self.mode == "spliteras") and (
                    len(self.ratio_stack) == len(self.eras)
                ):
                    i_nominal = self.variations_labels.index('tot')
                    ratio = self.ratio_stack[i_nominal]
                    sf_eras = {}
                    lumiweights = {}
                    for i, era in enumerate(self.variations_labels):
                        if era == 'tot':
                            continue
                        sf_eras[era] = self.ratio_stack[i]
                        lumiweights[era] = self.lumi_fractions[era] * np.ones_like(
                            self.ratio_stack[i]
                        )
                    sf_lumiweighed = np.average(
                        list(sf_eras.values()),
                        axis=0,
                        weights=list(lumiweights.values()),
                    )
                    unc = abs(sf_lumiweighed - ratio)
                    ratios = [ratio - unc, ratio + unc]
                    labels = ["eraDown", "eraUp"]
                    self.ratio_stack = ratios
                    self.variations_labels = labels
        elif self.dim == 1:
            pass
        else:
            sys.exit(f"save2d: Histograms with dimension {self.dim} are not supported")

    def save_corrections(self):
        '''Function to save the dictionary of corrections containing the scale factor value, the x-axis (and y-axis for 2D maps) and the data-taking year.'''
        if self.dim == 2:
            assert len(self.ratio_stack) == len(
                self.variations_labels
            ), "'ratio_stack' and 'variations_labels' have different length"
            for label, ratio in zip(self.variations_labels, self.ratio_stack):
                self.corrections[label] = {
                    'ratio_stack': ratio,
                    'year': self.year,
                    'hist_axis_x': self.hist_axis_x,
                    'hist_axis_y': self.hist_axis_y,
                }
        elif self.dim == 1:
            assert len(self.ratio_stack) == len(
                self.variations_labels
            ), "'ratio_stack' and 'variations_labels' have different length"
            for label, ratio in zip(self.variations_labels, self.ratio_stack):
                self.corrections[label] = {
                    'ratio_stack': ratio,
                    'year': self.year,
                    'hist_axis_x': self.hist_axis_x,
                }


def plot_efficiency_maps(shape, config, year, outputdir, save_plots=False):
    '''Function to plot 1D and 2D efficiencies and scale factors and save the corrections dictionaries for the systematic variations included in the input histograms.'''

    corrections = {shape.name : {}}

    efficiency_map = EfficiencyMap(shape, config, year, outputdir, mode="standard")
    efficiency_map.define_systematics()
    categories = [
        c for c in efficiency_map.categories if c.endswith(tuple(['pass']))
    ]

    for cat in categories:
        efficiency_map.initialize_stack()

        for syst in efficiency_map.systematics:
            efficiency_map.define_1d_figures(cat, syst, save_plots=save_plots)
            efficiency_map.define_variations(syst)

            for var in efficiency_map.variations:
                efficiency_map.define_datamc(cat, var)
                efficiency_map.compute_efficiency(cat, var)
                efficiency_map.plot1d(cat, syst, var, save_plots=save_plots)
                efficiency_map.plot2d(cat, syst, var, save_plots=save_plots)

            efficiency_map.save1d(save_plots=save_plots)

        efficiency_map.save_corrections()
        corrections[shape.name][cat] = efficiency_map.corrections
    return corrections


def plot_efficiency_maps_splitHT(shape, config, year, outputdir, save_plots=False):
    '''Function to plot 1D and 2D efficiencies and scale factors and save the corrections dictionaries for the HT systematic variation.'''

    corrections = {shape.name : {}}

    efficiency_map = EfficiencyMap(shape, config, year, outputdir, mode="splitHT")
    efficiency_map.define_systematics()
    categories = [
        c
        for c in efficiency_map.categories
        if c.endswith(tuple(['pass', 'pass_lowHT', 'pass_highHT']))
    ]
    efficiency_map.define_1d_figures(
        categories[0], "splitHT", save_plots=save_plots
    )
    efficiency_map.define_variations("splitHT")
    efficiency_map.initialize_stack()

    for cat in categories:
        efficiency_map.define_datamc(cat, "nominal")
        efficiency_map.compute_efficiency(cat, "nominal")
        efficiency_map.plot1d(
            cat, "HT", cat.split('_')[-1], save_plots=save_plots
        )
        efficiency_map.plot2d(cat, "nominal", "nominal", save_plots=save_plots)

    efficiency_map.save1d(save_plots=save_plots)
    efficiency_map.save_corrections()
    corrections[shape.name][
        [c for c in categories if c.endswith('pass')][0]
    ] = efficiency_map.corrections
    return corrections


def plot_efficiency_maps_spliteras(shape, config, year, outputdir, save_plots=False):
    '''Function to plot 1D and 2D efficiencies and scale factors and save the corrections dictionaries for the data-taking era systematic variation.'''

    corrections = {shape.name : {}}
    efficiency_map = EfficiencyMap(shape, config, year, outputdir, mode="spliteras")
    categories = [
        c for c in efficiency_map.categories if c.endswith(tuple(['pass']))
    ]

    for cat in categories:
        efficiency_map.define_systematics()
        efficiency_map.define_1d_figures(
            cat, "spliteras", save_plots=save_plots
        )
        efficiency_map.define_variations("spliteras")
        efficiency_map.initialize_stack()

        print("********************")
        print(efficiency_map.eras)

        for era in efficiency_map.eras:
            efficiency_map.define_datamc(cat, "nominal", era)
            efficiency_map.compute_efficiency(cat, "nominal", era=era)
            efficiency_map.plot1d(
                cat, "era", f"era{era}", save_plots=save_plots, era=era
            )
            efficiency_map.plot2d(
                cat, "nominal", "nominal", save_plots=save_plots, era=era
            )

        efficiency_map.save1d(save_plots=save_plots)
        efficiency_map.save_corrections()
        corrections[shape.name][cat] = efficiency_map.corrections
    return corrections
