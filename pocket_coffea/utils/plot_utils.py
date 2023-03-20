import os
import sys

import math
import numpy as np
import awkward as ak
import hist
from coffea.util import load

import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
from matplotlib.ticker import MultipleLocator, AutoMinorLocator
import mplhep as hep

from ..parameters.lumi import lumi, femtobarn

fontsize = 22
fontsize_legend_ratio = 12
plt.style.use([hep.style.ROOT, {'font.size': fontsize}])
plt.rcParams.update({'font.size': fontsize})

opts_figure = {
    "total" : {
        'figsize' : (12,12),
        'gridspec_kw' : {"height_ratios": (3, 1)},
        'sharex' : True,
    },
    "partial" : {
        'figsize' : (12,15),
        'gridspec_kw' : {"height_ratios": (3, 1)},
        'sharex' : True,
    },
}

opts_data = {
    'linestyle': 'solid',
    'linewidth': 0,
    'marker': '.',
    'markersize': 5.0,
    'color': 'black',
    'elinewidth': 1,
    'label': 'Data',
}

hatch_density = 4
opts_unc = {
    "total" : {
        "step": "post",
        "color": (0, 0, 0, 0.4),
        "facecolor": (0, 0, 0, 0.0),
        "linewidth": 0,
        "hatch": '/' * hatch_density,
        "zorder": 2,
    },
    'Up': {
        'linestyle': 'dashed',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        #'color': 'red',
        'elinewidth': 1,
    },
    'Down': {
        'linestyle': 'dotted',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        #'color': 'red',
        'elinewidth': 1,
    },
}

opts_unc_total = {
    'Up': {
        'linestyle': 'dashed',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        'color': 'gray',
        'elinewidth': 2,
    },
    'Down': {
        'linestyle': 'dotted',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        'color': 'gray',
        'elinewidth': 2,
    },
}

opts_sf = {
    'nominal': {
        'linestyle': 'solid',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
    },
    'Up': {
        'linestyle': 'dashed',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
    },
    'Down': {
        'linestyle': 'dotted',
        'linewidth': 1,
        'marker': '.',
        'markersize': 1.0,
        'color': 'red',
        'elinewidth': 1,
    },
}

opts_errorbar = {
    'nominal': {'linestyle': 'solid'},
    'Up': {'linestyle': 'dashed'},
    'Down': {'linestyle': 'dotted'},
}

# Flavor ordering by stats, dependent on plot scale
flavors_order = {
    'linear': ['l', 'bb', 'cc', 'b', 'c'],
    'log': ['c', 'b', 'cc', 'bb', 'l'],
}
# Color scheme for l, c+cc, b+bb
colors_bb = ['blue', 'magenta', 'cyan']
colors_cc = ['blue', 'cyan', 'magenta']
colors_5f = {
    'l': (0.51, 0.79, 1.0),  # blue
    'c': (1.0, 0.4, 0.4),  # red
    'b': (1.0, 0.71, 0.24),  # orange
    'cc': (0.96, 0.88, 0.40),  # yellow
    'bb': (0.51, 0.91, 0.51),  # green
}

colors_tthbb = {
    'ttHTobb': 'pink',
    'TTTo2L2Nu': (0.51, 0.79, 1.0),  # blue
    'TTToSemiLeptonic': (1.0, 0.71, 0.24),  # orange
    'SingleTop' : (1.0, 0.4, 0.4), #red
    'ST' : (1.0, 0.4, 0.4), #red
    'WJetsToLNu_HT' : '#cc99ff', #violet
}


def slice_accumulator(accumulator, entrystart, entrystop):
    '''Returns an accumulator containing only a reduced set of histograms, i.e. those between the positions `entrystart` and `entrystop`.'''
    _accumulator = dict([(key, value) for key, value in accumulator.items()])
    _accumulator['variables'] = dict(
        [(key, value) for key, value in accumulator['variables'].items()][
            entrystart:entrystop
        ]
    )
    return _accumulator


def dense_dim(h):
    '''Returns the number of dense axes of a histogram.'''
    dense_dim = 0
    if type(h) == dict:
        h = h[list(h.keys())[0]]
    for ax in h.axes:
        if not type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
            dense_dim += 1
    return dense_dim


def dense_axes(h):
    '''Returns the list of dense axes of a histogram.'''
    dense_axes = []
    if type(h) == dict:
        h = h[list(h.keys())[0]]
    for ax in h.axes:
        if not type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
            dense_axes.append(ax)
    return dense_axes


def stack_sum(stack):
    '''Returns the sum histogram of a stack (`hist.stack.Stack`) of histograms.'''
    if len(stack) == 1:
        return stack[0]
    else:
        htot = stack[0]
        for h in stack[1:]:
            htot = htot + h
        return htot


def get_axis_items(h, axis_name):
    axis = h.axes[axis_name]
    return list(axis.value(range(axis.size)))


def get_index(h, edges):
    indices = []

    for edge in edges:
        indices.append(np.where(np.isclose(h.axes[-1].edges, edge, atol=0.01))[0][0])
    return indices


def rebin_histogram(h, index):
    values = [
        np.sum(h.values()[start:stop]) for start, stop in zip(index[:-1], index[1:])
    ]
    variances = [
        np.sum(h.variances()[start:stop]) for start, stop in zip(index[:-1], index[1:])
    ]
    edges = [h.axes[-1].edges[i] for i in index]
    nh = hist.new.Var(edges, name=h.axes[-1].name, label=h.axes[-1].label).Weight()
    nh[...] = np.stack([values, variances], axis=-1)

    return nh


def rebin_stack(stack, index):
    histos_rebinned = {}
    for i, h in enumerate(stack):
        # print(type(rebin(h, index)))
        histos_rebinned[h.name] = rebin_histogram(h, index)

    return hist.Stack.from_dict(histos_rebinned)


def rebin(h, edges):
    index = get_index(h, edges)
    if type(h) == hist.stack.Stack:
        return rebin_stack(h, index)
    elif type(h) == hist.hist.Hist:
        return rebin_histogram(h, index)


def get_data_mc_ratio(h1, h2):
    if type(h1) == hist.Stack:
        h1 = stack_sum(h1)
    if type(h2) == hist.Stack:
        h2 = stack_sum(h2)
    num = h1.values()
    den = h2.values()
    ratio = num / den
    #ratio[ratio == 0] = np.nan
    #ratio[np.isinf(ratio)] = np.nan
    unc = np.sqrt(num) / den
    unc[np.isnan(unc)] = np.inf

    return ratio, unc


def get_systematic_uncertainty(
    stack_mc, variations, mcstat=True, stat_only=False, edges=None
):
    '''This function computes the total systematic uncertainty on the MC stack, by summing in quadrature the systematic uncertainties of the single MC samples.
    For each MC sample, the systematic uncertainty is computed by summing in quadrature all the systematic uncertainties.
    The asymmetric error band is constructed by summing in quadrature uncertainties that are moving the nominal value in the same direction, i.e. the total "up" uncertainty is obtained by summing in quadrature all the systematic effects that are pulling the nominal value up.'''
    if stack_mc[0].ndim != 2:
        raise Exception(
            "Only histograms with a dense axis and the variation axis are allowed as input."
        )
    if type(variations) == str:
        variations = [variations]

    systematics = [s.split("Up")[0] for s in variations if 'Up' in s]

    nom_template = stack_mc[0][{'variation': 'nominal'}]
    if edges:
        nom_template = rebin(nom_template, edges)

    syst_err2_up_dict = {}
    syst_err2_down_dict = {}
    if not stat_only:
        syst_err2_up_dict = {syst : 0.0 for syst in systematics}
        syst_err2_down_dict = {syst : 0.0 for syst in systematics}
        #syst_err2_tot_up = np.zeros_like(nom_template.values())
        #syst_err2_tot_down = np.zeros_like(nom_template.values())
    if mcstat:
        syst_err2_up_dict.update({"mcstat" : 0.0})
        syst_err2_down_dict.update({"mcstat" : 0.0})
    for h in stack_mc:
        sample = h.name
        nom = h[{'variation': 'nominal'}]
        if edges:
            nom = rebin(nom, edges)
        syst_err2_up = np.zeros_like(nom.values())
        syst_err2_down = np.zeros_like(nom.values())
        if not stat_only:
            for syst in systematics:
                systUp = f"{syst}Up"
                systDown = f"{syst}Down"

                # Compute the uncertainties corresponding to the up/down variations
                varUp = h[{'variation': systUp}]
                varDown = h[{'variation': systDown}]
                if edges:
                    varUp = rebin(varUp, edges)
                    varDown = rebin(varDown, edges)
                errUp = varUp.values() - nom.values()
                errDown = varDown.values() - nom.values()

                # Compute the flags to check which of the two variations (up and down) are pushing the nominal value up and down
                up_is_up = errUp > 0
                down_is_down = errDown < 0
                # Compute the flag to check if the uncertainty is one-sided, i.e. when both variations are up or down
                is_onesided = (up_is_up ^ down_is_down)

                # Sum in quadrature of the systematic uncertainties
                # N.B.: valid only for double sided uncertainties
                syst_err2_up_twosided = np.where(up_is_up, errUp**2, errDown**2)
                syst_err2_down_twosided = np.where(up_is_up, errDown**2, errUp**2)
                syst_err2_max = np.maximum(syst_err2_up_twosided, syst_err2_down_twosided)
                syst_err2_up_onesided = np.where(is_onesided & up_is_up, syst_err2_max, 0)
                syst_err2_down_onesided = np.where(is_onesided & down_is_down, syst_err2_max, 0)
                syst_err2_up_combined = np.where(is_onesided, syst_err2_up_onesided, syst_err2_up_twosided)
                syst_err2_down_combined = np.where(is_onesided, syst_err2_down_onesided, syst_err2_down_twosided)

                syst_err2_up_dict[syst] += syst_err2_up_combined
                syst_err2_down_dict[syst] += syst_err2_down_combined

        # Add in quadrature the MC statistical uncertainty
        if mcstat:
            mcstat_err2 = nom.variances()
            #syst_err2_up += mcstat_err2
            #syst_err2_down += mcstat_err2
            syst_err2_up_dict["mcstat"] += mcstat_err2
            syst_err2_down_dict["mcstat"] += mcstat_err2

        # Sum in quadrature of the systematic uncertainties for each MC sample
        #syst_err2_tot_up += syst_err2_up
        #syst_err2_tot_down += syst_err2_down

    #return np.sqrt(syst_err2_tot_up), np.sqrt(syst_err2_tot_down)
    return syst_err2_up_dict,  syst_err2_down_dict


def plot_uncertainty_band(h_mc_sum, syst_err2_up, syst_err2_down, ax, ratio=False):
    '''This function computes and plots the uncertainty band as a hashed gray area.
    To plot the systematic uncertainty in a ratio plot, `ratio` has to be set to True and the uncertainty band will be plotted around 1 in the ratio plot.'''
    nom = h_mc_sum.values()
    # Sum in quadrature of the systematic uncertainties for each variation
    up = nom + np.sqrt(sum(syst_err2_up.values()))
    down = nom - np.sqrt(sum(syst_err2_down.values()))

    if ratio:
        # In order to get a consistent uncertainty band, the up/down variations of the ratio are set to 1 where the nominal value is 0
        up = np.where(nom != 0, up / nom, 1)
        down = np.where(nom!=0, down / nom, 1)

    unc_band = np.array([down, up])

    ax.fill_between(
        h_mc_sum.axes[0].edges,
        np.r_[unc_band[0], unc_band[0, -1]],
        np.r_[unc_band[1], unc_band[1, -1]],
        **opts_unc['total'],
        label="syst. unc.",
    )
    if ratio:
        ax.hlines(1.0, *ak.Array(h_mc_sum.axes[0].edges)[[0,-1]], colors='gray', linestyles='dashed')

def plot_systematic_uncertainty(
    stack_mc_nominal, syst_err2_up, syst_err2_down, ax, ratio=False, split_systematics=False, only_syst=[], partial_unc_band=False,
):
    '''This function plots the asymmetric systematic uncertainty band on top of the MC stack, if `ratio` is set to False.
    To plot the systematic uncertainty in a ratio plot, `ratio` has to be set to True and the uncertainty band will be plotted around 1 in the ratio plot.'''
    if (not type(syst_err2_up) == dict) or (not type(syst_err2_down) == dict):
        raise Exception("`syst_err2_up` and `syst_err2_down` must be dictionaries")

    h_mc_sum = stack_sum(stack_mc_nominal)
    nom = h_mc_sum.values()

    if only_syst:
        syst_err2_up_filtered = { syst : unc for syst, unc in syst_err2_up.items() if any(key in syst for key in only_syst)}
        syst_err2_down_filtered = { syst : unc for syst, unc in syst_err2_down.items() if any(key in syst for key in only_syst)}
    else:
        syst_err2_up_filtered = syst_err2_up
        syst_err2_down_filtered = syst_err2_down

    if not split_systematics:
        plot_uncertainty_band(h_mc_sum, syst_err2_up_filtered, syst_err2_down_filtered, ax, ratio)
    else:
        if syst_err2_up_filtered.keys() != syst_err2_down_filtered.keys():
            raise Exception("The up and down uncertainties comes from different systematics. `syst_err2_up_filtered` and `syst_err2_down_filtered` must have the same keys.")

        axis_x = h_mc_sum.axes[0]
        edges_x = axis_x.edges
        binwidth_x = np.ediff1d(edges_x)
        x = edges_x[:-1] + 0.5 * binwidth_x
        xerr = 0.5 * binwidth_x

        if partial_unc_band:
            # Sum in quadrature of the systematic uncertainties for each variation
            up_tot = nom + np.sqrt(sum(syst_err2_up_filtered.values()))
            down_tot = nom - np.sqrt(sum(syst_err2_down_filtered.values()))
            label = "partial"
        else:
            # Sum in quadrature of the systematic uncertainties for each variation
            up_tot = nom + np.sqrt(sum(syst_err2_up.values()))
            down_tot = nom - np.sqrt(sum(syst_err2_down.values()))
            label = "total"
        if ratio:
            up_tot = up_tot / nom
            down_tot = down_tot / nom
        linesUp_tot = ax.errorbar(x, up_tot, yerr=0, xerr=xerr, label=f"{label}Up", **opts_unc_total['Up'], fmt='none')
        linesDown_tot = ax.errorbar(x, down_tot, yerr=0, xerr=xerr, label=f"{label}Down", **opts_unc_total['Down'], fmt='none')
        for lines, var in zip([linesDown_tot, linesUp_tot], ['Down', 'Up']):
            errorbar_x = lines[-1][0]
            errorbar_y = lines[-1][1]
            errorbar_x.set_linestyle(opts_errorbar[var]['linestyle'])
            errorbar_y.set_linewidth(0)

        color = iter(cm.gist_rainbow(np.linspace(0, 1, len(syst_err2_up_filtered.keys()))))
        for i, syst in enumerate(syst_err2_up_filtered.keys()):
            up = nom + np.sqrt(syst_err2_up_filtered[syst])
            down = nom - np.sqrt(syst_err2_down_filtered[syst])
            if ratio:
                up = up / nom
                down = down / nom
            c = next(color)
            linesUp = ax.errorbar(x, up, yerr=0, xerr=xerr, label=f"{syst}Up", **opts_unc['Up'], fmt='none', color=c)
            linesDown = ax.errorbar(x, down, yerr=0, xerr=xerr, label=f"{syst}Down", **opts_unc['Down'], fmt='none', color=c)

            for lines, var in zip([linesDown, linesUp], ['Down', 'Up']):
                errorbar_x = lines[-1][0]
                errorbar_y = lines[-1][1]
                errorbar_x.set_linestyle(opts_errorbar[var]['linestyle'])
                errorbar_y.set_linewidth(0)
        if ratio:
            ax.hlines(1.0, *ak.Array(edges_x)[[0,-1]], colors='gray', linestyles='dashed')
            ax.legend(fontsize=fontsize_legend_ratio, ncols=2)
        else:
            ax.legend(fontsize=fontsize, ncols=2)


def plot_data_mc_hist1D(
    h,
    histname,
    config=None,
    plot_dir=None,
    save=True,
    only_cat=None,
    mcstat=True,
    stat_only=False,
    reweighting_function=None,
    flavorsplit=None,
    split_systematics=False,
    only_syst=False,
    partial_unc_band=False,
    log=False,
):
    '''This function plots 1D histograms in all the categories contained in the `cat` axis, for each data-taking year in the `year` axis.
    The data/MC ratio is also shown in the bottom subplot
    The MC systematic uncertainty is plotted on top of the MC stack in the histogram plot and around 1 in the ratio plot.
    The uncertainty on data corresponds to the statistical uncertainty only.
    The plots are saved in `plot_dir` if no config argument is passed. If `save` is False, the plots are not saved but just shown.
    The argument `only_cat` can be a string or a list of strings to plot only a subset of categories.
    To reweight the histograms, one can additionally pass the `reweighting_function` argument which is a 1D function of the variable on the x-axis that modifies the histogram weights.'''
    for sample in h.keys():
        if dense_dim(h[sample]) != 1:
            print(
                f"Histograms with dense dimension {dense_dim(h[sample])} cannot be plotted. Only 1D histograms are supported."
            )
            return
    samples = h.keys()
    samples_data = list(filter(lambda d: 'DATA' in d, samples))
    samples_mc = list(filter(lambda d: 'DATA' not in d, samples))
    
    print("********************")
    print(samples_mc)

    h_mc = h[samples_mc[0]]

    years = get_axis_items(h_mc, 'year')
    categories = get_axis_items(h_mc, 'cat')
    variations = get_axis_items(h_mc, 'variation')

    axis_x = dense_axes(h_mc)[0]
    edges_x = axis_x.edges
    binwidth_x = np.ediff1d(edges_x)

    if len(samples_data) == 0:
        is_mc_only = True
    else:
        is_mc_only = False

    for year in years:

        for cat in categories:
            if 'noreweight' in cat:
                continue
            if only_cat:
                if isinstance(only_cat, list):
                    if not cat in only_cat:
                        continue
                elif isinstance(only_cat, str):
                    if cat != only_cat:
                        continue
                else:
                    raise NotImplementedError
            slicing_mc = {'year': year, 'cat': cat}
            slicing_mc_nominal = {'year': year, 'cat': cat, 'variation': 'nominal'}
            if flavorsplit == '3f':
                flavors = flavors_order['linear']
                dict_mc = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc]
                                for d in samples_mc
                                if d.endswith(f'_{f}')
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_mc_nominal = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc_nominal]
                                for d in samples_mc
                                if d.endswith(f'_{f}')
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_mc = {
                    'l': dict_mc['l'],
                    'c+cc': dict_mc['c'] + dict_mc['cc'],
                    'b+bb': dict_mc['b'] + dict_mc['bb'],
                }
                dict_mc_nominal = {
                    'l': dict_mc_nominal['l'],
                    'c+cc': dict_mc_nominal['c'] + dict_mc_nominal['cc'],
                    'b+bb': dict_mc_nominal['b'] + dict_mc_nominal['bb'],
                }
                if any(
                    cc in histname
                    for cc in [
                        'btagDDCvLV2',
                        'particleNetMD_Xcc_QCD',
                        'deepTagMD_ZHccvsQCD',
                    ]
                ):
                    dict_mc = {
                        'l': dict_mc['l'],
                        'b+bb': dict_mc['b+bb'],
                        'c+cc': dict_mc['c+cc'],
                    }
                    colors = colors_cc
                else:
                    colors = colors_bb
            elif flavorsplit == '5f':
                flavors = flavors_order['linear']
                if config:
                    if hasattr(config, "plot_options"):
                        if histname in config.plot_options["variables"].keys():
                            cfg_plot = config.plot_options["variables"][histname]
                            if 'scale' in cfg_plot.keys():
                                flavors = flavors_order[
                                    cfg_plot['scale']
                                ]
                dict_mc = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluH' not in d))
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_mc_nominal = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc_nominal]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluH' not in d))
                            ]
                        )
                    )
                    for f in flavors
                }
                colors = [colors_5f[f] for f in flavors]
                nevents = {
                    f: round(sum(dict_mc_nominal[f].values()), 1) for f in flavors
                }
                print("************")
                print(histname)
                print(samples_mc)
                dict_gghbb = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluHToBB' in d))
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_gghbb_nominal = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc_nominal]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluHToBB' in d))
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_gghcc = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluHToCC' in d))
                            ]
                        )
                    )
                    for f in flavors
                }
                dict_gghcc_nominal = {
                    f: stack_sum(
                        hist.Stack.from_iter(
                            [
                                h[d][slicing_mc_nominal]
                                for d in samples_mc
                                if (d.endswith(f'_{f}') & ('GluGluHToCC' in d))
                            ]
                        )
                    )
                    for f in flavors
                }
            else:
                dict_mc = {d: h[d][slicing_mc] for d in samples_mc}
                dict_mc_nominal = {d: h[d][slicing_mc_nominal] for d in samples_mc}
                nevents = {
                    d: round(sum(dict_mc_nominal[d].values()), 1) for d in samples_mc
                }
                reverse=True
                if ("variables" in config.plot_options) & (histname in config.plot_options["variables"].keys()):
                    cfg_plot = config.plot_options["variables"][histname]
                    if 'scale' in cfg_plot.keys():
                        reverse = False
                if log:
                    reverse = False
                nevents = dict( sorted(nevents.items(), key=lambda x:x[1], reverse=reverse) )
                dict_mc = {d: dict_mc[d] for d in nevents.keys()}
                dict_mc_nominal = {d: dict_mc_nominal[d] for d in nevents.keys()}
                colors = [colors_tthbb[d] for d in nevents.keys()]
            if reweighting_function:
                for sample, val in dict_mc_nominal.items():
                    histo_reweighted = hist.Hist(dict_mc_nominal[sample].axes[0])
                    histo_reweighted.fill(
                        dict_mc_nominal[sample].axes[0].centers,
                        weight=dict_mc_nominal[sample].values()
                        * reweighting_function(dict_mc_nominal[sample].axes[0].centers),
                    )
                    dict_mc_nominal[sample] = histo_reweighted
            stack_mc = hist.Stack.from_dict(dict_mc)
            stack_mc_nominal = hist.Stack.from_dict(dict_mc_nominal)
            if flavorsplit == '5f':
                stack_gghbb = hist.Stack.from_dict(dict_gghbb)
                stack_gghbb_nominal = hist.Stack.from_dict(dict_gghbb_nominal)
                stack_gghcc = hist.Stack.from_dict(dict_gghcc)
                stack_gghcc_nominal = hist.Stack.from_dict(dict_gghcc_nominal)
                hist_gghbb = stack_sum(stack_gghbb)
                hist_gghbb_nominal = stack_sum(stack_gghbb_nominal)
                hist_gghcc = stack_sum(stack_gghcc)
                hist_gghcc_nominal = stack_sum(stack_gghcc_nominal)

            if not is_mc_only:
                # Sum over eras if era axis exists in data histogram
                if 'era' in h[samples_data[0]].axes.name:
                    slicing_data = {'year': year, 'cat': cat, 'era': sum}
                else:
                    slicing_data = {'year': year, 'cat': cat}
                dict_data = {d: h[d][slicing_data] for d in samples_data}
                stack_data = hist.Stack.from_dict(dict_data)

            rebinning = False
            if config:
                if hasattr(config, "plot_options"):
                    if histname in config.plot_options["variables"].keys():
                        cfg_plot = config.plot_options["variables"][histname]
                        if 'binning' in cfg_plot.keys():
                            rebinning = True
                            stack_data = rebin(
                                stack_data, cfg_plot['binning']
                            )
                            stack_mc_nominal = rebin(
                                stack_mc_nominal,
                                cfg_plot['binning'],
                            )

            if not is_mc_only:
                h_data = stack_sum(stack_data)
                if flavorsplit == '5f':
                    nevents['Data'] = round(sum(h_data.values()))

            totalLumi = femtobarn(lumi[year]['tot'], digits=1)
            if partial_unc_band:
                opts_fig = opts_figure['partial']
            else:
                opts_fig = opts_figure['total']
            fig, (ax, rax) = plt.subplots(2, 1, **opts_fig)
            fig.subplots_adjust(hspace=0.06)
            hep.cms.text("Preliminary", fontsize=fontsize, loc=0, ax=ax)
            hep.cms.lumitext(
                text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}',
                fontsize=fontsize,
                ax=ax,
            )
            stack_mc_nominal.plot(stack=True, histtype='fill', ax=ax, color=colors)
            if not is_mc_only:
                x = dense_axes(stack_mc_nominal)[0].centers
                ax.errorbar(
                    x, h_data.values(), yerr=np.sqrt(h_data.values()), **opts_data
                )
                # stack_data.plot(stack=True, color='black', ax=ax)
            if flavorsplit == '5f':
                sf = 100
                hist_gghbb_nominal = sf * hist_gghbb_nominal
                hist_gghcc_nominal = sf * hist_gghcc_nominal
                hist_gghbb_nominal.plot(stack=False, histtype='step', ax=ax, color='red', label=f'ggHbb x {sf}')
                hist_gghcc_nominal.plot(stack=False, histtype='step', ax=ax, color='green', label=f'ggHcc x {sf}')
            if rebinning:
                syst_err2_up, syst_err2_down = get_systematic_uncertainty(
                    stack_mc,
                    variations,
                    mcstat=mcstat,
                    stat_only=stat_only,
                    edges=cfg_plot['binning'],
                )
            else:
                syst_err2_up, syst_err2_down = get_systematic_uncertainty(
                    stack_mc, variations, mcstat=mcstat, stat_only=stat_only
                )
            # print("syst_err2_up", syst_err2_up)
            # print("syst_err2_down", syst_err2_down)
            plot_systematic_uncertainty(
                stack_mc_nominal, syst_err2_up, syst_err2_down, ax
            )
            if not is_mc_only:
                print(cat, histname)
                ratio, unc = get_data_mc_ratio(stack_data, stack_mc_nominal)
                rax.errorbar(x, ratio, unc, **opts_data)
            plot_systematic_uncertainty(
                stack_mc_nominal, syst_err2_up, syst_err2_down, rax, ratio=True, split_systematics=split_systematics, only_syst=only_syst, partial_unc_band=partial_unc_band
            )
            if is_mc_only:
                maximum = max(stack_sum(stack_mc_nominal).values())
            else:
                maximum = max(stack_sum(stack_data).values())
            if not np.isnan(maximum):
                ax.set_ylim((0, 2.0 * maximum))
            rax.set_ylim((0.5, 1.5))
            xlabel = ax.get_xlabel()
            ax.set_xlabel("")
            ax.set_ylabel("Counts", fontsize=fontsize)
            rax.set_xlabel(xlabel, fontsize=fontsize)
            rax.set_ylabel("Data / MC", fontsize=fontsize)
            rax.yaxis.set_label_coords(-0.075, 1)
            ax.legend(fontsize=fontsize, ncols=2, loc="upper right")
            if flavorsplit == '5f':
                handles, labels = ax.get_legend_handles_labels()
                labels_new = []
                handles_new = []
                for i, f in enumerate(labels):
                    if f in nevents:
                        labels_new.append(f"{f} [{nevents[f]}]")
                    else:
                        labels_new.append(f)
                    handles_new.append(handles[i])
                labels = labels_new
                handles = handles_new
                ax.legend(handles, labels, fontsize=fontsize, ncols=2, loc="upper right")
            ax.tick_params(axis='x', labelsize=fontsize)
            ax.tick_params(axis='y', labelsize=fontsize)
            #rax.set_yticks((0, 0.5, 1, 1.5, 2), axis='y', labelsize=fontsize)
            rax.tick_params(axis='x', labelsize=fontsize)
            rax.tick_params(axis='y', labelsize=fontsize)

            if log:
                ax.set_yscale("log")
                exp = math.floor(
                    math.log(max(stack_sum(stack_mc_nominal).values()), 10)
                )
                ax.set_ylim((0.01, 10 ** (exp + 3)))
                # if flavorsplit == '5f':
                #    ax.legend(handles, labels, loc="upper right", fontsize=fontsize, ncols=2)
                # else:
                #    ax.legend(loc="upper right", fontsize=fontsize, ncols=2)
            if config:
                if hasattr(config, "plot_options"):
                    if "labels" in config.plot_options:
                        handles, labels = ax.get_legend_handles_labels()
                        labels_new = []
                        handles_new = []
                        for i, l in enumerate(labels):
                            if l in config.plot_options["labels"]:
                                labels_new.append(f"{config.plot_options['labels'][l]}")
                            else:
                                labels_new.append(l)
                            handles_new.append(handles[i])
                        labels = labels_new
                        handles = handles_new
                        ax.legend(handles, labels, fontsize=fontsize, ncols=2, loc="upper right")
                    if ("variables" in config.plot_options) & (histname in config.plot_options["variables"].keys()):
                        cfg_plot = config.plot_options["variables"][histname]
                        if 'xlim' in cfg_plot.keys():
                            ax.set_xlim(*cfg_plot['xlim'])
                        if 'scale' in cfg_plot.keys():
                            ax.set_yscale(cfg_plot['scale'])
                            if cfg_plot['scale'] == 'log':
                                exp = math.floor(
                                    math.log(
                                        max(stack_sum(stack_mc_nominal).values()), 10
                                    )
                                )
                                ax.set_ylim((0.01, 10 ** (exp + 3)))
                                # ax.legend(handles, labels, loc="upper right", fontsize=fontsize, ncols=2)
                        if 'ylim' in cfg_plot.keys():
                            if isinstance(cfg_plot['ylim'], tuple):
                                ax.set_ylim(*cfg_plot['ylim'])
                            elif isinstance(
                                cfg_plot['ylim'], float
                            ) | isinstance(cfg_plot['ylim'], int):
                                rescale = cfg_plot['ylim']
                                ax.set_ylim(
                                    (
                                        0,
                                        rescale
                                        * max(stack_sum(stack_mc_nominal).values()),
                                    )
                                )
                        if 'xlabel' in cfg_plot.keys():
                            rax.set_xlabel(cfg_plot['xlabel'])
                        if 'ylabel' in cfg_plot.keys():
                            rax.set_ylabel(cfg_plot['ylabel'])
                        if 'xticks' in cfg_plot.keys():
                            rax.set_xticks(cfg_plot['xticks'])
                            rax.xaxis.set_minor_locator(MultipleLocator(binwidth_x[0]))
                else:
                    if histname in config.variables.keys():
                        if config.variables[histname].axes[0].lim != (0, 0):
                            ax.set_xlim(*config.variables[histname].axes[0].lim)
                        else:
                            ax.set_xlim(
                                config.variables[histname].axes[0].start,
                                config.variables[histname].axes[0].stop,
                            )
                plot_dir = os.path.join(config.plots, cat)
            if log:
                plot_dir = os.path.join(plot_dir, "log")
            if not os.path.exists(plot_dir):
                os.makedirs(plot_dir)
            filepath = os.path.join(plot_dir, f"{histname}_{year}_{cat}.png")

            if save:
                print("Saving", filepath)
                plt.savefig(filepath, dpi=150, format="png")
            else:
                plt.show()
            plt.close(fig)


def plot_shapes_comparison(
    df,
    var,
    shapes,
    title=None,
    ylog=False,
    output_folder=None,
    figsize=(8, 9),
    dpi=100,
    lumi_label="$137/fb$ (13 TeV)",
    outputfile=None,
):
    '''
    This function plots the comparison between different shapes, specified in the format
    shapes = [ (sample,cat,year,variation, label),]

    The sample, cat and year are used to retrive the shape from the `df`, the label is used in the plotting.
    The ratio of all the shapes w.r.t. of the first one in the list are printed.

    The plot is saved if outputfile!=None.
    '''
    H = df[var]
    fig = plt.figure(figsize=figsize, dpi=dpi)
    gs = fig.add_gridspec(nrows=2, ncols=1, hspace=0.05, height_ratios=[0.75, 0.25])
    axs = gs.subplots(sharex=True)
    plt.subplots_adjust(wspace=0.3)

    axu = axs[0]
    axd = axs[1]

    for sample, cat, year, variation, label in shapes:
        print(sample, cat, year, variation)
        hep.histplot(H[sample][cat, variation, year, :], label=label, ax=axu)

    if ylog:
        axu.set_yscale("log")
    axu.legend()
    axu.set_xlabel('')
    axu.set_ylabel('Events')
    hep.plot.ylow(axu)
    hep.plot.yscale_legend(axu)

    # Ratios
    sample, cat, year, variation, label = shapes[0]
    nom = H[sample][cat, variation, year, :]
    nomvalues = nom.values()
    nom_sig2 = nom.variances()
    centers = nom.axes[0].centers
    edges = nom.axes[0].edges
    minratio, maxratio = 1000.0, 0.0
    for sample, cat, year, variation, label in shapes[:]:
        h = H[sample][cat, variation, year, :]
        h_val = h.values()
        h_sig2 = h.variances()

        err = np.sqrt(
            (1 / nomvalues) ** 2 * h_sig2 + (h_val / nomvalues**2) ** 2 * nom_sig2
        )
        r = np.where(nomvalues > 0, h.values() / nomvalues, 1.0)
        m, M = np.min(r), np.max(r)
        if m < minratio:
            minratio = m
        if M > maxratio:
            maxratio = M
        axd.errorbar(
            centers,
            r,
            xerr=0,
            yerr=err,
            label=label,
            fmt=".",
            linestyle='none',
            elinewidth=1,
        )

    axd.legend(ncol=3, fontsize='xx-small')
    hep.plot.yscale_legend(axd)
    axd.set_xlabel(nom.axes[0].label)
    axd.set_ylim(0.8 * minratio, 1.2 * maxratio)
    axd.set_ylabel("ratio")
    axd.grid(which="both", axis="y")

    if title:
        axu.text(0.5, 1.025, title, transform=axu.transAxes, fontsize='x-small')

    hep.cms.label(llabel="", rlabel=lumi_label, loc=0, ax=axu)

    if outputfile:
        fig.savefig(outputfile.replace("*", "png"))
        fig.savefig(outputfile.replace("*", "pdf"))
    return fig
