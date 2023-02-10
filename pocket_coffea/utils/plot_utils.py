import os
import sys

import math
import numpy as np
import awkward as ak
import hist
from coffea.util import load

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import mplhep as hep

from ..parameters.lumi import lumi, femtobarn

fontsize = 22
plt.style.use([hep.style.ROOT, {'font.size': fontsize}])
plt.rcParams.update({'font.size': fontsize})

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
    "step": "post",
    "color": (0, 0, 0, 0.4),
    "facecolor": (0, 0, 0, 0.0),
    "linewidth": 0,
    "hatch": '/' * hatch_density,
    "zorder": 2,
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
    ratio[ratio == 0] = np.nan
    ratio[np.isinf(ratio)] = np.nan
    unc = np.sqrt(num) / den
    unc[np.isnan(ratio)] = np.nan

    return ratio, unc


def get_systematic_uncertainty(
    stack_mc, variations, mcstat=True, stat_only=False, edges=None
):
    '''This function computes the total systematic uncertainty on the MC stack, by summing in quadrature the systematic uncertainties of the single MC samples.
    For each MC sample, the systematic uncertainty is computed by summing in quadrature all the systematic uncertainties.
    The asymmetric error band is constructed by summing in quadrature uncertainties that are moving the nominal value in the same direction, i.e. the total "up" uncertainty is obtained by summing in quadrature all the systematic effects that are pulling the nominal value up.'''
    if stack_mc[0].ndim != 2:
        sys.exit(
            "Only histograms with a dense axis and the variation axis are allowed as input."
        )
    if type(variations) == str:
        variations = [variations]

    systematics = [s.split("Up")[0] for s in variations if 'Up' in s]

    nom_template = stack_mc[0][{'variation': 'nominal'}]
    if edges:
        nom_template = rebin(nom_template, edges)

    syst_err2_tot_up = np.zeros_like(nom_template.values())
    syst_err2_tot_down = np.zeros_like(nom_template.values())
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

                up_is_up = errUp > 0

                # Sum in quadrature of the systematic uncertainties
                syst_err2_up += np.where(up_is_up, errUp**2, errDown**2)
                syst_err2_down += np.where(up_is_up, errDown**2, errUp**2)

        # Add in quadrature the MC statistical uncertainty
        if mcstat:
            syst_err2_up += nom.variances()
            syst_err2_down += nom.variances()

        # Sum in quadrature of the systematic uncertainties for each MC sample
        syst_err2_tot_up += syst_err2_up
        syst_err2_tot_down += syst_err2_down

    return np.sqrt(syst_err2_tot_up), np.sqrt(syst_err2_tot_down)


def plot_systematic_uncertainty(
    stack_mc_nominal, syst_err_up, syst_err_down, ax, ratio=False
):
    '''This function plots the asymmetric systematic uncertainty band on top of the MC stack, if `ratio` is set to False.
    To plot the systematic uncertainty in a ratio plot, `ratio` has to be set to True and the uncertainty band will be plotted around 1 in the ratio plot.'''
    h_mc_sum = stack_sum(stack_mc_nominal)
    nom = h_mc_sum.values()
    up = nom + syst_err_up
    down = nom - syst_err_down

    if ratio:
        up = up / nom
        down = down / nom

    # unc_band = np.array([down, up])
    unc_band = np.nan_to_num(np.array([down, up]), nan=1)

    # print("up", up)
    # print("nom", nom)
    # print("down", down)

    ax.fill_between(
        h_mc_sum.axes[0].edges,
        np.r_[unc_band[0], unc_band[0, -1]],
        np.r_[unc_band[1], unc_band[1, -1]],
        **opts_unc,
        label="syst. unc.",
    )


def plot_data_mc_hist1D(
    h,
    histname,
    config=None,
    plot_dir="plots",
    save=True,
    only_cat=None,
    mcstat=True,
    stat_only=False,
    reweighting_function=None,
    flavorsplit=None,
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

    h_mc = h[samples_mc[0]]

    years = get_axis_items(h_mc, 'year')
    categories = get_axis_items(h_mc, 'cat')
    variations = get_axis_items(h_mc, 'variation')

    if len(samples_data) == 0:
        is_mc_only = True
    else:
        is_mc_only = False

    for year in years:

        for cat in categories:
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
                        if histname in config.plot_options.keys():
                            if 'scale' in config.plot_options[histname].keys():
                                flavors = flavors_order[
                                    config.plot_options[histname]['scale']
                                ]
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
                colors = [colors_5f[f] for f in flavors]
                nevents = {
                    f: round(sum(dict_mc_nominal[f].values()), 1) for f in flavors
                }
            else:
                dict_mc = {d: h[d][slicing_mc] for d in samples_mc}
                dict_mc_nominal = {d: h[d][slicing_mc_nominal] for d in samples_mc}
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
                    if histname in config.plot_options.keys():
                        if 'binning' in config.plot_options[histname].keys():
                            rebinning = True
                            stack_data = rebin(
                                stack_data, config.plot_options[histname]['binning']
                            )
                            stack_mc_nominal = rebin(
                                stack_mc_nominal,
                                config.plot_options[histname]['binning'],
                            )

            if not is_mc_only:
                if len(stack_data) > 1:
                    raise NotImplementedError
                else:
                    h_data = stack_data[0]
                if flavorsplit == '5f':
                    nevents['Data'] = round(sum(h_data.values()))

            totalLumi = femtobarn(lumi[year]['tot'], digits=1)
            fig, (ax, rax) = plt.subplots(
                2,
                1,
                figsize=(12, 12),
                gridspec_kw={"height_ratios": (3, 1)},
                sharex=True,
            )
            fig.subplots_adjust(hspace=0.06)
            hep.cms.text("Preliminary", fontsize=fontsize, loc=0, ax=ax)
            hep.cms.lumitext(
                text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}',
                fontsize=fontsize,
                ax=ax,
            )
            if flavorsplit:
                stack_mc_nominal.plot(stack=True, histtype='fill', ax=ax, color=colors)
            else:
                stack_mc_nominal.plot(stack=True, histtype='fill', ax=ax)
            if not is_mc_only:
                x = dense_axes(stack_mc_nominal)[0].centers
                ax.errorbar(
                    x, h_data.values(), yerr=np.sqrt(h_data.values()), **opts_data
                )
                # stack_data.plot(stack=True, color='black', ax=ax)
            if rebinning:
                syst_err_up, syst_err_down = get_systematic_uncertainty(
                    stack_mc,
                    variations,
                    mcstat=mcstat,
                    stat_only=stat_only,
                    edges=config.plot_options[histname]['binning'],
                )
            else:
                syst_err_up, syst_err_down = get_systematic_uncertainty(
                    stack_mc, variations, mcstat=mcstat, stat_only=stat_only
                )
            # print("syst_err_up", syst_err_up)
            # print("syst_err_down", syst_err_down)
            plot_systematic_uncertainty(
                stack_mc_nominal, syst_err_up, syst_err_down, ax
            )
            if not is_mc_only:
                ratio, unc = get_data_mc_ratio(stack_data, stack_mc_nominal)
                rax.errorbar(x, ratio, unc, **opts_data)
            plot_systematic_uncertainty(
                stack_mc_nominal, syst_err_up, syst_err_down, rax, ratio=True
            )
            ax.set_ylim((0, 1.20 * max(stack_sum(stack_mc_nominal).values())))
            rax.set_ylim((0.0, 2.0))
            xlabel = ax.get_xlabel()
            ax.set_xlabel("")
            ax.set_ylabel("Counts", fontsize=fontsize)
            rax.set_xlabel(xlabel, fontsize=fontsize)
            rax.set_ylabel("Data / MC", fontsize=fontsize)
            rax.yaxis.set_label_coords(-0.075, 1)
            ax.legend(fontsize=fontsize, ncols=2)
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
                ax.legend(handles, labels, fontsize=fontsize, ncols=2)
            ax.tick_params(axis='x', labelsize=fontsize)
            ax.tick_params(axis='y', labelsize=fontsize)
            rax.set_yticks((0, 0.5, 1, 1.5, 2), axis='y', labelsize=fontsize)
            rax.tick_params(axis='x', labelsize=fontsize)
            rax.tick_params(axis='y', labelsize=fontsize)

            if log:
                ax.set_xlim(0, 1)
                exp = math.floor(
                    math.log(max(stack_sum(stack_mc_nominal).values()), 10)
                )
                ax.set_ylim((0.01, 10 ** (exp + 2)))
                # if flavorsplit == '5f':
                #    ax.legend(handles, labels, loc="upper right", fontsize=fontsize, ncols=2)
                # else:
                #    ax.legend(loc="upper right", fontsize=fontsize, ncols=2)
            if config:
                if hasattr(config, "plot_options"):
                    if histname in config.plot_options.keys():
                        if 'xlim' in config.plot_options[histname].keys():
                            ax.set_xlim(*config.plot_options[histname]['xlim'])
                        if 'scale' in config.plot_options[histname].keys():
                            ax.set_yscale(config.plot_options[histname]['scale'])
                            if config.plot_options[histname]['scale'] == 'log':
                                exp = math.floor(
                                    math.log(
                                        max(stack_sum(stack_mc_nominal).values()), 10
                                    )
                                )
                                ax.set_ylim((0.01, 10 ** (exp + 2)))
                                # ax.legend(handles, labels, loc="upper right", fontsize=fontsize, ncols=2)
                        if 'ylim' in config.plot_options[histname].keys():
                            if isinstance(config.plot_options[histname]['ylim'], tuple):
                                ax.set_ylim(*config.plot_options[histname]['ylim'])
                            elif isinstance(
                                config.plot_options[histname]['ylim'], float
                            ) | isinstance(config.plot_options[histname]['ylim'], int):
                                rescale = config.plot_options[histname]['ylim']
                                ax.set_ylim(
                                    (
                                        0,
                                        rescale
                                        * max(stack_sum(stack_mc_nominal).values()),
                                    )
                                )
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
