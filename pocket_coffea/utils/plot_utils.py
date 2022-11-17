import os
import sys

import numpy as np
import awkward as ak
import hist
from coffea.util import load

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import mplhep as hep

from ..parameters.lumi import lumi, femtobarn

plt.style.use([hep.style.ROOT, {'font.size': 18}])

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


def get_systematic_uncertainty(stack_mc, variations):
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

    syst_err2_tot_up = np.zeros_like(nom_template.values())
    syst_err2_tot_down = np.zeros_like(nom_template.values())
    for h in stack_mc:
        sample = h.name
        nom = h[{'variation': 'nominal'}]
        syst_err2_up = np.zeros_like(nom.values())
        syst_err2_down = np.zeros_like(nom.values())
        for syst in systematics:
            systUp = f"{syst}Up"
            systDown = f"{syst}Down"

            # Compute the uncertainties corresponding to the up/down variations
            varUp = h[{'variation': systUp}]
            varDown = h[{'variation': systDown}]
            errUp = varUp.values() - nom.values()
            errDown = varDown.values() - nom.values()

            up_is_up = errUp > 0

            # Sum in quadrature of the systematic uncertainties
            syst_err2_up += np.where(up_is_up, errUp**2, errDown**2)
            syst_err2_down += np.where(up_is_up, errDown**2, errUp**2)

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


def plot_data_mc_hist1D(h, histname, config):
    '''This function plots 1D histograms in all the categories contained in the `cat` axis, for each data-taking year in the `year` axis.
    The data/MC ratio is also shown in the bottom subplot
    The MC systematic uncertainty is plotted on top of the MC stack in the histogram plot and around 1 in the ratio plot.
    The uncertainty on data corresponds to the statistical uncertainty only.'''
    for sample in h.keys():
        if dense_dim(h[sample]) != 1:
            return
    samples = h.keys()
    samples_data = list(filter(lambda d: 'DATA' in d, samples))
    samples_mc = list(filter(lambda d: 'DATA' not in d, samples))

    h_mc = h[samples_mc[0]]

    dense_axis = dense_axes(h_mc)[0]
    x = dense_axis.centers
    years = get_axis_items(h_mc, 'year')
    categories = get_axis_items(h_mc, 'cat')
    variations = get_axis_items(h_mc, 'variation')

    if len(samples_data) == 0:
        is_mc_only = True
    else:
        is_mc_only = False

    for year in years:
        totalLumi = femtobarn(lumi[year]['tot'], digits=1)

        for cat in categories:
            slicing_mc = {'year': '2018', 'cat': cat}
            slicing_mc_nominal = {'year': '2018', 'cat': cat, 'variation': 'nominal'}
            dict_mc = {d: h[d][slicing_mc] for d in samples_mc}
            dict_mc_nominal = {d: h[d][slicing_mc_nominal] for d in samples_mc}
            stack_mc = hist.Stack.from_dict(dict_mc)
            stack_mc_nominal = hist.Stack.from_dict(dict_mc_nominal)
            if not is_mc_only:
                slicing_data = {'year': '2018', 'cat': cat}
                dict_data = {d: h[d][slicing_data] for d in samples_data}
                stack_data = hist.Stack.from_dict(dict_data)
                if len(stack_data) > 1:
                    raise NotImplementedError
                else:
                    h_data = stack_data[0]

            fig, (ax, rax) = plt.subplots(
                2,
                1,
                figsize=(12, 12),
                gridspec_kw={"height_ratios": (3, 1)},
                sharex=True,
            )
            fig.subplots_adjust(hspace=0.08)
            hep.cms.text("Preliminary", loc=0, ax=ax)
            hep.cms.lumitext(
                text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}',
                fontsize=18,
                ax=ax,
            )
            stack_mc_nominal.plot(stack=True, histtype='fill', ax=ax)
            if not is_mc_only:
                ax.errorbar(
                    x, h_data.values(), yerr=np.sqrt(h_data.values()), **opts_data
                )
                # stack_data.plot(stack=True, color='black', ax=ax)
            syst_err_up, syst_err_down = get_systematic_uncertainty(
                stack_mc, variations
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
            rax.set_ylim((0.8, 1.2))
            if config.variables[histname].axes[0].lim != (0,0):
                ax.set_xlim(*config.variables[histname].axes[0].lim)
            else:
                ax.set_xlim(config.variables[histname].axes[0].start, config.variables[histname].axes[0])

            xlabel = ax.get_xlabel()
            ax.set_xlabel("")
            rax.set_xlabel(xlabel)
            ax.legend()
            filepath = os.path.join(config.plots, f"{histname}_{year}_{cat}.png")
            print("Saving", filepath)
            plt.savefig(filepath, dpi=300, format="png")
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
