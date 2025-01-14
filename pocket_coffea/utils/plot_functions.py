import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np

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
        datasets = H[sample].keys()
        datasets_by_year = list(filter(lambda x : year in x, datasets))
        h_by_year = {k :val for k, val in H[sample].items() if k in datasets_by_year}
        if len(h_by_year) == 0:
            raise ValueError(f"No datasets found for {sample} in year {year} in histogram {var}")
        h_sum_datasets = sum(h_by_year.values())
        hep.histplot(h_sum_datasets[cat, variation, :], label=label, ax=axu)

    if ylog:
        axu.set_yscale("log")
    axu.legend()
    axu.set_xlabel('')
    axu.set_ylabel('Events')
    hep.plot.ylow(axu)
    hep.plot.yscale_legend(axu)

    # Ratios
    sample, cat, year, variation, label = shapes[0]
    datasets = H[sample].keys()
    datasets_by_year = list(filter(lambda x : x.endswith(year), datasets))
    h_sum_datasets = sum({k :val for k, val in H[sample].items() if k in datasets_by_year}.values())
    nom = h_sum_datasets[cat, variation, :]
    nomvalues = nom.values()
    nom_sig2 = nom.variances()
    centers = nom.axes[0].centers
    edges = nom.axes[0].edges
    minratio, maxratio = 1000.0, 0.0
    for sample, cat, year, variation, label in shapes[:]:
        datasets = H[sample].keys()
        datasets_by_year = list(filter(lambda x : x.endswith(year), datasets))
        h_sum_datasets = sum({k :val for k, val in H[sample].items() if k in datasets_by_year}.values())
        h = h_sum_datasets[cat, variation, :]
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
    hep.plot.yscale_legend(axd, soft_fail=True)
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
