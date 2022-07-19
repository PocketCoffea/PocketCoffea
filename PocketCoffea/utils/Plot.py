import os
import sys

import numpy as np
import matplotlib.pyplot as plt

import mplhep as hep
from coffea.hist import Hist, plot
import coffea.hist as hist

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
}

patch_opts = {
    'data' : {'vmax' : 1.0},
    'mc' :   {'vmax' : 1.0},
    'sf' :   {'vmax' : 1.2, 'label' : "Trigger SF"},
    'unc_data' : {'vmax' : 0.05},
    'unc_mc' :   {'vmax' : 0.05},
    'unc_sf' :   {'vmax' : 0.05, 'label' : "Trigger SF unc."},
    'unc_rel_data' : {'vmax' : 0.05},
    'unc_rel_mc' :   {'vmax' : 0.05},
    'unc_rel_sf' :   {'vmax' : 0.05, 'label' : "Trigger SF unc."},
}

def rearrange_axes(accumulator):
    for (histname, h) in accumulator.items():
        if h.axes()[-1] == h.sparse_axes()[-1]:
            accumulator[histname] = hist.Hist("$N_{events}$", *h.sparse_axes(), *h.dense_axes())

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
        ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts)
        return
    elif data & (var != 'nominal'):
        return
    lines = ax.errorbar(x, y, yerr=yerr, xerr=xerr, **opts)
    handles, labels = ax.get_legend_handles_labels()
    if kwargs['histname'] in config.plot_options['scalefactor' if sf else 'efficiency'][kwargs['year']][kwargs['cat']].keys():
        ylim = config.plot_options['scalefactor' if sf else 'efficiency'][kwargs['year']][kwargs['cat']][kwargs['histname']]['ylim']
    else:
        ylim = (0.7, 1.3) if sf else (0, 1)

    if syst != 'nominal':
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

def plot_efficiency(accumulator, config):
    plot_dir = os.path.join(config.plots, "trigger_efficiency")
    fontsize   = config.plot_options["fontsize"]
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
            variable = histname.lstrip('hist_')
            if len(variable.split('_')) == 2:
                varname_x  = variable.split('_')[1]
            elif len(variable.split('_')) == 1:
                varname_x  = variable.split('_')[0]
            varnames = [varname_x]
            if variable in config.plot_options["rebin"].keys():
                h = h.rebin(varname_x, hist.Bin(varname_x, h.axis(varname_x).label, **config.plot_options["rebin"][variable]["binning"]))
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
            totalLumi = femtobarn(lumi[year], digits=1)
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
                        fig_sf, (ax_sf, ax_sf_diff)   = plt.subplots(2,1,figsize=[10,10], gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
                        filepath_eff = os.path.join(plot_dir, f"{histname}_{year}_eff_datamc_{cat}_{syst}.png")
                        filepath_sf = os.path.join(plot_dir, f"{histname}_{year}_sf_{cat}_{syst}.png")
                    elif h.dense_dim() == 2:
                        pass
                    # Loop over nominal + up/down variations. If syst is 'nominal', only the nominal variation is plotted.
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
                        #fields = {varname_x : bincenter_x}
                        eff.fill(cat='data', **fields, weight=eff_data.flatten())
                        eff.fill(cat='mc', **fields, weight=eff_mc.flatten())
                        eff.fill(cat='sf', **fields, weight=sf.flatten())
                        unc_eff.fill(cat='unc_data', **fields, weight=unc_eff_data.flatten())
                        unc_eff.fill(cat='unc_mc', **fields, weight=unc_eff_mc.flatten())
                        unc_eff.fill(cat='unc_sf', **fields, weight=unc_sf.flatten())
                        unc_rel_eff.fill(cat='unc_rel_data', **fields, weight=unc_rel_eff_data.flatten())
                        unc_rel_eff.fill(cat='unc_rel_mc', **fields, weight=unc_rel_eff_mc.flatten())
                        unc_rel_eff.fill(cat='unc_rel_sf', **fields, weight=unc_rel_sf.flatten())

                        if h.dense_dim() == 1:
                            extra_args = {'totalLumi' : totalLumi, 'histname' : histname, 'year' : year, 'variable' : variable, 'config' : config, 'cat' : cat, 'fontsize' : fontsize}
                            plot_variation(bincenter_x, eff['data'].sum('cat').values()[()], unc_eff['unc_data'].sum('cat').values()[()], 0.5*binwidth_x,
                                           axis_x.label, eff.label, syst, var, opts_data, ax_eff, data=True, sf=False, **extra_args)

                            plot_variation(bincenter_x, eff['mc'].sum('cat').values()[()], unc_eff['unc_mc'].sum('cat').values()[()], 0.5*binwidth_x,
                                           axis_x.label, eff.label, syst, var, opts_mc[var.split(syst)[-1] if syst != 'nominal' else syst], ax_eff, data=False, sf=False, **extra_args)

                            plot_variation(bincenter_x, eff['sf'].sum('cat').values()[()], unc_eff['unc_sf'].sum('cat').values()[()], 0.5*binwidth_x,
                                           axis_x.label, "Trigger SF", syst, var, opts_sf[var.split(syst)[-1] if syst != 'nominal' else syst], ax_sf, data=False, sf=True, **extra_args)

                        elif h.dense_dim() == 2:
                            if (syst != 'nominal') and (var == 'nominal'): continue
                            for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf"], [eff, eff, eff, unc_eff, unc_eff, unc_eff, unc_rel_eff, unc_rel_eff, unc_rel_eff]):
                            #for label in ["sf"]:
                                fig_map, ax_map = plt.subplots(1,1,figsize=[16,10])
                                efficiency_map = histo[label].sum('cat')
                                if label == "sf":
                                    efficiency_map.label = "Trigger SF"
                                elif label == "unc_sf":
                                    efficiency_map.label = "Trigger SF unc."

                                plot.plot2d(efficiency_map, ax=ax_map, xaxis=eff.axes()[-2], patch_opts=patch_opts[label])
                                hep.cms.text("Preliminary", loc=0, ax=ax_map)
                                hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax_map)
                                ax_map.set_title(var)
                                print("varname_x", varname_x)
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
                                            ax_map.text(x, y, f"{efficiency_map.values()[()][i][j]:.2}",
                                                    color=color, ha="center", va="center", fontsize=config.plot_options["fontsize_map"], fontweight="bold")
                                        else:
                                            ax_map.text(x, y, f"{efficiency_map.values()[()][i][j]:.2}",
                                                    color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")

                                #plt.title("Electron trigger SF")
                                #cbar = plt.colorbar()
                                #cbar.set_label(legend, fontsize=fontsize)

                                filepath = os.path.join(plot_dir, f"{histname}_{year}_{label}_{cat}_{var}.png")

                                print("Saving", filepath)
                                plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                    if h.dense_dim() == 1:
                        print("Saving", filepath_eff)
                        fig_eff.savefig(filepath_eff, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_eff)
                        print("Saving", filepath_sf)
                        fig_sf.savefig(filepath_sf, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig_sf)
