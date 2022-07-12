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
    'linestyle' : 'none',
    'marker' : '.',
    'markersize' : 1.,
    'color' : 'black',
    'elinewidth' : 1,
    'label' : 'Data'
}
opts_mc = {
    'linestyle' : 'none',
    'marker' : '.',
    'markersize' : 1.,
    'color' : 'red',
    'elinewidth' : 1,
    'label' : r'$t\bar{t}$ DL + SL MC'
}
opts_sf = {
    'linestyle' : 'none',
    'marker' : '.',
    'markersize' : 1.,
    'color' : 'red',
    'elinewidth' : 1,
    'label' : 'SF'
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
            axis_x = h.axes()[-1]
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
            for cat in categories:
                h_data = h[(datasets_data, categories, year, )].sum('sample', 'year')
                h_mc   = h[(datasets_mc, categories, year, )].sum('sample', 'year')
                print(histname)
                #print(h_data.values())
                axis_cat          = h_mc.axis('cat')
                axes_electron     = [h_mc.axis(varname) for varname in varnames]
                #axis_electron_var = h_mc.axis(varname_x)#print(h_data.values())
                num_data = h_data[cat].sum('cat').values()[()]
                den_data = h_data['inclusive'].sum('cat').values()[()]
                eff_data = np.nan_to_num( num_data / den_data )
                num_mc, sumw2_num_mc   = h_mc[cat].sum('cat').values(sumw2=True)[()]
                den_mc, sumw2_den_mc   = h_mc['inclusive'].sum('cat').values(sumw2=True)[()]
                eff_mc   = np.nan_to_num( num_mc / den_mc )
                sf       = np.nan_to_num( eff_data / eff_mc )
                sf       = np.where(sf < 100, sf, 100)
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
                    fig, ax = plt.subplots(1,1,figsize=[10,10])
                    hep.cms.text("Preliminary", loc=0, ax=ax)
                    hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax)
                    #plot.plot1d(eff[['mc','data']], ax=ax, error_opts=opts_datamc);
                    ax.errorbar(bincenter_x, eff['mc'].sum('cat').values()[()], yerr=unc_eff['unc_mc'].sum('cat').values()[()], xerr=0.5*binwidth_x, **opts_mc)
                    ax.errorbar(bincenter_x, eff['data'].sum('cat').values()[()], yerr=unc_eff['unc_data'].sum('cat').values()[()], xerr=0.5*binwidth_x, **opts_data)
                    ax.legend()
                    ax.set_xlabel(axis_x.label, fontsize=fontsize)
                    ax.set_ylabel(eff.label, fontsize=fontsize)
                    ax.set_xlim(*config.variables[variable]['xlim'])
                    ax.set_ylim(0,1.0)
                    #if 'xticks' in config.variables[variable].keys():
                    #    xticks = config.variables[variable]['xticks']
                    #else:
                    #    xticks = axis_x.edges()
                    #ax.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)

                    filepath = os.path.join(plot_dir, f"{histname}_eff_datamc_{cat}_{year}.png")
                    print("Saving", filepath)
                    plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                    plt.close(fig)

                    fig, ax = plt.subplots(figsize=[10,10])
                    hep.cms.text("Preliminary", loc=0, ax=ax)
                    hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax)
                    ax.errorbar(bincenter_x, eff['sf'].sum('cat').values()[()], yerr=unc_eff['unc_sf'].sum('cat').values()[()], xerr=0.5*binwidth_x, **opts_sf)
                    ax.legend()
                    ax.set_xlabel(axis_x.label, fontsize=fontsize)
                    ax.set_ylabel("Trigger SF", fontsize=fontsize)
                    ax.set_xlim(*config.variables[variable]['xlim'])
                    ax.set_ylim(0.8,1.2)

                    filepath = os.path.join(plot_dir, f"{histname}_sf_{cat}_{year}.png")
                    print("Saving", filepath)
                    plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                    plt.close(fig)
                elif h.dense_dim() == 2:
                    for (label, histo) in zip(["data", "mc", "sf", "unc_data", "unc_mc", "unc_sf", "unc_rel_data", "unc_rel_mc", "unc_rel_sf"], [eff, eff, eff, unc_eff, unc_eff, unc_eff, unc_rel_eff, unc_rel_eff, unc_rel_eff]):
                    #for label in ["sf"]:
                        efficiency_map = histo[label].sum('cat')
                        if label == "sf":
                            efficiency_map.label = "Trigger SF"
                        elif label == "unc_sf":
                            efficiency_map.label = "Trigger SF unc."
                        #if label == "sf":
                        #    print(efficiency_map.values())
                        #map    = map_dict["map"]
                        #legend = map_dict["legend"]
                        #vmax   = map_dict["vmax"]
                        #y, x = np.meshgrid(bincenter_y, bincenter_x)
                        #assert ((x.shape == eff[label].shape) & (y.shape == eff[label].shape)), "The arrays corresponding to the x- and y-axes need to have the same shape as the efficiency map."
                        fig, ax = plt.subplots(figsize=[16,10])
                        #weights = map.flatten()
                        #hist, xbins, ybins, im = plt.hist2d(x.flatten(), y.flatten(), weights=map.flatten(), bins=(axis_x.edges(), axis_y.edges()), vmax=vmax)
                        plot.plot2d(efficiency_map, ax=ax, xaxis=eff.axes()[-2], patch_opts=patch_opts[label])
                        hep.cms.text("Preliminary", loc=0, ax=ax)
                        hep.cms.lumitext(text=f'{totalLumi}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=18, ax=ax)
                        print("varname_x", varname_x)
                        if varname_x == 'pt':
                            ax.set_xscale('log')
                        ax.set_xlim(*config.variables2d[variable2d][varname_x]['xlim'])
                        ax.set_ylim(*config.variables2d[variable2d][varname_y]['ylim'])
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
                        ax.xaxis.label.set_size(fontsize)   
                        ax.yaxis.label.set_size(fontsize)
                        ax.set_xticks(xticks, [str(round(t, 4)) for t in xticks], fontsize=fontsize)
                        ax.set_yticks(yticks, [str(round(t, 4)) for t in yticks], fontsize=fontsize)

                        #plt.yticks(yticks, [str(t) for t in yticks], fontsize=fontsize)
                        plt.vlines(axis_x.edges(), axis_y.edges()[-1], axis_y.edges()[0], linestyle='--', color='gray')
                        plt.hlines(axis_y.edges(), axis_x.edges()[-1], axis_x.edges()[0], linestyle='--', color='gray')

                        for (i, x) in enumerate(bincenter_x):
                            if (x < ax.get_xlim()[0]) | (x > ax.get_xlim()[1]): continue
                            for (j, y) in enumerate(bincenter_y):
                                color = "w" if efficiency_map.values()[()][i][j] < patch_opts[label]['vmax'] else "k"
                                #color = "w"
                                if histname == "hist2d_electron_etaSC_vs_electron_pt":
                                    ax.text(x, y, f"{efficiency_map.values()[()][i][j]:.2}",
                                            color=color, ha="center", va="center", fontsize=config.plot_options["fontsize_map"], fontweight="bold")
                                else:
                                    ax.text(x, y, f"{efficiency_map.values()[()][i][j]:.2}",
                                            color=color, ha="center", va="center", fontsize=fontsize, fontweight="bold")

                        #plt.title("Electron trigger SF")
                        #cbar = plt.colorbar()
                        #cbar.set_label(legend, fontsize=fontsize)

                        filepath = os.path.join(plot_dir, f"{histname}_{label}_{cat}_{year}.png")

                        print("Saving", filepath)
                        plt.savefig(filepath, dpi=config.plot_options['dpi'], format="png")
                        plt.close(fig)
