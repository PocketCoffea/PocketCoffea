import os
from copy import deepcopy
from multiprocessing import Pool
from collections import defaultdict
from functools import partial

import math
import numpy as np
import awkward as ak
import hist

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
from matplotlib.ticker import MultipleLocator, AutoMinorLocator
import mplhep as hep

from omegaconf import OmegaConf
from pocket_coffea.parameters.defaults import merge_parameters


class Style:
    '''This class manages all the style options for Data/MC plots.'''

    def __init__(self, style_cfg) -> None:
        self.style_cfg = style_cfg
        self._required_keys = ["opts_figure", "opts_mc", "opts_data", "opts_unc"]
        for key in self._required_keys:
            assert (
                key in style_cfg
            ), f"The key `{key}` is not defined in the style dictionary."
        for key, item in style_cfg.items():
            setattr(self, key, item)
        self.has_labels = False
        self.has_samples_groups = False
        self.has_lumi = False
        if "labels_mc" in style_cfg:
            self.has_labels = True
        if "samples_groups" in style_cfg:
            self.has_samples_groups = True

        self.set_defaults()

    def set_defaults(self):
        if not "stack" in self.opts_mc:
            self.opts_mc["stack"] = True
        if not hasattr(self, "fontsize"):
            self.fontsize = 22

    def update(self, style_cfg):
        '''Updates the style options with a new dictionary.'''
        if not type(style_cfg) in [dict, defaultdict]:
            raise Exception("The style options should be passed as a dictionary.")
        style_new = OmegaConf.create(style_cfg)
        valid_keys = self._required_keys + ["opts_axes"]
        for key, item in style_cfg.items():
            if key not in valid_keys:
                raise Exception(f"The key `{key}` is not a valid style option. Valid keys: {valid_keys}")
            self.style_cfg = merge_parameters(self.style_cfg, style_new, update=True)
        for key, item in style_cfg.items():
            setattr(self, key, item)


class PlotManager:
    '''This class manages multiple Shape objects and their plotting.'''

    def __init__(
        self,
        variables,
        hist_objs,
        datasets_metadata,
        plot_dir,
        style_cfg,
        toplabel=None,
        only_cat=None,
        workers=8,
        log=False,
        density=False,
        save=True
    ) -> None:

        self.shape_objects = {}
        self.plot_dir = plot_dir
        self.only_cat = only_cat
        self.workers = workers
        self.log = log
        self.density = density
        self.save = save
        self.nhists = len(variables)
        self.toplabel = toplabel

        # Reading the datasets_metadata to
        # build the correct shapes for each datataking year
        # taking histo objects from hist_objs
        self.hists_to_plot = {}
        for variable in variables:
            vs = {}
            for year, samples in datasets_metadata["by_datataking_period"].items():
                hs = {}
                for sample, datasets in samples.items():
                    hs[sample] = {}
                    for dataset in datasets:
                        try:
                            hs[sample][dataset] = hist_objs[variable][sample][dataset]
                        except:
                            print(f"Warning: missing dataset {dataset} for variable {variable}, year {year}")
                vs[year] = hs
            self.hists_to_plot[variable] = vs
        
        for variable, histoplot in self.hists_to_plot.items():
            for year, h_dict in histoplot.items():
                name = '_'.join([variable, year])
                # If toplabel is overwritten we use that, if not we take the lumi from the year
                if self.toplabel:
                    toplabel_to_use = self.toplabel
                else:
                    toplabel_to_use = f"$\mathcal{{L}}$ = {style_cfg.plot_upper_label.by_year[year]:.2f}/fb"
                
                self.shape_objects[name] = Shape(
                    h_dict,
                    datasets_metadata["by_dataset"],
                    name,
                    plot_dir,
                    style_cfg=style_cfg,
                    only_cat=self.only_cat,
                    log=self.log,
                    density=self.density,
                    toplabel=toplabel_to_use
                )
        if self.save:
            self.make_dirs()
            matplotlib.use('agg')

    def make_dirs(self):
        '''Create directories recursively before saving plots with multiprocessing
        to avoid conflicts between different processes.'''
        for name, shape in self.shape_objects.items():
            for cat in shape.categories:
                plot_dir = os.path.join(self.plot_dir, cat)
                if not os.path.exists(plot_dir):
                    os.makedirs(plot_dir)

    def plot_datamc(self, name, syst=True, spliteras=False):
        '''Plots one histogram, for all years and categories.'''
        print("Plotting: ", name)
        shape = self.shape_objects[name]
        if shape.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {shape.name} with dimension {shape.dense_dim}.")
            print("The method `plot_datamc` will be skipped.")
            return

        if ((shape.is_mc_only) | (shape.is_data_only)):
            ratio = False
        else:
            ratio = True
        shape.plot_datamc_all(ratio, syst, spliteras=spliteras, save=self.save)

    def plot_datamc_all(self, syst=True,  spliteras=False):
        '''Plots all the histograms contained in the dictionary, for all years and categories.'''
        shape_names = list(self.shape_objects.keys())
        if self.workers > 1:
            with Pool(processes=self.workers) as pool:
                # Parallel calls of plot_datamc() on different shape objects
                pool.map(partial(self.plot_datamc, syst=syst, spliteras=spliteras), shape_names)
                pool.close()
        else:
            for shape in shape_names:
                self.plot_datamc(shape, syst=syst, spliteras=spliteras)


class Shape:
    '''This class handles the plotting of 1D data/MC histograms.
    The constructor requires as arguments:
    - h_dict: dictionary of histograms, with the following structure {}
    - name: name that identifies the Shape object.
    - style_cfg: dictionary with style and plotting options.
    '''
    def __init__(
        self,
        h_dict,
        datasets_metadata, 
        name,
        plot_dir,
        style_cfg,
        toplabel=None,
        only_cat=None,
        log=False,
        density=False,
    ) -> None:
        self.h_dict = h_dict
        self.name = name
        self.plot_dir = plot_dir
        self.only_cat = only_cat if only_cat is not None else []
        self.style = Style(style_cfg)
        self.toplabel = toplabel if toplabel else ""
        if self.style.has_lumi:
            self.lumi_fraction = {year : l / lumi[year]['tot'] for year, l in self.style.lumi_processed.items()}
        self.log = log
        self.density = density
        self.datasets_metadata=datasets_metadata
        self.sample_is_MC = {}
        self._stacksCache = defaultdict(dict)
        assert (
            type(h_dict) in [dict, defaultdict]
        ), "The Shape object receives a dictionary of hist.Hist objects as argument."
        self.group_samples()
        self.load_attributes()
        self.syst_manager = SystManager(self, self.style)

    def load_attributes(self):
        '''Loads the attributes from the dictionary of histograms.'''
        assert len(
            set([self.h_dict[s].ndim for s in self.samples_mc])
        ), f"{self.name}: Not all the MC histograms have the same dimension."
        assert len(
            set([self.h_dict[s].ndim for s in self.samples_data])
        ), f"{self.name}: Not all the data histograms have the same dimension."
        
        for ax in self.categorical_axes_mc:
            setattr(
                self,
                {'year': 'years', 'cat': 'categories', 'variation': 'variations'}[
                    ax.name
                ],
                self.get_axis_items(ax.name),
            )
        xaxis = self.dense_axes[0]
        self.style.update(
            {
            'opts_axes' : {
                'xlabel' : xaxis.label,
                'xcenters' : xaxis.centers.tolist(),
                'xedges' : xaxis.edges.tolist(),
                'xbinwidth' : np.ediff1d(xaxis.edges).tolist()
                }
            }
        )

        if self.dense_dim == 2:
            yaxis = self.dense_axes[1]
            self.style.update(
                {
                    'opts_axes' : {
                        'ylabel' : yaxis.label,
                        'ycenters' : yaxis.centers.tolist(),
                        'yedges' : yaxis.edges.tolist(),
                        'ybinwidth' : np.ediff1d(yaxis.edges).tolist()
                    }
                }
            )

        self.is_mc_only = True if len(self.samples_data) == 0 else False
        self.is_data_only = True if len(self.samples_mc) == 0 else False

        
    @property
    def dense_axes(self):
        '''Returns the list of dense axes of a histogram, defined as the axes that are not categorical axes.'''
        dense_axes_dict = {s: [] for s in self.h_dict.keys()}

        for s, h in self.h_dict.items():
            for ax in h.axes:
                if not type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
                    dense_axes_dict[s].append(ax)
        dense_axes = list(dense_axes_dict.values())
        assert all(
            v == dense_axes[0] for v in dense_axes
        ), "Not all the histograms in the dictionary have the same dense dimension."
        dense_axes = dense_axes[0]

        return dense_axes

    def _categorical_axes(self, mc=True):
        '''Returns the list of categorical axes of a histogram.'''
        # Since MC and data have different categorical axes, the argument mc needs to specified
        if mc:
            d = {s: v for s, v in self.h_dict.items() if s in self.samples_mc}
        else:
            d = {s: v for s, v in self.h_dict.items() if s in self.samples_data}
        categorical_axes_dict = {s: [] for s in d.keys()}

        for s, h in d.items():
            for ax in h.axes:
                if type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
                    categorical_axes_dict[s].append(ax)
        categorical_axes = list(categorical_axes_dict.values())
        assert all(
            v == categorical_axes[0] for v in categorical_axes
        ), "Not all the histograms in the dictionary have the same categorical dimension."
        categorical_axes = categorical_axes[0]

        return categorical_axes

    @property
    def categorical_axes_mc(self):
        '''Returns the list of categorical axes of a MC histogram.'''
        return self._categorical_axes(mc=True)

    @property
    def categorical_axes_data(self):
        '''Returns the list of categorical axes of a data histogram.'''
        return self._categorical_axes(mc=False)

    @property
    def dense_dim(self):
        '''Returns the number of dense axes of a histogram.'''
        return len(self.dense_axes)

    def get_axis_items(self, axis_name, mc=True):
        '''Returns the list of values contained in a Hist axis.'''
        if mc:
            axis = [ax for ax in self.categorical_axes_mc if ax.name == axis_name][0]
        else:
            axis = [ax for ax in self.categorical_axes_data if ax.name == axis_name][0]
        return list(axis.value(range(axis.size)))

    def _stack_sum(self, cat=None, mc=None, stack=None):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of histograms.'''
        if not stack:
            if not cat:
                raise Exception("The category `cat` should be passed when the `stack` option is not specified.")
            if mc:
                stack = self._stacksCache[cat]["mc"]
            else:
                stack = self._stacksCache[cat]["data"]
        if len(stack) == 1:
            return stack[0]
        else:
            htot =hist.Hist(stack[0])
            for h in stack[1:]:
                htot = htot + h
            return htot

    @property
    def stack_sum_data(self, cat):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of data histograms.'''
        return self._stack_sum(cat, mc=False)

    @property
    def stack_sum_mc_nominal(self, cat):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of MC histograms.'''
        return self._stack_sum(cat, mc=True)

    @property
    def samples(self):
        return list(self.h_dict.keys())

    @property
    def samples_data(self):
        return list(filter(lambda d: not self.sample_is_MC[d], self.samples))

    @property
    def samples_mc(self):
        return list(filter(lambda d: self.sample_is_MC[d], self.samples))

    def group_samples(self):
        '''Groups samples according to the dictionary self.style.samples_map'''
        # # First of all check if collapse_datasets options is true,
        # # in that case all the datasets (parts) for each sample are summed
        if self.style.collapse_datasets:
            # Sum over the different datasets for each sample
            for sample, datasets in self.h_dict.items():
                self.h_dict[sample] = self._stack_sum(
                    stack=hist.Stack.from_dict(
                        {s: h for s, h in datasets.items() if s in datasets}
                    )
                )
                isMC = None
                for dataset in datasets:
                    isMC_d = self.datasets_metadata[dataset]["isMC"] == "True"
                    if isMC is None:
                        isMC = isMC_d
                        self.sample_is_MC[sample] = isMC
                    elif isMC != isMC_d:
                        raise Exception(f"You are collapsing together data and MC histogram!")
                    
        else:
            raise NotImplementedError("Plotting histograms without collapsing is still not implemented")
        
        if not self.style.has_samples_groups:
            return
        h_dict_grouped = {}
        samples_in_map = []
        for sample_new, samples_list in self.style.samples_groups.items():
            h_dict_grouped[sample_new] = self._stack_sum(
                stack=hist.Stack.from_dict(
                    {s: h for s, h in self.h_dict.items() if s in samples_list}
                )
            )
            samples_in_map += samples_list
        for s, h in self.h_dict.items():
            if s not in samples_in_map:
                h_dict_grouped[s] = h
        self.h_dict = deepcopy(h_dict_grouped)
        

    def _get_stacks(self, cat, spliteras=False):
        '''Builds the data and MC stacks, applying a slicing by category.
        The stacks are cached in a dictionary so that they are not recomputed every time.
        If spliteras is True, the extra axis "era" is kept in the data stack to
        distinguish between data samples from different data-taking eras.'''
        if not cat in self._stacksCache:
            stacks = {}
            slicing_mc = {'cat': cat}
            slicing_mc_nominal = {'cat': cat, 'variation': 'nominal'}
            h_dict_mc = {d: self.h_dict[d][slicing_mc] for d in self.samples_mc}
            h_dict_mc_nominal = {
                d: self.h_dict[d][slicing_mc_nominal] for d in self.samples_mc
            }
            # Store number of weighted MC events
            self.nevents = {
                d: round(sum(h_dict_mc_nominal[d].values()), 1)
                for d in self.samples_mc
            }
            reverse = True
            # Order the events dictionary by decreasing number of events if linear scale, increasing if log scale
            # N.B.: Here implement if log: reverse=False
            self.nevents = dict(
                sorted(self.nevents.items(), key=lambda x: x[1], reverse=reverse)
            )
            color = iter(cm.gist_rainbow(np.linspace(0, 1, len(self.nevents.keys()))))
            # Assign random colors to each sample
            self.colors = [next(color) for d in self.nevents.keys()]
            if hasattr(self.style, "colors_mc"):
                # Initialize random colors
                for i, d in enumerate(self.nevents.keys()):
                    # If the color for a corresponding sample exists in the dictionary, assign the color to the sample
                    if d in self.style.colors_mc:
                        self.colors[i] = self.style.colors_mc[d]
            # Order the MC dictionary by number of events
            h_dict_mc = {d: h_dict_mc[d] for d in self.nevents.keys()}
            h_dict_mc_nominal = {
                d: h_dict_mc_nominal[d] for d in self.nevents.keys()
            }
            # Build MC stack with variations and nominal MC stack
            stacks["mc"] = hist.Stack.from_dict(h_dict_mc)
            stacks["mc_nominal"] = hist.Stack.from_dict(h_dict_mc_nominal)
            stacks["mc_nominal_sum"] = self._stack_sum(stack = stacks["mc_nominal"])

            if not self.is_mc_only:
                # Sum over eras if specified as extra argument
                if 'era' in [ax.name for ax in self.categorical_axes_data]:
                    if spliteras:
                        slicing_data = { 'cat': cat}
                    else:
                        slicing_data = {'cat': cat, 'era': sum}
                else:
                    if spliteras:
                        raise Exception(
                            "No axis 'era' found. Impossible to split data by era."
                        )
                    else:
                        slicing_data = {'cat': cat}
                self.h_dict_data = {
                    d: self.h_dict[d][slicing_data] for d in self.samples_data
                }
                stacks["data"] = hist.Stack.from_dict(self.h_dict_data)
                stacks["data_sum"] = self._stack_sum(stack = stacks["data"])
            self._stacksCache[cat] = stacks
            self.syst_manager.update()
        return self._stacksCache[cat]

    def get_datamc_ratio(self, cat):
        '''Computes the data/MC ratio and the corresponding uncertainty.'''
        stacks = self._get_stacks(cat)
        num = stacks["data_sum"].values()

        den = stacks["mc_nominal_sum"].values()

        if np.any(den <0 ):
            print(f"WARNING: negative bins in MC of shape {self.name}. BE CAREFUL! Putting negative bins to 0 for plotting..")
        den[den < 0] = 0
            
        ratio = num / den
        # TO DO: Implement Poisson interval valid also for num~0
        # np.sqrt(num) is just an approximation of the uncertainty valid at large num
        ratio_unc = np.sqrt(num) / den
        ratio_unc[np.isnan(ratio_unc)] = np.inf

        return ratio, ratio_unc

    def define_figure(self, ratio=True):
        '''Defines the figure for the Data/MC plot.
        If ratio is True, a subplot is defined to include the Data/MC ratio plot.'''
        plt.style.use([hep.style.ROOT, {'font.size': self.style.fontsize}])
        plt.rcParams.update({'font.size': self.style.fontsize})
        if ratio:
            self.fig, (self.ax, self.rax) = plt.subplots(
                2, 1, **self.style.opts_figure["datamc_ratio"]
            )
            self.fig.subplots_adjust(hspace=0.06)
            axes = (self.ax, self.rax)
        else:
            self.fig, self.ax = plt.subplots(1, 1, **self.style.opts_figure["datamc"])
            axes = self.ax
        if self.is_mc_only:
            hep.cms.text(
                "Simulation Preliminary",
                fontsize=self.style.fontsize,
                loc=0,
                ax=self.ax,
            )
        else:
            hep.cms.text(
                "Preliminary",
                fontsize=self.style.fontsize,
                loc=0,
                ax=self.ax,
            )
        if self.toplabel:
            hep.cms.lumitext(
                text=self.toplabel,
                fontsize=self.style.fontsize,
                ax=self.ax,
            )
        return self.fig, axes

    def format_figure(self, cat, ratio=True):
        '''Formats the figure's axes, labels, ticks, xlim and ylim.'''
        stacks = self._get_stacks(cat)
        ylabel = "Counts" if not self.density else "A.U."
        self.ax.set_ylabel(ylabel, fontsize=self.style.fontsize)
        self.ax.legend(fontsize=self.style.fontsize, ncol=2, loc="upper right")
        self.ax.tick_params(axis='x', labelsize=self.style.fontsize)
        self.ax.tick_params(axis='y', labelsize=self.style.fontsize)
        self.ax.set_xlim(self.style.opts_axes["xedges"][0], self.style.opts_axes["xedges"][-1])
        if self.log:
            self.ax.set_yscale("log")
            if self.is_mc_only:
                exp = math.floor(math.log(max(stacks["mc_nominal_sum"].values()), 10))
            else:
                exp = math.floor(math.log(max(stacks["data_sum"].values()), 10))
            self.ax.set_ylim((0.01, 10 ** (exp + 3)))
        else:
            if self.is_mc_only:
                reference_shape = stacks["mc_nominal_sum"].values()
            else:
                reference_shape = stacks["data_sum"].values()
            if self.density:
                integral = sum(reference_shape) * np.array(self.style.opts_axes["xbinwidth"])
                reference_shape = reference_shape / integral
            ymax = max(reference_shape)
            if not np.isnan(ymax):
                self.ax.set_ylim((0, 2.0 * ymax))
        if ratio:
            self.ax.set_xlabel("")
            self.rax.set_xlabel(self.style.opts_axes["xlabel"], fontsize=self.style.fontsize)
            self.rax.set_ylabel("Data / MC", fontsize=self.style.fontsize)
            self.rax.yaxis.set_label_coords(-0.075, 1)
            self.rax.tick_params(axis='x', labelsize=self.style.fontsize)
            self.rax.tick_params(axis='y', labelsize=self.style.fontsize)
            self.rax.set_ylim((0.5, 1.5))
        if self.style.has_labels:
            handles, labels = self.ax.get_legend_handles_labels()
            labels_new = []
            handles_new = []
            for i, l in enumerate(labels):
                if l in self.style.labels_mc:
                    labels_new.append(f"{self.style.labels_mc[l]}")
                else:
                    labels_new.append(l)
                handles_new.append(handles[i])
            labels = labels_new
            handles = handles_new
            self.ax.legend(
                handles,
                labels,
                fontsize=self.style.fontsize,
                ncol=2,
                loc="upper right",
            )

    def plot_mc(self, cat, ax=None):
        '''Plots the MC histograms as a stacked plot.'''
        stacks = self._get_stacks(cat)
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_mc` will be skipped.")
            return

        if ax:
            self.ax = ax
        else:
            if not hasattr(self, "ax"):
                self.define_figure(ratio=False)
        stacks["mc_nominal"].plot(
            ax=self.ax, color=self.colors, density=self.density, **self.style.opts_mc
        )
        self.format_figure(cat, ratio=False)

    def plot_data(self, cat, ax=None):
        '''Plots the data histogram as an errorbar plot.'''
        stacks = self._get_stacks(cat)
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_data` will be skipped.")
            return

        if ax:
            self.ax = ax
        else:
            if not hasattr(self, "ax"):
                self.define_figure(ratio=False)
        y = stacks["data_sum"].values()
        yerr = np.sqrt(y)
        integral = sum(y) * np.array(self.style.opts_axes["xbinwidth"])
        if self.density:
            y = y / integral
            yerr = yerr / integral
        self.ax.errorbar(self.style.opts_axes["xcenters"], y, yerr=yerr, **self.style.opts_data)
        self.format_figure(cat, ratio=False)

    def plot_datamc_ratio(self, cat, ax=None):
        '''Plots the Data/MC ratio as an errorbar plot.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_datamc_ratio` will be skipped.")
            return

        ratio, ratio_unc = self.get_datamc_ratio(cat)
        if ax:
            self.rax = ax
        else:
            if not hasattr(self, "rax"):
                self.define_figure(ratio=True)
        self.rax.errorbar(
            self.style.opts_axes["xcenters"], ratio, yerr=ratio_unc, **self.style.opts_data
        )
        self.format_figure(cat, ratio=True)

    def plot_systematic_uncertainty(self, cat, ratio=False, ax=None):
        '''Plots the asymmetric systematic uncertainty band on top of the MC stack, if `ratio` is set to False.
        To plot the systematic uncertainty in a ratio plot, `ratio` has to be set to True and the uncertainty band will be plotted around 1 in the ratio plot.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_systematic_uncertainty` will be skipped.")
            return

        if ax:
            self.ax = ax
            if ratio:
                self.rax = ax
        else:
            if not hasattr(self, "ax"):
                self.define_figure(ratio=ratio)
        if ratio:
            # In order to get a consistent uncertainty band, the up/down variations of the ratio are set to 1 where the nominal value is 0
            ax = self.rax
            up = self.syst_manager.total(cat).ratio_up
            down = self.syst_manager.total(cat).ratio_down
        else:
            ax = self.ax
            up = self.syst_manager.total(cat).up
            down = self.syst_manager.total(cat).down

        unc_band = np.array([down, up])
        ax.fill_between(
            self.style.opts_axes["xedges"],
            np.r_[unc_band[0], unc_band[0, -1]],
            np.r_[unc_band[1], unc_band[1, -1]],
            **self.style.opts_unc['total'],
            label="syst. unc.",
        )
        if ratio:
            ax.hlines(
                1.0, *ak.Array(self.style.opts_axes["xedges"])[[0, -1]], colors='gray', linestyles='dashed'
            )

    def plot_datamc(self, cat, ratio=True, syst=True, ax=None, rax=None):
        '''Plots the data histogram as an errorbar plot on top of the MC stacked histograms.
        If ratio is True, also the Data/MC ratio plot is plotted.
        If syst is True, also the total systematic uncertainty is plotted.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_datamc` will be skipped.")
            return

        if ratio:
            if self.is_mc_only:
                raise Exception(
                    "The Data/MC ratio cannot be plotted if the histogram is MC only."
                )
            if self.is_data_only:
                raise Exception(
                    "The Data/MC ratio cannot be plotted if the histogram is Data only."
                )

        if ax:
            self.ax = ax
        if rax:
            self.rax = rax
        if (not self.is_mc_only) & (not self.is_data_only):
            self.plot_mc(cat)
            self.plot_data(cat)
            if syst:
                self.plot_systematic_uncertainty(cat)
        elif self.is_mc_only:
            self.plot_mc(cat)
            if syst:
                self.plot_systematic_uncertainty(cat)
        elif self.is_data_only:
            self.plot_data(cat)

        if ratio:
            self.plot_datamc_ratio(cat)
            if syst:
                self.plot_systematic_uncertainty(cat, ratio=ratio)

        self.format_figure(cat, ratio=ratio)

    def plot_datamc_all(self, ratio=True, syst=True, spliteras=False, save=True):
        '''Plots the data and MC histograms for each year and category contained in the histograms.
        If ratio is True, also the Data/MC ratio plot is plotted.
        If syst is True, also the total systematic uncertainty is plotted.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_datamc_all` will be skipped.")
            return

        for cat in self.categories:
            if self.only_cat and cat not in self.only_cat:
                continue
            self.define_figure(ratio)
            self.plot_datamc(cat, ratio=ratio, syst=syst)
            if save:
                plot_dir = os.path.join(self.plot_dir, cat)
                if self.log:
                    filepath = os.path.join(plot_dir, f"log_{self.name}_{cat}.png")
                else:
                    filepath = os.path.join(plot_dir, f"{self.name}_{cat}.png")
                print("Saving", filepath)
                plt.savefig(filepath, dpi=150, format="png")
            else:
                plt.show(self.fig)
            plt.close(self.fig)


class SystManager:
    '''This class handles the systematic uncertainties of 1D MC histograms.'''

    def __init__(self, shape: Shape, style: Style, has_mcstat=True) -> None:
        self.shape = shape
        self.style = style
        assert all(
            [
                (var == "nominal") | var.endswith(("Up", "Down"))
                for var in self.shape.variations
            ]
        ), "All the variations names that are not 'nominal' must end in 'Up' or 'Down'."
        self.variations_up = [
            var for var in self.shape.variations if var.endswith("Up")
        ]
        self.variations_down = [
            var for var in self.shape.variations if var.endswith("Down")
        ]
        assert len(self.variations_up) == len(
            self.variations_down
        ), "The number of up and down variations is mismatching."
        self.systematics = [s.split("Up")[0] for s in self.variations_up]
        if has_mcstat:
            self.systematics.append("mcstat")
        self.syst_dict = defaultdict(dict)

    def update(self):
        '''Updates the dictionary of systematic uncertainties with the new cached stacks.'''
        for cat, stacks in self.shape._stacksCache.items():
            for syst_name in self.systematics:
                self.syst_dict[cat][syst_name] = SystUnc(self.shape.style, stacks, syst_name)

    def total(self, cat):
        return SystUnc(style = self.style, name="total", syst_list=list(self.syst_dict[cat].values()))

    def mcstat(self, cat):
        return self.syst_dict[cat]["mcstat"]

    def get_syst(self, syst_name: str, cat: str):
        '''Returns the SystUnc object corresponding to a given systematic uncertainty,
        passed as the argument `syst_name` and for a given category, passed as the argument `cat`.'''
        return self.syst_dict[cat][syst_name]


class SystUnc:
    '''This class stores the information of a single systematic uncertainty of a 1D MC histogram.
    The built-in __add__() method implements the sum in quadrature of two systematic uncertainties,
    returning a `SystUnc` instance corresponding to their sum in quadrature.'''

    def __init__(
            self, style: Style, stacks: dict = None, name: str = None, syst_list: list = None
    ) -> None:
        self.name = name
        self.is_mcstat = self.name == "mcstat"

        # Initialize the arrays of the nominal yield and squared errors as 0
        self.nominal = 0.0
        self.err2_up = 0.0
        self.err2_down = 0.0
        self.style = style
        if stacks:
            if syst_list:
                raise Exception(
                    "The initialization of the instance is ambiguous. Either a `Shape` object or a list of `SystUnc` objects should be passed to the constructor."
                )
            else:
                self.syst_list = [self]
            self._get_err2(stacks)
            # Full nominal MC including all MC samples
            self.h_mc_nominal = stacks["mc_nominal_sum"]
            self.nominal = stacks["mc_nominal_sum"].values()
        elif syst_list:
            self.syst_list = syst_list
            assert (
                self.nsyst > 0
            ), "Attempting to initialize a `SystUnc` instance with an empty list of systematic uncertainties."
            assert not (
                (self._n_empty == 1) & (self.nsyst == 1)
            ), "Attempting to intialize a `SystUnc` instance with an empty systematic uncertainty."
            _syst = self.syst_list[0]
            self.h_mc_nominal = _syst.h_mc_nominal
            self.nominal = _syst.nominal
            self._get_err2_from_syst()

    def __add__(self, other):
        '''Sum in quadrature of two systematic uncertainties.
        In case multiple objects are summed, the information on the systematic uncertainties that
        have been summed is stored in self.syst_list.'''
        return SystUnc(style=self.style, name=f"{self.name}_{other.name}", syst_list=[self, other])

    @property
    def up(self):
        return self.nominal + np.sqrt(self.err2_up)

    @property
    def down(self):
        return self.nominal - np.sqrt(self.err2_down)

    @property
    def ratio_up(self):
        return np.where(self.nominal != 0, self.up / self.nominal, 1)

    @property
    def ratio_down(self):
        return np.where(self.nominal != 0, self.down / self.nominal, 1)

    @property
    def nsyst(self):
        return len(self.syst_list)

    @property
    def _is_empty(self):
        return np.sum(self.nominal) == 0

    @property
    def _n_empty(self):
        return len([s for s in self.syst_list if s._is_empty])

    def _get_err2_from_syst(self):
        '''Method used in the constructor to instanstiate a SystUnc object from
        a list of SystUnc objects. The sytematic uncertainties in self.syst_list,
        are summed in quadrature to define a new SystUnc object.'''
        if not self._is_empty:
            index_non_empty = [i for i, s in enumerate(self.syst_list) if not s._is_empty][0]
        else:
            raise Exception(' '.join([f"The systematic uncertainty `{self.name}` is empty.",
                                    "Please check the histogram definition. If a histogram is expected to be empty in a given category, the category can be excluded with the option `only_cat`."]))
        self.nominal = self.syst_list[index_non_empty].nominal
        for syst in self.syst_list:
            if not ((self._is_empty) | (syst._is_empty)):
                assert all(
                    np.equal(self.nominal, syst.nominal)
                ), "Attempting to sum systematic uncertainties with different nominal MC."
                assert all(
                    np.equal(self.style.opts_axes["xcenters"], syst.style.opts_axes["xcenters"])
                ), "Attempting to sum systematic uncertainties with different bin centers."
            self.err2_up += syst.err2_up
            self.err2_down += syst.err2_down

    def _get_err2(self, stacks):
        '''Method used in the constructor to instanstiate a SystUnc object from
        a Shape object. The corresponding up/down squared uncertainties are stored and take
        into account the possibility for the uncertainty to be one-sided.'''
        # Loop over all the MC samples and sum the systematic uncertainty in quadrature
        for h in stacks["mc"]:
            # Nominal variation for a single MC sample
            h_nom = h[{'variation': 'nominal'}]
            nom = h_nom.values()
            # Sum in quadrature of mcstat
            if self.is_mcstat:
                mcstat_err2 = h_nom.variances()
                self.err2_up += mcstat_err2
                self.err2_down += mcstat_err2
                continue
            # Up/down variations for a single MC sample
            var_up = h[{'variation': f'{self.name}Up'}].values()
            var_down = h[{'variation': f'{self.name}Down'}].values()
            # Compute the uncertainties corresponding to the up/down variations
            err_up = var_up - nom
            err_down = var_down - nom
            # Compute the flags to check which of the two variations (up and down) are pushing the nominal value up and down
            up_is_up = err_up > 0
            down_is_down = err_down < 0
            # Compute the flag to check if the uncertainty is one-sided, i.e. when both variations are up or down
            is_onesided = up_is_up ^ down_is_down

            # Sum in quadrature of the systematic uncertainties taking into account if the uncertainty is one- or double-sided
            err2_up_twosided = np.where(up_is_up, err_up**2, err_down**2)
            err2_down_twosided = np.where(up_is_up, err_down**2, err_up**2)
            err2_max = np.maximum(err2_up_twosided, err2_down_twosided)
            err2_up_onesided = np.where(is_onesided & up_is_up, err2_max, 0)
            err2_down_onesided = np.where(is_onesided & down_is_down, err2_max, 0)
            err2_up_combined = np.where(is_onesided, err2_up_onesided, err2_up_twosided)
            err2_down_combined = np.where(
                is_onesided, err2_down_onesided, err2_down_twosided
            )
            # Sum in quadrature of the systematic uncertainty corresponding to a MC sample
            self.err2_up += err2_up_combined
            self.err2_down += err2_down_combined

    def plot(self, ax=None):
        '''Plots the nominal, up and down systematic variations on the same plot.'''
        plt.style.use([hep.style.ROOT, {'font.size': self.style.fontsize}])
        plt.rcParams.update({'font.size': self.style.fontsize})
        if not ax:
            self.fig, self.ax = plt.subplots(1, 1, **self.style.opts_figure["systematics"])
        else:
            self.ax = ax
            self.fig = self.ax.get_figure
        hep.cms.text(
            "Simulation Preliminary", fontsize=self.style.fontsize, loc=0, ax=self.ax
        )
        # hep.cms.lumitext(text=f'{self.lumi[year]}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=self.style.fontsize, ax=self.ax)
        self.ax.hist(
            self.style.opts_axes["xcenters"],
            weights=self.nominal,
            histtype="step",
            label="nominal",
            **self.style.opts_syst["nominal"],
        )
        self.ax.hist(
            self.style.opts_axes["xcenters"],
            weights=self.up,
            histtype="step",
            label=f"{self.name} up",
            **self.style.opts_syst["up"],
        )
        self.ax.hist(
            self.style.opts_axes["xcenters"],
            weights=self.down,
            histtype="step",
            label=f"{self.name} down",
            **self.style.opts_syst["down"],
        )
        self.ax.set_xlabel(self.style.opts_axes["xlabel"])
        self.ax.set_ylabel("Counts")
        self.ax.legend()
        return self.fig, self.ax
