import os
from copy import deepcopy
from multiprocessing import Pool
from collections import defaultdict
from functools import partial
from itertools import product
from warnings import warn

import math
import decimal
from decimal import Decimal
import numpy as np
import awkward as ak
import hist

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import mplhep as hep
from mplhep.error_estimation import poisson_interval
from cycler import cycler

from omegaconf import OmegaConf
from pocket_coffea.parameters.defaults import merge_parameters, get_default_parameters

np.seterr(divide="ignore", invalid="ignore", over="ignore")

plotting_style_defaults = get_default_parameters()["plotting_style"]

# colormaps according to CMS guidelines
# https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/#categorical-data-eg-1d-stackplots
CMAP_6 = hep.styles.cms.cmap_petroff
CMAP_10 = [
    "#3f90da",
    "#ffa90e",
    "#bd1f01",
    "#94a4a2",
    "#832db6",
    "#a96b59",
    "#e76300",
    "#b9ac70",
    "#717581",
    "#92dadd",
]

COLOR_ALIASES = {
    "CMS_blue": CMAP_10[0],        # Blue
    "CMS_orange": CMAP_10[1],      # Orange
    "CMS_red": CMAP_10[2],         # Red
    "CMS_gray": CMAP_10[3],        # Gray
    "CMS_purple": CMAP_10[4],      # Purple
    "CMS_brown": CMAP_10[5],       # Brown
    "CMS_dark_orange": CMAP_10[6], # Dark Orange
    "CMS_beige": CMAP_10[7],       # Beige
    "CMS_dark_gray": CMAP_10[8],   # Dark Gray
    "CMS_light_blue": CMAP_10[9]   # Light Blue
}

class Style:
    '''This class manages all the style options for Data/MC plots.'''

    def __init__(self, style_cfg) -> None:
        self.style_cfg = style_cfg
        self._required_keys = [
            "opts_figure",
            "opts_mc",
            "opts_sig",
            "opts_data",
            "opts_unc",
        ]
        self.set_defaults()
        for key, item in self.style_cfg.items():
            setattr(self, key, item)

        self.has_labels = "labels_mc" in style_cfg
        self.has_samples_groups = "samples_groups" in style_cfg
        self.has_only_samples = "only_samples" in style_cfg
        self.has_exclude_samples = "exclude_samples" in style_cfg
        self.has_rescale_samples = "rescale_samples" in style_cfg
        self.has_colors_mc = "colors_mc" in style_cfg
        self.has_signal_samples = "signal_samples" in style_cfg
        self.has_order_mc = "order_mc" in style_cfg

        self.has_blind_hists = False
        if "blind_hists" in style_cfg:
            if (
                "categories" in style_cfg["blind_hists"]
                and "histograms" in style_cfg["blind_hists"]
            ):
                if (
                    len(style_cfg["blind_hists"]["categories"]) != 0
                    and len(style_cfg["blind_hists"]["histograms"]) != 0
                ):
                    self.has_blind_hists = True

        self.has_compare_ref = False
        if "compare" in self.style_cfg:
            if "ref" in self.style_cfg["compare"]:
                self.has_compare_ref = True

        self.flow = self.opts_mc["flow"] == "sum"

    def set_defaults(self):
        # load default plotting paramters, update with user-defined values
        self.style_cfg = merge_parameters(
            plotting_style_defaults, self.style_cfg, update=True
        )

        # check if the user-defined values are valid
        for key, is_mc in zip(
            ["categorical_axes_data", "categorical_axes_mc"], [False, True]
        ):
            for subkey, val in self.style_cfg[key].items():
                if (subkey, val) not in self._available_categorical_axes(is_mc).items():
                    raise Exception(
                        f"The key `{subkey}` with value `{val}` is not a valid categorical axis for {key}. Available axes: {self._available_categorical_axes(is_mc)}"
                    )

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

    def _available_categorical_axes(self, is_mc=True):
        '''Returns the list of available categorical axes for MC or data histograms.'''
        if is_mc:
            return {
                "year" : "years",
                "cat" : "categories",
                "variation" : "variations"
            }
        else:
            return {
                "year" : "years",
                "cat" : "categories",
                "era" : "eras"
            }

class PlotManager:
    '''This class manages multiple Shape objects and their plotting.'''

    def __init__(
        self,
        variables,
        hist_objs,
        datasets_metadata,
        plot_dir,
        style_cfg,
        has_mcstat=True,
        toplabel=None,
        only_cat=None,
        only_year=None,
        workers=8,
        log_x=False,
        log_y=False,
        density=False,
        verbose=1,
        save=True,
        index_file=None,
        cache=True
    ) -> None:

        self.shape_objects = {}
        self.plot_dir = plot_dir
        self.only_cat = only_cat
        self.only_year = only_year
        self.workers = workers
        self.log_x = log_x
        self.log_y = log_y
        self.density = density
        self.save = save
        self.index_file = index_file
        self.nhists = len(variables)
        self.toplabel = toplabel
        self.verbose=verbose
        self.cache = cache

        # Reading the datasets_metadata to
        # build the correct shapes for each datataking year
        # taking histo objects from hist_objs
        self.hists_to_plot = {}
        for variable in variables:
            vs = {}
            for year, samples in datasets_metadata["by_datataking_period"].items():
                if self.only_year and year not in self.only_year:
                    continue
                hs = {}
                for sample, datasets in samples.items():
                    hs[sample] = {}
                    for dataset in datasets:
                        try:
                            hs[sample][dataset] = hist_objs[variable][sample][dataset]
                        except:
                            print(f"Warning: missing dataset {dataset} for variable {variable}, year {year}")
                    if len(hs[sample].keys()) == 0:
                        del hs[sample]
                vs[year] = hs
            self.hists_to_plot[variable] = vs
        
        for variable, histoplot in self.hists_to_plot.items():
            for year, h_dict in histoplot.items():
                if self.only_year and year not in self.only_year:
                    continue
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
                    log_x=self.log_x,
                    log_y=self.log_y,
                    density=self.density,
                    has_mcstat=has_mcstat,
                    toplabel=toplabel_to_use,
                    year=year,
                    verbose=self.verbose,
                    cache=self.cache
                )
        del self.hists_to_plot

        if self.save:
            self.make_dirs()
            if self.index_file is not None:
                self.copy_index_file()
            matplotlib.use('agg')

    def make_dirs(self):
        '''Create directories recursively before saving plots with multiprocessing
        to avoid conflicts between different processes.'''
        for name, shape in self.shape_objects.items():
            for cat in shape.categories:
                if self.only_cat and cat not in self.only_cat:
                    continue
                plot_dir = os.path.join(self.plot_dir, cat)
                if not os.path.exists(plot_dir):
                    os.makedirs(plot_dir)

    def copy_index_file(self):
        '''Copy the specified index file to the plot directory and each of the subdirectories.'''
        if not os.path.exists(self.index_file):
            raise Exception(f"Index file {self.index_file} not found.")
        os.system(f"cp {self.index_file} {self.plot_dir}")
        for name, shape in self.shape_objects.items():
            for cat in shape.categories:
                if self.only_cat and cat not in self.only_cat:
                    continue
                plot_dir = os.path.join(self.plot_dir, cat)
                os.system(f"cp {self.index_file} {plot_dir}")

    def plot_datamc(self, name, ratio=True, syst=True, spliteras=False, format="png"):
        '''Plots one histogram, for all years and categories.'''
        if self.verbose>0:
            print("Plotting: ", name)
        shape = self.shape_objects[name]
        if shape.dense_dim > 1:
            if self.verbose>0:
                print(f"WARNING: cannot plot data/MC for histogram {shape.name} with dimension {shape.dense_dim}.")
                print("The method `plot_datamc` will be skipped.")
            return

        if ((shape.is_mc_only) | (shape.is_data_only)):
            ratio = False
        else:
            ratio = ratio
        shape.plot_datamc_all(ratio, syst, spliteras=spliteras, save=self.save, format=format)

    def plot_datamc_all(self, ratio=True, syst=True,  spliteras=False, format="png"):
        '''Plots all the histograms contained in the dictionary, for all years and categories.'''
        shape_names = list(self.shape_objects.keys())
        if self.workers > 1:
            with Pool(processes=self.workers) as pool:
                # Parallel calls of plot_datamc() on different shape objects
                pool.map(partial(self.plot_datamc, ratio=ratio, syst=syst, spliteras=spliteras, format=format), shape_names)
                pool.close()
        else:
            for shape in shape_names:
                self.plot_datamc(shape, ratio=ratio, syst=syst, spliteras=spliteras, format=format)

    def plot_comparison(self, name, ratio=True, format="png"):
        '''Plots one histogram, for all years and categories.'''
        if self.verbose>0:
            print("Plotting: ", name)
        shape = self.shape_objects[name]
        if shape.dense_dim > 1:
            if self.verbose>0:
                print(f"WARNING: cannot plot histogram {shape.name} with dimension {shape.dense_dim}. It will be skipped")
            return

        shape.plot_comparison_all(ratio,  save=self.save, format=format)

    def plot_comparison_all(self, ratio=True, format=format):
        '''Plots all the histograms contained in the dictionary, for all years and categories.'''
        shape_names = list(self.shape_objects.keys())
        if self.workers > 1:
            with Pool(processes=self.workers) as pool:
                pool.map(partial(self.plot_comparison, ratio=ratio, format=format), shape_names)
                pool.close()
        else:
            for shape in shape_names:
                self.plot_comparison(shape, ratio=ratio, syst=syst, spliteras=spliteras, format=format)


    def plot_systematic_shifts(self, shape, format="png", ratio=True):
        """Plots the systematic shifts for all the variations."""
        if self.verbose > 0:
            print("Plotting systematic shifts for:", shape.name)
        if shape.dense_dim > 1:
            print(
                f"WARNING: cannot plot systematic shifts for histogram {shape.name}"
                f"with dimension {shape.dense_dim}."
            )
            print("The method `plot_systematic_shifts` will be skipped.")
            return
        if shape.is_data_only:
            raise Exception(
                "The systematic shifts cannot be plotted if the histogram is Data only."
            )
        shape.plot_systematic_shifts_all(ratio=ratio, format=format)

    def plot_systematic_shifts_all(self, format="png", ratio=True):
        """Plots the systematic shifts for all the shape objects."""
        if self.workers > 1:
            with Pool(processes=self.workers) as pool:
                # Parallel calls of plot_systematic_shifts() on different shape objects
                pool.map(
                    partial(self.plot_systematic_shifts, format=format, ratio=ratio),
                    self.shape_objects.values(),
                )
                pool.close()
        else:
            for shape in self.shape_objects.values():
                self.plot_systematic_shifts(shape, format=format, ratio=ratio)

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
        has_mcstat=True,
        toplabel=None,
        only_cat=None,
        log_x=False,
        log_y=False,
        density=False,
        year = None,
        verbose=1,
        cache=True
    ) -> None:
        self.h_dict = h_dict
        self.name = name
        self.plot_dir = plot_dir
        self.only_cat = only_cat if only_cat is not None else []
        self.style = Style(style_cfg)
        self.has_mcstat = has_mcstat
        self.toplabel = toplabel if toplabel else ""
        self.log_x = log_x
        self.log_y = log_y
        self.density = density
        self.datasets_metadata=datasets_metadata
        self.sample_is_MC = {}
        self.year=year
        self.verbose = verbose
        self.cache = cache
        self._stacksCache = defaultdict(dict)
        assert (
            type(h_dict) in [dict, defaultdict]
        ), "The Shape object receives a dictionary of hist.Hist objects as argument."
        self.group_samples()
        self.is_mc_only = len(self.samples_data) == 0
        self.is_data_only = len(self.samples_mc) == 0
        self.filter_samples()
        self.rescale_samples()
        if not self.is_data_only:
            self.replace_missing_variations()
        self.load_attributes()
        self.load_syst_manager()

    def load_attributes(self):
        '''Loads the attributes from the dictionary of histograms.'''
        if self.verbose>1:
            print(self.h_dict)
            print("samples:", self.samples_mc)

        #self.signal_samples =  self.style.signal_samples


        if not self.is_data_only:
            assert len(
                set([self.h_dict[s].ndim for s in self.samples_mc])
            ), f"{self.name}: Not all the MC histograms have the same dimension."
            # Load categorical axes for MC histograms
            for ax in self.categorical_axes_mc:
                setattr(
                    self,
                    self.style.categorical_axes_mc[
                        ax.name
                    ],
                    self.get_axis_items(ax.name, is_mc=True),
                )
        if not self.is_mc_only:
            assert len(
                set([self.h_dict[s].ndim for s in self.samples_data])
            ), f"{self.name}: Not all the data histograms have the same dimension."
            # Load categorical axes for data histograms
            for ax in self.categorical_axes_data:
                setattr(
                    self,
                    self.style.categorical_axes_data[
                        ax.name
                    ],
                    self.get_axis_items(ax.name, is_mc=False),
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

    def load_syst_manager(self):
        '''Loads the attributes from the dictionary of histograms.'''
        if self.verbose>1:
            print(self.h_dict)
            print("samples:", self.samples_mc)

        self.is_data_only = len(self.samples_mc) == 0

        if not self.is_data_only:
            self.syst_manager = SystManager(self, self.style, verbose=self.verbose)

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

    def replace_missing_variations(self):
        '''Replaces the missing categories in the MC histograms with the nominal values.'''
        d = {s: v for s, v in self.h_dict.items() if s in self.samples_mc}
        h0 = self.h_dict[list(d.keys())[0]]
        # Define the categorical axes dict with the categorical axis name as key and the list of available categories as value
        categorical_axes_dict = {axis_name : set() for axis_name in [ax.name for ax in h0.axes if type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]]}

        # Save the union of the categories for each axis across samples
        for axis_name in categorical_axes_dict.keys():
            for s, h in d.items():
                ax = [ax for ax in h.axes if ax.name == axis_name][0]
                categorical_axes_dict[axis_name] |= {ax.value(i) for i in range(len(ax))}

        categories_sorted = {}
        for axis_name in categorical_axes_dict.keys():
            for s, h in d.items():
                ax = [ax for ax in h.axes if ax.name == axis_name][0]
                if len(ax) == len(categorical_axes_dict[axis_name]):
                    categories_sorted[axis_name] = [ax.value(i) for i in range(len(ax))]

        # Use the union of sets to define categorical_axes
        for i, (axis_name, categories) in enumerate(categorical_axes_dict.items()):
            for s, h in d.items():
                ax = [ax for ax in h.axes if ax.name == axis_name][0]
                categories_per_sample = {ax.value(i) for i in range(len(ax))}
                if categories_per_sample != categories:
                    if len(categorical_axes_dict) != 2:
                        raise NotImplementedError("The number of categorical axes is different from 2. This case is not implemented yet. Only the axes `cat` and `variation` are supported.")
                    if not axis_name == "variation":
                        raise NotImplementedError(f"The axis `variation` is the only axis that could differ across samples. The axis `{axis_name}` is not supported.")
                    categories_missing = categories - categories_per_sample
                    # Copy the histogram h and extend the axis including the missing categories
                    axes_to_copy = [ax for ax in h.axes if ax.name != axis_name]
                    axis_name_other = list(categorical_axes_dict.keys())[1-i]
                    axis_other = [ax for ax in h.axes if ax.name == axis_name_other][0]
                    if type(ax) == hist.axis.StrCategory:
                        axis_new = hist.axis.StrCategory(categories_sorted[axis_name], name=axis_name, label=ax.label)
                    elif type(ax) == hist.axis.IntCategory:
                        axis_new = hist.axis.IntCategory(categories_sorted[axis_name], name=axis_name, label=ax.label)
                    # Create new histogram with the missing categories
                    new_hist = hist.Hist(axis_other, axis_new, *self.dense_axes, storage=h._storage_type)
                    new_hist_view = new_hist.view()
                    warn_msg = f"WARNING: Sample {s} is missing variations in the axis `{axis_name}`. Filling the {axis_name} with nominal values.\nMissing variations: {categories_missing}"
                    warn_flag = False
                    for category_other in categorical_axes_dict[axis_name_other]:
                        index_other = axis_other.index(category_other)
                        for category in categories:
                            index_category = axis_new.index(category)
                            # Fill the missing categories with the nominal values
                            if category in categories_missing:
                                warn_flag = True
                                new_hist_view[index_other, index_category, :] = h[{axis_name_other: category_other, axis_name: "nominal"}].view()
                            else:
                                new_hist_view[index_other, index_category, :] = h[{axis_name_other: category_other, axis_name: category}].view()
                    if warn_flag:
                        print(warn_msg)
                    self.h_dict[s] = new_hist

    def _categorical_axes(self, is_mc=True):
        '''Returns the list of categorical axes of a histogram.'''
        # Since MC and data have different categorical axes, the argument mc needs to specified
        if is_mc:
            d = {s: v for s, v in self.h_dict.items() if s in self.samples_mc}
        else:
            d = {s: v for s, v in self.h_dict.items() if s in self.samples_data}
        h0 = self.h_dict[list(d.keys())[0]]
        # Define the categorical axes dict with the categorical axis name as key and the list of available categories as value
        categorical_axes_dict = {axis_name : set() for axis_name in [ax.name for ax in h0.axes if type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]]}

        # Save the union of the categories for each axis across samples
        for axis_name in categorical_axes_dict.keys():
            for s, h in d.items():
                ax = [ax for ax in h.axes if ax.name == axis_name][0]
                categorical_axes_dict[axis_name] |= {ax.value(i) for i in range(len(ax))}

        # Use the union of sets to define categorical_axes
        #categorical_axes = list(categorical_axes_dict.values())
        error_msg = f"The Shape object `{self.name}` contains histograms with different categorical axes in the %1 datasets.\nMismatching axes:\n"
        if is_mc:
            error_msg = error_msg.replace("%1", "MC")
        else:
            error_msg = error_msg.replace("%1", "Data")
        for i, (axis_name, categories) in enumerate(categorical_axes_dict.items()):
            for s, h in d.items():
                ax = [ax for ax in h.axes if ax.name == axis_name][0]
                categories_per_sample = {ax.value(i) for i in range(len(ax))}
                if categories_per_sample != categories:
                    if not is_mc:
                        raise NotImplementedError("The data histograms have different categories. This case is not implemented yet.")
                    if len(categorical_axes_dict) != 2:
                        raise NotImplementedError("The number of categorical axes is different from 2. This case is not implemented yet. Only the axes `cat` and `variation` are supported.")
                    if not axis_name == "variation":
                        raise NotImplementedError(f"The axis `variation` is the only axis that could differ across samples. The axis `{axis_name}` is not supported.")
                    categories_missing = categories - categories_per_sample
                    error_msg += f"{axis_name}\n" + f"Missing categories: {categories_missing}"
                    raise Exception(error_msg)

        h0 = self.h_dict[list(d.keys())[0]]
        categorical_axes = [ax for ax in h0.axes if type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]]

        return categorical_axes

    @property
    def categorical_axes_mc(self):
        '''Returns the list of categorical axes of a MC histogram.'''
        return self._categorical_axes(is_mc=True)

    @property
    def categorical_axes_data(self):
        '''Returns the list of categorical axes of a data histogram.'''
        return self._categorical_axes(is_mc=False)

    @property
    def dense_dim(self):
        '''Returns the number of dense axes of a histogram.'''
        return len(self.dense_axes)

    def get_axis_items(self, axis_name, is_mc):
        '''Returns the list of values contained in a Hist axis.'''
        if is_mc:
            axis = [ax for ax in self.categorical_axes_mc if ax.name == axis_name][0]
        else:
            axis = [ax for ax in self.categorical_axes_data if ax.name == axis_name][0]
        return list(axis.value(range(axis.size)))

    def _stack_sum(self, cat=None, is_mc=None, stack=None):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of histograms.'''
        if not stack:
            if not cat:
                raise Exception("The category `cat` should be passed when the `stack` option is not specified.")
            if is_mc:
                stack = self._get_stacks(cat)["mc"]
            else:
                stack = self._get_stacks(cat)["data"]
        if len(stack) == 1:
            return stack[0]
        else:
            htot = hist.Hist(stack[0])
            for h in stack[1:]:
                htot = htot + h
            if is_mc:
                htot.name = 'mc_sum'
            else:
                htot.name = 'data_sum'
            return htot

    @property
    def stack_sum_data(self, cat):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of data histograms.'''
        return self._stack_sum(cat, is_mc=False)

    @property
    def stack_sum_mc_nominal(self, cat):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of MC histograms.'''
        return self._stack_sum(cat, is_mc=True)

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
                #print("Grouping:", sample, datasets)
                self.h_dict[sample] = self._stack_sum(
                    stack=hist.Stack.from_dict(
                        {s: h for s, h in datasets.items() if s in datasets}
                    )
                )
                isMC = None
                for dataset in datasets:
                    if dataset not in self.datasets_metadata:
                        raise Exception(f"Dataset `{dataset}` not found in datasets metadata!")
                    isMC_d = self.datasets_metadata[dataset]["isMC"] == "True"
                    if isMC is None:
                        isMC = isMC_d
                        self.sample_is_MC[sample] = isMC
                    elif isMC != isMC_d:
                        raise Exception("You are collapsing together data and MC histogram!")

        else:
            raise NotImplementedError("Plotting histograms without collapsing is still not implemented")

        if not self.style.has_samples_groups:
            return
        h_dict_grouped = {}
        samples_in_map = []

        cleaned_samples_list = self.style.samples_groups.copy()
        for sample_new, samples_list in self.style.samples_groups.items():
            #print(sample_new, samples_list)
            for s_to_group in samples_list:
                if s_to_group not in self.h_dict.keys():
                    if self.verbose>=1:
                        print(f"{self.name}: WARNING. Sample ",s_to_group," is not in the list of samples: ", list(self.h_dict.keys()), "Skipping it.")
                    cleaned_samples_list[sample_new].remove(s_to_group)
                    continue
                if self.verbose>=1:
                    print(f"\t {self.name}: Sample ",s_to_group," will be grouped into sample", sample_new)

        for sample_new, samples_list in cleaned_samples_list.items():
            if len(samples_list)==0:
                if self.verbose>=1:
                    print("WARNING. The list of samples to group is empty!  Group name:", sample_new)
                continue

            h_dict_grouped[sample_new] = self._stack_sum(
                stack=hist.Stack.from_dict(
                    {s: h for s, h in self.h_dict.items() if s in samples_list}
                )
            )
            isMC = self.sample_is_MC[samples_list[0]]
            self.sample_is_MC[sample_new] = self.sample_is_MC[samples_list[0]]
            samples_in_map += samples_list

        for s, h in self.h_dict.items():
            if s not in samples_in_map:
                h_dict_grouped[s] = h
        self.h_dict = deepcopy(h_dict_grouped)

    def filter_samples(self):
        '''Filters samples according to the list of samples in the style options.
        If the option `only_samples` is specified, only the samples in the list are kept.
        If the option `exclude_samples` is specified, the samples in the list are removed.
        If both options are specified, the samples in the list `only_samples` are kept, provided they are not in the list `exclude_samples`.
        '''
        if not any([self.style.has_only_samples, self.style.has_exclude_samples]):
            return
        h_dict_filtered = {}
        for s, h in self.h_dict.items():
            if self.style.has_only_samples and self.style.has_exclude_samples:
                if s in self.style.only_samples and s not in self.style.exclude_samples:
                    h_dict_filtered[s] = h
            elif self.style.has_only_samples:
                if s in self.style.only_samples:
                    h_dict_filtered[s] = h
            elif self.style.has_exclude_samples:
                if s not in self.style.exclude_samples:
                    h_dict_filtered[s] = h
        self.h_dict = deepcopy(h_dict_filtered)

    def rescale_samples(self):
        if not self.style.has_rescale_samples:
            return
        for sample,scale in self.style.rescale_samples.items():

            if sample in self.h_dict.keys():
                if self.verbose>=2:
                    print("Rescaling hist", self.h_dict[sample].name, "in sample",sample, " by ", scale)
                self.h_dict[sample] = self.h_dict[sample]*scale
            else:
                if self.verbose>0:
                    print("Warning: the rescaling sample is not among the samples in the histograms. Nothing will be rescaled! ")
                    print("\t Rescale requested for:", sample, ";  hists exist:", self.h_dict.keys())

    def _get_stacks(self, cat, spliteras=False):
        '''Builds the data and MC stacks, applying a slicing by category.
        The stacks are cached in a dictionary so that they are not recomputed every time.
        If spliteras is True, the extra axis "era" is kept in the data stack to
        distinguish between data samples from different data-taking eras.'''
        if not cat in self._stacksCache:
            stacks = {}
            if not self.is_data_only:
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
                # create cycler from the colormap and instantiate it to get iterator
                cmap = CMAP_6 if len(self.nevents) <= 6 else CMAP_10
                if len(self.nevents) > 10 and self.verbose>0:
                    print(
                        "Warning: more than 10 samples to plot. "
                        "No official CMS colormap for that case"
                    )
                color = cycler("color", cmap)()
                # Assign colors from cycle to samples
                self.colors = {sample: next(color)["color"] for sample in self.nevents}

                # Order the events dictionary by decreasing number of events if linear scale, increasing if log scale
                reverse = True
                if self.log_y:
                    reverse = False
                self.nevents = dict(
                    sorted(self.nevents.items(), key=lambda x: x[1], reverse=reverse)
                )
                # If the order of MC samples is specified in the plotting style, move the sample to the beginning of the dictionary
                if self.style.has_order_mc:
                    if self.log_y:
                        nevents_new =  {k : val for k, val in self.nevents.items() if k not in self.style.order_mc}
                        nevents_new.update({k : val for k, val in self.nevents.items() if k in self.style.order_mc})
                        self.nevents = nevents_new
                    else:
                        for sample in reversed(self.style.order_mc):
                            if sample in self.nevents:
                                self.nevents = {sample: self.nevents[sample], **self.nevents}

                # order colors accordingly
                self.colors = {sample: self.colors[sample] for sample in self.nevents}

                if self.style.has_colors_mc:
                    # Initialize random colors
                    for d in self.nevents:
                        # If the color for a corresponding sample exists in the dictionary, assign the color to the sample
                        if d in self.style.colors_mc:
                            color_custom = self.style.colors_mc[d]
                            # Check if the color is an alias
                            if color_custom in COLOR_ALIASES:
                                color_custom = COLOR_ALIASES[color_custom]
                            else:
                                try:
                                    # Check if the color is a valid color
                                    matplotlib.colors.to_rgba(color_custom)
                                except ValueError:
                                    raise ValueError(
                                        f"Invalid color `{color_custom}` for sample `{d}`. Available aliases: {list(COLOR_ALIASES.keys())}"
                                    )
                            self.colors[d] = color_custom

                # Order the MC dictionary by number of events
                h_dict_mc = {d: h_dict_mc[d] for d in self.nevents.keys()}
                h_dict_mc_nominal = {
                    d: h_dict_mc_nominal[d] for d in self.nevents.keys()
                }
                # Build MC stack with variations and nominal MC stack
                stacks["mc"] = hist.Stack.from_dict(h_dict_mc)
                stacks["mc_nominal"] = hist.Stack.from_dict(h_dict_mc_nominal)
                stacks["mc_nominal_sum"] = self._stack_sum(stack = stacks["mc_nominal"])
                if any(np.isnan(stacks["mc_nominal_sum"].values())):
                    raise ValueError(f"NaN values found in the nominal MC stack of histogram {self.name} for category {cat}.")

            if not self.is_mc_only:
                # Sum over eras if specified as extra argument
                if 'era' in [ax.name for ax in self.categorical_axes_data]:
                    if spliteras:
                        slicing_data = {'cat': cat}
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
            if self.cache:
                self._stacksCache[cat] = stacks
            if not self.is_data_only:
                self.syst_manager.update(cat, stacks)
        else:
            stacks = self._stacksCache[cat]
        return stacks

    def _is_empty(self, cat):
        '''Checks if the data and MC stacks are empty.'''
        is_empty = True
        if not self.is_data_only:
            if sum(self._get_stacks(cat)["mc_nominal_sum"].values()) != 0:
                is_empty = False
        if not self.is_mc_only:
            if sum(self._get_stacks(cat)["data_sum"].values()) != 0:
                is_empty = False
        return is_empty

    def _merge_flow_bins(self, values):
        '''Add the underflow and overflow bins to the first and last bins, respectively.'''
        values_copy = deepcopy(values)
        if self.style.flow:
            values_copy[1] += values_copy[0]  # Merge underflow bin
            values_copy[-2] += values_copy[-1]  # Merge overflow bin
            values_copy = values_copy[1:-1]  # Remove the original underflow and overflow bins
        return values_copy

    def get_datamc_ratio(self, cat):
        '''Computes the data/MC ratio and the corresponding uncertainty.'''
        stacks = self._get_stacks(cat)
        hnum = stacks["data_sum"]
        hden = stacks["mc_nominal_sum"]

        num = self._merge_flow_bins(hnum.values(flow=self.style.flow))
        den = self._merge_flow_bins(hden.values(flow=self.style.flow))
        num_variances = self._merge_flow_bins(hnum.variances(flow=self.style.flow))
        den_variances = self._merge_flow_bins(hden.variances(flow=self.style.flow))

        if self.density:
            num_integral = sum(num * np.array(self.style.opts_axes["xbinwidth"]) )
            if num_integral > 0:
                num = num * (1./num_integral)
                num_variances = num_variances * (1./num_integral)**2
            den_integral = sum(den * np.array(self.style.opts_axes["xbinwidth"]) )
            if den_integral > 0:
                den = den * (1./den_integral)
                den_variances = den_variances * (1./den_integral)**2

        ratio = num / den
        # Blinding ratio for certain variables (if defined in plotting config)
        ratio = self.blind_hist(cat,ratio)
        # Total uncertainty propagation of num / den :
        # ratio_variance = np.power(ratio,2)*( num_variances*np.power(num, -2) + den_variances*np.power(den, -2))
        # Only the uncertainty of num (DATA) propagated:
        ratio_variance = num_variances * np.power(den, -2)

        ratio_uncert = np.abs(poisson_interval(ratio, ratio_variance) - ratio)
        ratio_uncert[np.isnan(ratio_uncert)] = np.inf

        return ratio, ratio_uncert

    def get_ref_ratios(self, cat, ref=None):
        '''Computes the ratios and the corresponding uncertainty between two processes.'''
        stacks = self._get_stacks(cat)

        #print("Everything in the Stack:", stacks.keys())
        #print("\t mc_nominal:", stacks['mc_nominal'])

        # den and hden refer to the denomerator, which is the reference histogram for ratios
        # num and hnum refer to the numerator

        if ref=='data_sum':
            hden = stacks['data_sum']
        elif ref=='mc_nominal_sum':
            hden = stacks['mc_nominal_sum']
        else:
            hden = stacks['mc_nominal'][ref]

        den = self._merge_flow_bins(hden.values(flow=self.style.flow))
        den_variances = self._merge_flow_bins(hden.variances(flow=self.style.flow))

        if self.density:
            den_integral = sum(den * np.array(self.style.opts_axes["xbinwidth"]) )
            if den_integral>0:
                den = den * (1./den_integral)
                den_variances = den_variances * (1./den_integral)**2

        ratios = {}
        ratios_unc = {}

        # Create ratios for all MC hist compared to the reference
        histograms_list = [h for h in stacks['mc_nominal'] ]

        if not self.is_mc_only:
            histograms_list.append(stacks['data_sum'])
        for hnum in histograms_list:
            #print("Process:", hnum.name, type(hnum))
            num = self._merge_flow_bins(hnum.values(flow=self.style.flow))
            num_variances = self._merge_flow_bins(hnum.variances(flow=self.style.flow))

            if self.density:
                num_integral = sum(num * np.array(self.style.opts_axes["xbinwidth"]) )
                num = num * (1./num_integral)
                num_variances = num_variances * (1./num_integral)**2

            ratio = num / den
            # Total uncertainy of num x den :
            # ratio_variance = np.power(ratio,2)*( num_variances*np.power(num, -2) + den_variances*np.power(den, -2))

            # Only the uncertainty of numerator is propagated, the reference hist uncertainty is ignored
            ratio_variance = np.power(ratio,2)*num_variances*np.power(num, -2)

            # Only the uncertainty of the denominator is propagated:
            # ratio_variance = np.power(ratio,2)*den_variances*np.power(den, -2)

            ratio_uncert = np.abs(poisson_interval(ratio, ratio_variance) - ratio)
            #ratio_uncert[np.isnan(ratio_uncert)] = np.inf

            ratios[hnum.name] = ratio
            ratios_unc[hnum.name] = ratio_uncert

        return ratios, ratios_unc

    def define_figure(self, ratio=True):
        '''Defines the figure for the Data/MC plot.
        If ratio is True, a subplot is defined to include the Data/MC ratio plot.'''
        # load CMS plotting style
        # https://cms-analysis.docs.cern.ch/guidelines/plotting/
        hep.style.use("CMS")
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
                loc=self.style.experiment_label_loc,
                ax=self.ax,
            )
        else:
            hep.cms.text(
                "Preliminary",
                fontsize=self.style.fontsize,
                loc=self.style.experiment_label_loc,
                ax=self.ax,
            )
        if self.toplabel:
            hep.cms.lumitext(
                text=self.toplabel,
                fontsize=self.style.fontsize,
                ax=self.ax,
            )
        return self.fig, axes

    def format_figure(self, cat, ratio=True, ref=None):
        '''Formats the figure's axes, labels, ticks, xlim and ylim.'''
        stacks = self._get_stacks(cat)
        ylabel = "Counts" if not self.density else "A.U."
        self.ax.set_ylabel(ylabel, fontsize=self.style.fontsize)
        self.ax.tick_params(axis='x', labelsize=self.style.fontsize)
        self.ax.tick_params(axis='y', labelsize=self.style.fontsize)
        self.ax.set_xlim(self.style.opts_axes["xedges"][0], self.style.opts_axes["xedges"][-1])
        handles, labels = self.ax.get_legend_handles_labels()
        if self.log_y:
            self.ax.set_yscale("log")
            if self.is_data_only:
                arg_log = max(stacks["data_sum"].values())
            elif self.is_mc_only:
                arg_log = max(stacks["mc_nominal_sum"].values())
            else:
                arg_log = max(
                    max(stacks["data_sum"].values()), max(stacks["mc_nominal_sum"].values())
                )
            if arg_log == 0:
                arg_log = 100
            exp = math.floor(math.log(arg_log, 10))
            y_lim_hi = self.style.opts_ylim["datamc"]["ylim_log"].get("hi", 10 ** (exp*1.75))
            self.ax.set_ylim((self.style.opts_ylim["datamc"]["ylim_log"]["lo"], y_lim_hi))
            # Revert handles and labels in logarithmic plot
            if self.is_mc_only:
                handles_to_reverse = handles[:-1]
                labels_to_reverse = labels[:-1]
                handles_last = [handles[-1]]
                labels_last = [labels[-1]]
            else:
                handles_to_reverse = handles[:-2]
                labels_to_reverse = labels[:-2]
                handles_last = handles[-2:]
                labels_last = labels[-2:]
            handles = handles_to_reverse[::-1] + handles_last
            labels = labels_to_reverse[::-1] + labels_last
        else:
            if self.is_data_only:
                reference_shape = stacks["data_sum"].values()
            elif ref!=None:
                if ref=='data_sum':
                    reference_shape = stacks['data_sum'].values()
                elif ref=='mc_nominal_sum':
                    reference_shape = stacks['mc_nominal_sum'].values()
                else:
                    reference_shape = stacks['mc_nominal'][ref].values()
            else:
                reference_shape = stacks["mc_nominal_sum"].values()
            if self.density:
                integral = sum(reference_shape) * np.array(self.style.opts_axes["xbinwidth"])
                reference_shape = reference_shape / integral
            ymax = max(reference_shape)
            if not np.isnan(ymax):
                if ymax==0: ymax=1
                self.ax.set_ylim((0, 2.0 * ymax))
        if self.log_x:
            self.ax.set_xscale("log")

        self.ax.legend(handles, labels, fontsize=self.style.fontsize_legend, ncol=2, loc="upper right")
        if ratio:
            self.ax.set_xlabel("")
            self.rax.set_xlabel(self.style.opts_axes["xlabel"], fontsize=self.style.fontsize)
            if ref:
                self.rax.set_ylabel("Others / Ref", fontsize=self.style.fontsize)
            else:
                self.rax.set_ylabel("Data / MC", fontsize=self.style.fontsize)
            self.rax.yaxis.set_label_coords(-0.075, 1)
            self.rax.tick_params(axis='x', labelsize=self.style.fontsize)
            self.rax.tick_params(axis='y', labelsize=self.style.fontsize)
            self.rax.set_ylim((0.5, 1.5))

        if self.style.has_labels or self.style.has_signal_samples:
            labels_new = []
            handles_new = []
            for i, l in enumerate(labels):
                # If additional scale is provided, plot it on the legend:
                scale_str = ""
                if self.style.has_rescale_samples and (l in self.style.rescale_samples.keys()) and (round(self.style.rescale_samples[l],2)!=1.0):
                    scale_str = " x%.2f"%self.style.rescale_samples[l]
                if self.style.has_signal_samples and len(l)>=4 and l[:-4] in self.style.signal_samples.keys():
                    # Note: the l[:-4] strips away the potential '_sig' str from the name (added in plot_mc function).
                    scale_str = " x%i"%self.style.signal_samples[l[:-4]]

                if self.style.has_labels and l in self.style.labels_mc:
                    labels_new.append(f"{self.style.labels_mc[l]}"+scale_str)
                elif self.style.has_labels and l[:-4] in self.style.labels_mc:
                    labels_new.append(f"{self.style.labels_mc[l[:-4]]}"+scale_str)
                else:
                    labels_new.append(l+scale_str)

                handles_new.append(handles[i])

            labels = labels_new
            handles = handles_new
            self.ax.legend(
                handles,
                labels,
                fontsize=self.style.fontsize_legend,
                ncol=2,
                loc="upper right",
            )

        if self.style.print_info["year"]:
            self.ax.text(0.04, 0.75, f'Year: {self.year}', fontsize=0.7*self.style.fontsize, transform=self.ax.transAxes)
        if self.style.print_info["category"]:
            self.ax.text(0.04, 0.70, f'Cat: {cat}', fontsize=0.7*self.style.fontsize, transform=self.ax.transAxes)

    def plot_mc(self, cat, ax=None):
        '''Plots the MC histograms as a stacked plot.'''
        stacks = self._get_stacks(cat)
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_mc` will be skipped.")
            return

        # Converting dictionary of colors to a list (to be used in hist.plot)
        colors_list = list(self.colors.values())

        if ax:
            self.ax = ax
        else:
            if not hasattr(self, "ax"):
                self.define_figure(ratio=False)
        stacks["mc_nominal"].plot(
            ax=self.ax, color=colors_list, density=self.density, **self.style.opts_mc)

        if self.style.has_signal_samples:
            for sig in self.style.signal_samples.keys():
                if sig in self.h_dict.keys():
                    scale_sig = self.style.signal_samples[sig]
                    if self.verbose>1:
                        print("Plotting signal sample:", sig, "scale=", scale_sig, "cat=", cat, "hist=", self.name)

                    h_sig = scale_sig*self.h_dict[sig][{'cat': cat, 'variation': 'nominal'}]
                    h_sig.plot(ax=self.ax, color=self.colors[sig], density=self.density, **self.style.opts_sig, label=sig+'_sig')
                    # Note: '_sig' str is added to the label of the signal sample, in order to distinguish it
                    # from the same label present in the mc-stack histograms.
                else:
                    if self.verbose>0:
                        print("WARNING. Signal sample does not exist amoung the histograms", sig, cat, self.name)

        self.format_figure(cat, ratio=False)

    def blind_hist(self, cat, hist):
        if self.style.has_blind_hists and cat in self.style.blind_hists.categories:
            for blind_hname, blind_range in self.style.blind_hists.histograms.items():
                if not self.name.startswith(blind_hname):
                    continue
                else:
                    if self.verbose>0:
                        print("Blinding histogram:", self.name, "in category:", cat, 'in range', blind_range)
                    if len(blind_range)!=2:
                        if self.verbose>0:
                            print("WARNING: The range for blinding region is not correct:", blind_hname, blind_range)
                        continue
                    hist_edges = np.array(self.style.opts_axes["xedges"])
                    bins_to_zero = (hist_edges[:-1] >= blind_range[0]) & (hist_edges[:-1] < blind_range[1])
                    hist[bins_to_zero] = 0
        return hist

    def plot_data(self, cat, ax=None):
        '''Plots the data histogram as an errorbar plot.'''
        stacks = self._get_stacks(cat)
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_data` will be skipped.")
            return

        if ax:
            self.ax = ax
        else:
            if not hasattr(self, "ax"):
                self.define_figure(ratio=False)
        y = self._merge_flow_bins(stacks["data_sum"].values(flow=self.style.flow))

        # Blinding data for certain variables (if defined in plotting config)
        y = self.blind_hist(cat,y)

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

        # Removing nans and inf
        np.nan_to_num(ratio, copy=False)
        np.nan_to_num(ratio_unc, copy=False)

        self.rax.errorbar(
            self.style.opts_axes["xcenters"], ratio, yerr=ratio_unc, **self.style.opts_data
        )
        self.format_figure(cat, ratio=True)
        self.rax.axhline(1.0, color="black", linestyle="--")
                        
    def plot_compare_ratios(self, cat, ref, ax=None):
        '''Plots the ratios as an errorbar plots.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_compare_ratios` will be skipped.")
            return

        ratios, ratios_unc = self.get_ref_ratios(cat, ref=ref)
        if ax:
            self.rax = ax
        else:
            if not hasattr(self, "rax"):
                self.define_figure(ratio=True)

        for proc in ratios.keys():

            np.nan_to_num(ratios[proc], copy=False)
            np.nan_to_num(ratios_unc[proc], copy=False)

            if ref==proc:
                unity = np.ones_like(ratios_unc[proc])
                down = unity[0] - ratios_unc[proc][0]
                up = unity[1] + ratios_unc[proc][1]
                
                #print("Ratio unc:", ratios_unc[proc])
                #print("Up-down:", up-down)
                
                if ref=='data_sum':
                    color = 'black'
                else:
                    color = self.colors[proc]
                self.rax.stairs(down, baseline=up, edges=self.style.opts_axes["xedges"],
                                color=color, alpha=0.4, linewidth=0, facecolor="none", hatch='////')
            else:
                if proc=='data_sum':
                    color = 'black'
                else:
                    color = self.colors[proc]
                self.rax.errorbar(self.style.opts_axes["xcenters"], ratios[proc], yerr=ratios_unc[proc],
                                  color=color, linewidth=0, elinewidth=1, marker='o', markersize=4)
        ref_label = ref
        if ref_label in self.style.labels_mc:
            ref_label = self.style.labels_mc[ref_label]
        self.rax.text(0.04, 0.85, f'Ref = {ref_label}', fontsize=self.style.fontsize, transform=self.rax.transAxes)

        self.format_figure(cat, ref=ref, ratio=True)
        self.rax.axhline(1.0, color="gray", linestyle="--")

    def plot_systematic_uncertainty(self, cat, ratio=False, ax=None):
        '''Plots the asymmetric systematic uncertainty band on top of the MC stack, if `ratio` is set to False.
        To plot the systematic uncertainty in a ratio plot, `ratio` has to be set to True and the uncertainty band will be plotted around 1 in the ratio plot.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_systematic_uncertainty` will be skipped.")
            return
        if len(self.syst_manager.systematics) == 0:
            print(f"WARNING: no systematics found for histogram {self.name}.")
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

            # If the histogram is in density mode, the systematic uncertainty has to be normalized to the integral of the MC stack
            if self.density:
                mc_integral = sum(self._get_stacks(cat)["mc_nominal_sum"].values(flow=self.style.flow)) * np.array(self.style.opts_axes["xbinwidth"])
                up = up / mc_integral
                down = down / mc_integral

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
            if syst & (not self._is_empty(cat)):
                self.plot_systematic_uncertainty(cat)
        elif self.is_mc_only:
            self.plot_mc(cat)
            if syst & (not self._is_empty(cat)):
                self.plot_systematic_uncertainty(cat)
        elif self.is_data_only:
            self.plot_data(cat)

        if ratio:
            self.plot_datamc_ratio(cat)
            if syst & (not self._is_empty(cat)):
                self.plot_systematic_uncertainty(cat, ratio=ratio)

        self.format_figure(cat, ratio=ratio)

    def plot_datamc_all(self, ratio=True, syst=True, spliteras=False, save=True, format='png'):
        '''Plots the data and MC histograms for each year and category contained in the histograms.
        If ratio is True, also the Data/MC ratio plot is plotted.
        If syst is True, also the total systematic uncertainty is plotted.'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot data/MC for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_datamc_all` will be skipped.")
            return

        for cat in self.categories:
            if self.verbose>1:
                print('Plotting category:', cat)
            if self.only_cat and cat not in self.only_cat:
                continue
            self.define_figure(ratio)
            self.plot_datamc(cat, ratio=ratio, syst=syst)
            if save:
                plot_dir = os.path.join(self.plot_dir, cat)
                if self.log_x or self.log_y:
                    if self.log_x and self.log_y:
                        filepath = os.path.join(plot_dir, f"logxy_{self.name}_{cat}.{format}")
                    elif self.log_x:
                        filepath = os.path.join(plot_dir, f"logx_{self.name}_{cat}.{format}")
                    else:
                        filepath = os.path.join(plot_dir, f"logy_{self.name}_{cat}.{format}")
                else:
                    filepath = os.path.join(plot_dir, f"{self.name}_{cat}.{format}")
                if self.verbose>0:
                    print("Saving", filepath)
                plt.savefig(filepath, dpi=150, format=format, bbox_inches="tight")
            else:
                plt.show(self.fig)
            plt.close(self.fig)

    def plot_comparison(self, cat, ratio=True, ax=None, rax=None):
        '''Plots the comparison of the histograms'''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot for histogram {self.name} with dimension {self.dense_dim}.")
            print("The method `plot_comprison` will be skipped.")
            return

        self.style.opts_mc["stack"] = False
        self.style.opts_mc["histtype"] = "step"

        if ax:
            self.ax = ax
        if rax:
            self.rax = rax
        if (not self.is_mc_only) & (not self.is_data_only):
            self.plot_mc(cat)
            self.plot_data(cat)
        elif self.is_mc_only:
            self.plot_mc(cat)
        elif self.is_data_only:
            self.plot_data(cat)


        ref=None
        if ratio and self.style.has_compare_ref:
            ref = self.style.compare.ref
            self.plot_compare_ratios(cat, ref=ref)
            self.format_figure(cat, ratio=ratio, ref=ref)
        else:
            self.format_figure(cat, ratio=False)


    def plot_comparison_all(self, ratio=True, save=True, format='png'):
        ''' '''
        if self.dense_dim > 1:
            print(f"WARNING: cannot plot histogram {self.name} with dimension {self.dense_dim}. It will be skipped.")
            return

        for cat in self.categories:
            if self.verbose>1:
                print('Plotting category:', cat)
            if self.only_cat and cat not in self.only_cat:
                continue
            self.define_figure(ratio)
            self.plot_comparison(cat, ratio=ratio)
            if save:
                plot_dir = os.path.join(self.plot_dir, cat)
                if self.log_x or self.log_y:
                    if self.log_x and self.log_y:
                        filepath = os.path.join(plot_dir, f"logxy_{self.name}_{cat}.{format}")
                    elif self.log_x:
                        filepath = os.path.join(plot_dir, f"logx_{self.name}_{cat}.{format}")
                    else:
                        filepath = os.path.join(plot_dir, f"logy_{self.name}_{cat}.{format}")
                else:
                    filepath = os.path.join(plot_dir, f"{self.name}_{cat}.{format}")
                if self.verbose>0:
                    print("Saving", filepath)
                plt.savefig(filepath, dpi=150, format=format, bbox_inches="tight")
            else:
                plt.show(self.fig)
                plt.close(self.fig)


    def plot_systematic_shifts(
        self, cat, syst_name, ratio=True, format="png", save=True
    ):
        """Plots the systematic shifts (up/down) of a given systematic uncertainty.
        The systematic shifts are plotted as a ratio plot if ratio is set to True."""
        if self.dense_dim > 1:
            print(
                f"WARNING: cannot plot data/MC for histogram {self.name} with dimension"
                f"{self.dense_dim}"
            )

        if self.is_data_only:
            raise Exception("Cannot plot systematic shifts for a data-only histogram.")

        # we need to call this to update the histograms in the syst_manager
        self._get_stacks(cat)
        systematic = self.syst_manager.get_syst(syst_name, cat)
        systematic.plot(log_x=self.log_x,log_y=self.log_y, toplabel=self.toplabel, ratio=ratio)

        if save:
            plot_dir = os.path.join(self.plot_dir, cat, syst_name)
            os.makedirs(plot_dir, exist_ok=True)

            filename = f"{self.name}_{cat}_{syst_name}"
            if self.log_x or self.log_y:
                if self.log_x and self.log_y:
                    filename = f"logxy_{filename}"
                elif self.log_x:
                    filename = f"logx_{filename}"
                else:
                    filename = f"logy_{filename}"

            filepath = os.path.join(plot_dir, f"{filename}.{format}")
            if self.verbose > 0:
                print("Saving", filepath)
            plt.savefig(filepath, dpi=150, format=format, bbox_inches="tight")
        else:
            plt.show(systematic.fig)
        plt.close(systematic.fig)

    def plot_systematic_shifts_all(self, ratio=True, format="png", save=True):
        """Plots the systematic shifts (up/down) of all the systematic uncertainties
        for a given category."""
        for syst_name in self.syst_manager.systematics:
            for cat in self.categories:
                if self.only_cat and cat not in self.only_cat:
                    continue
                if self.verbose > 1:
                    print("Plotting systematic:", syst_name, "for category:", cat)
                self.plot_systematic_shifts(
                    cat, syst_name, ratio=ratio, format=format, save=save
                )


class SystManager:
    '''This class handles the systematic uncertainties of 1D MC histograms.'''

    def __init__(self, shape: Shape, style: Style, verbose: int = 1) -> None:
        self.shape = shape
        self.style = style
        self.verbose = verbose
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
        if self.shape.has_mcstat:
            self.systematics.append("mcstat")
        self.syst_dict = defaultdict(dict)

    def update(self, cat, stacks):
        '''Updates the dictionary of systematic uncertainties with the new cached stacks.'''
        for syst_name in self.systematics:
            self.syst_dict[cat][syst_name] = SystUnc(self.shape, stacks, syst_name, verbose=self.verbose)

    def total(self, cat):
        return SystUnc(self.shape, name="total", syst_list=list(self.syst_dict[cat].values()), verbose=self.verbose)

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
            self, shape: Shape, stacks: dict = None, name: str = None, syst_list: list = None, verbose: int = 1
    ) -> None:
        self.shape = shape
        self.style = shape.style
        self.name = name
        self.is_mcstat = self.name == "mcstat"
        self.verbose = verbose

        # Initialize the arrays of the nominal yield and squared errors as 0
        self.nominal = 0.0
        self.err2_up = 0.0
        self.err2_down = 0.0
        if stacks:
            if syst_list:
                raise Exception(
                    "The initialization of the instance is ambiguous. Either a `Shape` object or a list of `SystUnc` objects should be passed to the constructor."
                )
            else:
                self.syst_list = [self]
            self.bins = stacks["mc_nominal_sum"].axes[0].edges
            self.h_mc_nominal = stacks["mc_nominal_sum"]
            self.nominal = self.shape._merge_flow_bins(self.h_mc_nominal.values(flow=self.style.flow))
            self.check_empty_variations(stacks)
            self._get_err2(stacks)
            # Full nominal MC including all MC samples
        elif syst_list:
            self.syst_list = syst_list
            assert (
                self.nsyst > 0
            ), "Attempting to initialize a `SystUnc` instance with an empty list of systematic uncertainties."
            assert not (
                (self._n_empty == 1) & (self.nsyst == 1)
            ), "Attempting to intialize a `SystUnc` instance with an empty systematic uncertainty."
            # Get nominal yields from the first systematic uncertainty in the list
            _syst = self.syst_list[0]
            self.h_mc_nominal = _syst.h_mc_nominal
            self.nominal = _syst.nominal
            self._get_err2_from_syst()

    def __add__(self, other):
        '''Sum in quadrature of two systematic uncertainties.
        In case multiple objects are summed, the information on the systematic uncertainties that
        have been summed is stored in self.syst_list.'''
        return SystUnc(self.shape, name=f"{self.name}_{other.name}", syst_list=[self, other], verbose=self.verbose)

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
    def yaxis_ratio_limit(self) -> tuple:
        """
        Calculate the limits for the y-axis in the ratio plot.

        :return: A tuple containing the lower and upper limits for the y-axis.
        :rtype: tuple
        """
        # get the maximum deviation from nominal
        max_deviation = max(
            abs(self.ratio_down.min() - 1.0),
            abs(self.ratio_up.max() - 1.0),
            abs(self.ratio_down.max() - 1.0),
            abs(self.ratio_up.min() - 1.0),
        )
        if max_deviation == 0.0:
            warn(
                f"The ratio plot for {self.name} is flat. "
                "The y-axis limits will be set to 0.5 and 1.5."
            )
            return (0.5, 1.5)
        # add white space, so lines do not line up with axis limits
        white_space = max_deviation * 0.25
        max_deviation += white_space
        # get first digit different from 0, to get the order of magnitude
        # math.floor is used to get the integer part of the logarithm, e.g. -1.3 --> -2
        n_digits = math.floor(math.log10(white_space))
        # round to desired decimal places
        precision = Decimal(10) ** n_digits
        # maximum deviation + white space, symmetric around 1
        limits = Decimal(max_deviation).quantize(precision, decimal.ROUND_UP)
        return (1 - float(limits), 1 + float(limits))

    @property
    def nsyst(self):
        return len(self.syst_list)

    @property
    def _is_empty(self):
        return np.sum(self.nominal) == 0

    @property
    def _n_empty(self):
        return len([s for s in self.syst_list if s._is_empty])

    def check_empty_variations(self, stacks):
        '''Method used in the constructor to check if any of the systematic variations is empty.'''
        for h in stacks["mc"]:
            for variation in h.axes[0]:
                h_var = h[{'variation': variation}].values()
                h_nom = h[{'variation': 'nominal'}].values()
                # First, we check that the variation is not equal to the nominal            
                if not all(h_nom == h_var):
                    # Then, we check if the variation is empty
                    if all(h_var == np.zeros_like(h_var)):
                        if self.verbose>=1:
                            print(
                                f"WARNING: Empty variation found for systematic {self.name} in histogram {self.shape.name}. "+
                                "Please check if the input histograms are filled properly."
                            )

    def _get_err2_from_syst(self):
        '''Method used in the constructor to instanstiate a SystUnc object from
        a list of SystUnc objects. The sytematic uncertainties in self.syst_list,
        are summed in quadrature to define a new SystUnc object.'''
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
            nom = self.shape._merge_flow_bins(h_nom.values(flow=self.style.flow))
            # Sum in quadrature of mcstat
            if self.is_mcstat:
                mcstat_err2 = self.shape._merge_flow_bins(h_nom.variances(flow=self.style.flow))
                self.err2_up += mcstat_err2
                self.err2_down += mcstat_err2
                continue
            # Up/down variations for a single MC sample
            var_up = self.shape._merge_flow_bins(h[{'variation': f'{self.name}Up'}].values(flow=self.style.flow))
            var_down = self.shape._merge_flow_bins(h[{'variation': f'{self.name}Down'}].values(flow=self.style.flow))
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

    def define_figure(self, ratio=True, toplabel=None):
        """Defines the figure for the systematic shifts plot.

        :param ratio: plot ratio of shifts and nominal, defaults to True
        :type ratio: bool, optional
        """

        plt.style.use(hep.style.CMS)
        plt.rcParams.update({"font.size": self.style.fontsize})
        if ratio:
            self.fig, (self.ax, self.rax) = plt.subplots(
                2, 1, **self.style.opts_figure["systematics_ratio"]
            )
            self.fig.subplots_adjust(hspace=0.06)
            axes = (self.ax, self.rax)
        else:
            self.fig, self.ax = plt.subplots(
                1, 1, **self.style.opts_figure["systematics"]
            )
            axes = self.ax
        hep.cms.text(
            "Simulation Preliminary",
            fontsize=self.style.fontsize,
            loc=self.style.experiment_label_loc,
            ax=self.ax,
        )
        if toplabel:
            hep.cms.lumitext(
                text=toplabel,
                fontsize=self.style.fontsize,
                ax=self.ax,
            )
        return self.fig, axes

    def format_figure(self, ratio=True):
        """Formats the figure's axes, labels, ticks, xlim and ylim."""
        self.ax.set_ylabel("Counts", fontsize=self.style.fontsize)
        self.ax.legend(fontsize=self.style.fontsize_legend, ncol=2, loc="upper right")
        self.ax.tick_params(axis="x", labelsize=self.style.fontsize)
        self.ax.tick_params(axis="y", labelsize=self.style.fontsize)
        self.ax.set_xlim(
            self.style.opts_axes["xedges"][0], self.style.opts_axes["xedges"][-1]
        )
        if self.log_y:
            self.ax.set_yscale("log")
            exp = math.floor(math.log(self.nominal.max(), 10)) + 3
            y_lim_hi = self.style.opts_ylim["systematics"]["ylim_log"].get("hi", 10**exp)
            self.ax.set_ylim(
                (self.style.opts_ylim["systematics"]["ylim_log"]["lo"], y_lim_hi)
            )
        else:
            self.ax.set_ylim((0, 1.5 * self.nominal.max()))
        if self.log_x:
            self.ax.set_xscale("log")

        if ratio:
            self.rax.set_xlabel(
                self.style.opts_axes["xlabel"], fontsize=self.style.fontsize
            )
            self.rax.set_ylabel("Shift / Nominal", fontsize=self.style.fontsize)
            self.rax.yaxis.set_label_coords(-0.075, 1)
            self.rax.tick_params(axis="x", labelsize=self.style.fontsize)
            self.rax.tick_params(axis="y", labelsize=self.style.fontsize)
            self.rax.set_ylim(self.yaxis_ratio_limit)

        if self.style.has_labels:
            handles, labels = self.ax.get_legend_handles_labels()
            labels_new = []
            handles_new = []
            for i, label in enumerate(labels):
                # If additional scale is provided, plot it on the legend:
                scale_str = ""
                if (
                    self.style.has_rescale_samples
                    and label in self.style.rescale_samples
                ):
                    scale_str = f" x{self.style.rescale_samples[label]:.2f}"
                if label in self.style.labels_mc:
                    labels_new.append(f"{self.style.labels_mc[label]}" + scale_str)
                else:
                    labels_new.append(label + scale_str)

                handles_new.append(handles[i])
            labels = labels_new
            handles = handles_new
            self.ax.legend(
                handles,
                labels,
                fontsize=self.style.fontsize_legend,
                ncol=1,
                loc="upper right",
            )

    def plot(self, ratio=True, toplabel=None, log=False):
        """Plots the nominal, up and down systematic variations on the same plot."""

        # setup figure and corresponding axes
        self.log_x = log_x
        self.log_y = log_y
        self.define_figure(ratio=ratio, toplabel=toplabel)

        # plot histograms
        # https://stackoverflow.com/a/65935165
        self.ax.hist(
            self.bins[:-1],
            self.bins,
            weights=self.nominal,
            histtype="step",
            label="nominal",
            **self.style.opts_syst["nominal"],
        )
        self.ax.hist(
            self.bins[:-1],
            self.bins,
            weights=self.up,
            histtype="step",
            label=f"{self.name} up",
            **self.style.opts_syst["up"],
        )
        self.ax.hist(
            self.bins[:-1],
            self.bins,
            weights=self.down,
            histtype="step",
            label=f"{self.name} down",
            **self.style.opts_syst["down"],
        )

        # plot ratio
        if ratio:
            self.rax.axhline(1, color="gray", linestyle="--")
            self.rax.stairs(self.ratio_up, self.bins, **self.style.opts_syst["up"])
            self.rax.stairs(self.ratio_down, self.bins, **self.style.opts_syst["down"])

        # format figure
        self.format_figure(ratio=ratio)
