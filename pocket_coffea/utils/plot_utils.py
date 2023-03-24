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
from ..parameters.plotting import *

class DataMC:
    '''This class handles the plotting of 1D data/MC histograms.'''
    def __init__(self, h_dict, name, data_key="DATA", spliteras=False):
        self.h_dict = h_dict
        self.name = name
        self.data_key = data_key
        self.spliteras = spliteras
        assert type(h_dict) == dict, "The DataMC object receives a dictionary of hist.Hist objects as argument."
        self.define_samples()
        assert self.dense_dim == 1, f"Histograms with dense dimension {self.dense_dim} cannot be plotted. Only 1D histograms are supported."
        self.load_attributes()

    @property
    def dense_axes(self):
        '''Returns the list of dense axes of a histogram.'''
        dense_axes_dict = {s : [] for s in self.h_dict.keys()}

        for s, h in self.h_dict.items():
            for ax in h.axes:
                if not type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
                    dense_axes_dict[s].append(ax)
        dense_axes = list(dense_axes_dict.values())
        assert all(v == dense_axes[0] for v in dense_axes), "Not all the histograms in the dictionary have the same dense dimension."
        dense_axes = dense_axes[0]

        return dense_axes

    def _categorical_axes(self, mc=True):
        '''Returns the list of categorical axes of a histogram.'''
        # Since MC and data have different categorical axes, the argument mc needs to specified
        if mc:
            d = {s : v for s, v in self.h_dict.items() if s in self.samples_mc}
        else:
            d = {s : v for s, v in self.h_dict.items() if s in self.samples_data}
        categorical_axes_dict = {s : [] for s in d.keys()}

        for s, h in d.items():
            for ax in h.axes:
                if type(ax) in [hist.axis.StrCategory, hist.axis.IntCategory]:
                    categorical_axes_dict[s].append(ax)
        categorical_axes = list(categorical_axes_dict.values())
        assert all(v == categorical_axes[0] for v in categorical_axes), "Not all the histograms in the dictionary have the same categorical dimension."
        categorical_axes = categorical_axes[0]

        return categorical_axes

    @property
    def categorical_axes_mc(self):
        return self._categorical_axes(mc=True)

    @property
    def categorical_axes_data(self):
        return self._categorical_axes(mc=False)

    @property
    def dense_dim(self):
        '''Returns the number of dense axes of a histogram.'''
        return len(self.dense_axes)
    
    def get_axis_items(self, axis_name, mc=True):
        if mc:
            axis = [ax for ax in self.categorical_axes_mc if ax.name == axis_name][0]
        else:
            axis = [ax for ax in self.categorical_axes_data if ax.name == axis_name][0]
        return list(axis.value(range(axis.size)))

    def stack_sum(self, mc):
        '''Returns the sum histogram of a stack (`hist.stack.Stack`) of histograms.'''
        if mc:
            stack = self.stack_mc_nominal
        else:
            stack = self.stack_data
        if len(stack) == 1:
            return stack[0]
        else:
            htot = stack[0]
            for h in stack[1:]:
                htot = htot + h
            return htot

    @property
    def stack_sum_data(self):
        return self.stack_sum(mc=False)

    @property
    def stack_sum_mc_nominal(self):
        return self.stack_sum(mc=True)

    def define_samples(self):
        self.samples = self.h_dict.keys()
        self.samples_data = list(filter(lambda d : self.data_key in d, self.samples))
        self.samples_mc = list(filter(lambda d : self.data_key not in d, self.samples))

    def load_attributes(self):
        assert len(set([self.h_dict[s].ndim for s in self.samples_mc])), "Not all the MC histograms have the same dimension."
        for ax in self.categorical_axes_mc:
            setattr(self, {'year': 'years', 'cat': 'categories', 'variation': 'variations'}[ax.name], self.get_axis_items(ax.name))
        self.xaxis = self.dense_axes[0]
        self.xcenters = self.xaxis.centers
        self.xedges = self.xaxis.edges
        self.xbinwidth = np.ediff1d(self.xedges)
        self.is_mc_only = True if len(self.samples_data) == 0 else False
        if not self.is_mc_only:
            self.lumi = {year : femtobarn(lumi[year]['tot'], digits=1) for year in self.years}

    def build_stacks(self, year, cat):
        slicing_mc = {'year': year, 'cat': cat}
        slicing_mc_nominal = {'year': year, 'cat': cat, 'variation': 'nominal'}
        self.h_dict_mc = {d: self.h_dict[d][slicing_mc] for d in self.samples_mc}
        self.h_dict_mc_nominal = {d: self.h_dict[d][slicing_mc_nominal] for d in self.samples_mc}
        self.nevents = {d: round(sum(self.h_dict_mc_nominal[d].values()), 1) for d in self.samples_mc}
        reverse=True
        # Here implement if log: reverse=False
        self.nevents = dict( sorted(self.nevents.items(), key=lambda x:x[1], reverse=reverse) )
        # Build MC stack with variations and nominal MC stack
        self.stack_mc = hist.Stack.from_dict(self.h_dict_mc)
        self.stack_mc_nominal = hist.Stack.from_dict(self.h_dict_mc_nominal)

        if not self.is_mc_only:
            # Sum over eras if specified as extra argument
            if 'era' in self.categorical_axes_data:
                if self.spliteras:
                    slicing_data = {'year': year, 'cat': cat}
                else:
                    slicing_data = {'year': year, 'cat': cat, 'era': sum}
            else:
                if self.spliteras:
                    raise Exception("No axis 'era' found. Impossible to split data by era.")
                else:
                    slicing_data = {'year': year, 'cat': cat}
            self.h_dict_data = {d: self.h_dict[d][slicing_data] for d in self.samples_data}
            self.stack_data = hist.Stack.from_dict(self.h_dict_data)

    def plot_datamc(self, year, cat):
        plt.style.use([hep.style.ROOT, {'font.size': fontsize}])
        plt.rcParams.update({'font.size': fontsize})
        fig, (ax, rax) = plt.subplots(2, 1, **opts_figure["total"])
        fig.subplots_adjust(hspace=0.06)
        hep.cms.text("Preliminary", fontsize=fontsize, loc=0, ax=ax)
        hep.cms.lumitext(text=f'{self.lumi[year]}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=fontsize, ax=ax)
        self.stack_mc_nominal.plot(stack=True, histtype='fill', ax=ax)#, color=colors)
        if not self.is_mc_only:
            ax.errorbar(
                self.xcenters, self.stack_sum_data.values(), yerr=np.sqrt(self.stack_sum_data.values()), **opts_data
            )
            self.h_dict_data = {d: self.h_dict[d][slicing_data] for d in self.samples_data}
            self.stack_data = hist.Stack.from_dict(self.h_dict_data)

    def define_figure(self, year, ratio=True):
        plt.style.use([hep.style.ROOT, {'font.size': fontsize}])
        plt.rcParams.update({'font.size': fontsize})
        if ratio:
            fig, (self.ax, self.rax) = plt.subplots(2, 1, **opts_figure["total"])
            fig.subplots_adjust(hspace=0.06)
        else:
            fig, self.ax  = plt.subplots(1, 1, **opts_figure["total"])
        hep.cms.text("Preliminary", fontsize=fontsize, loc=0, ax=self.ax)
        hep.cms.lumitext(text=f'{self.lumi[year]}' + r' fb$^{-1}$, 13 TeV,' + f' {year}', fontsize=fontsize, ax=self.ax)

    def plot_mc(self):
        self.stack_mc_nominal.plot(stack=True, histtype='fill', ax=self.ax)#, color=colors)

    def plot_data(self):
        self.ax.errorbar(self.xcenters, self.stack_sum_data.values(), yerr=np.sqrt(self.stack_sum_data.values()), **opts_data)

    def plot_datamc(self, year, cat):
        self.define_figure(year)
        self.plot_mc()
        if not self.is_mc_only:
            self.plot_data()
