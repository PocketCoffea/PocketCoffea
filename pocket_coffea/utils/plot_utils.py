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

class DataMC:
    '''This class handles the plotting of 1D data/MC histograms.'''
    def __init__(self, h_dict, name, data_key="DATA"):
        self.h_dict = h_dict
        self.name = name
        self.data_key = data_key
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
    
    @property
    def categorical_axes(self, mc=True):
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
    def dense_dim(self):
        '''Returns the number of dense axes of a histogram.'''
        return len(self.dense_axes)
    
    def get_axis_items(self, axis_name):
        axis = [ax for ax in self.categorical_axes if ax.name == axis_name][0]
        return list(axis.value(range(axis.size)))

    def define_samples(self):
        self.samples = self.h_dict.keys()
        self.samples_data = list(filter(lambda d : self.data_key in d, self.samples))
        self.samples_mc = list(filter(lambda d : self.data_key not in d, self.samples))

    def load_attributes(self):
        assert len(set([self.h_dict[s].ndim for s in self.samples_mc])), "Not all the MC histograms have the same dimension."
        for ax in self.categorical_axes:
            setattr(self, {'year': 'years', 'cat': 'categories', 'variation': 'variations'}[ax.name], self.get_axis_items(ax.name))
        self.xaxis = self.dense_axes[0]
        self.xedges = self.xaxis.edges
        self.xbinwidth = np.ediff1d(self.xedges)
        self.is_mc_only = True if len(self.samples_data) == 0 else False

