import argparse

import awkward as ak
import numpy as np
import math
import uproot
from vector import MomentumObject4D

from coffea import hist
from coffea.nanoevents.methods import nanoaod

ak.behavior.update(nanoaod.behavior)

def load_puhist_target(filename):

    fi = uproot.open(filename)

    h = fi["pileup"]
    edges = np.array(h.edges)
    values_nominal = np.array(h.values)
    values_nominal = values_nominal / np.sum(values_nominal)

    h = fi["pileup_plus"]
    values_up = np.array(h.values)
    values_up = values_up / np.sum(values_up)

    h = fi["pileup_minus"]
    values_down = np.array(h.values)
    values_down = values_down / np.sum(values_down)
    return edges, (values_nominal, values_up, values_down)

def remove_inf_nan(arr):

    arr[np.isinf(arr)] = 0
    arr[np.isnan(arr)] = 0
    arr[arr < 0] = 0

### PileUp weight
def compute_pu_weights(pu_corrections_target, weights, mc_nvtx, reco_nvtx):

    pu_edges, (values_nom, values_up, values_down) = pu_corrections_target

    src_pu_hist = hist.Hist("Pileup", coffea.hist.Bin("pu", "pu", pu_edges))
    src_pu_hist.fill(pu=mc_nvtx, weight=weights)
    norm = sum(src_pu_hist.values)
    histo.scale(1./norm)
    #src_pu_hist.contents = src_pu_hist.contents/norm
    #src_pu_hist.contents_w2 = src_pu_hist.contents_w2/norm

    ratio = values_nom / src_pu_hist.values
#    ratio = values_nom / mc_values
    remove_inf_nan(ratio)
    #pu_weights = np.zeros_like(weights)
    pu_weights = np.take(ratio, np.digitize(reco_nvtx, np.array(pu_edges)) - 1)

    return pu_weights

# lepton scale factors
def compute_lepton_weights(leps, evaluator, SF_list, lepton_eta=None, year=None):

    lepton_pt = leps.pt
    if lepton_eta is None:
        lepton_eta = leps.eta
    weights = np.ones(len(lepton_pt))

    for SF in SF_list:
        if SF.startswith('mu'):
            if year=='2016':
                if 'trigger' in SF:
                    x = lepton_pt
                    y = np.abs(lepton_eta)
                else:
                    x = lepton_eta
                    y = lepton_pt
            else:
                x = lepton_pt
                y = np.abs(lepton_eta)
        elif SF.startswith('el'):
            if 'trigger' in SF:
                x = lepton_pt
                y = lepton_eta
            else:
                x = lepton_eta
                y = lepton_pt
        else:
            raise Exception(f'unknown SF name {SF}')
        weights = weights*evaluator[SF](x, y)
        #weights *= evaluator[SF](x, y)

    per_event_weights = ak.prod(weights, axis=1)
    return per_event_weights
