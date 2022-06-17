import os
import copy
import argparse

from .jets import *
from .leptons import *


def jet_nohiggs_selection(jets, mask_jets, fatjets, dr=1.2):

    #nested_mask = jets.p4.match(fatjets.p4[mask_fatjets,0], matchfunc=pass_dr, dr=dr)
    nested_mask = jets.match(fatjets, matchfunc=pass_dr, dr=dr)
    jets_pass_dr = nested_mask.all()

    return mask_jets & jets_pass_dr
