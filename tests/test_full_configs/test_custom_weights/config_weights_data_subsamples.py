from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.columns_manager import ColOut

import workflow
from workflow import BasicProcessor

import cloudpickle
import custom_cut_functions
cloudpickle.register_pickle_by_value(workflow)
cloudpickle.register_pickle_by_value(custom_cut_functions)

from custom_cut_functions import *
import os
localdir = os.path.dirname(os.path.abspath(__file__))

from pocket_coffea.lib.weights.common import common_weights

from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir + "/params")

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  update=True)

from pocket_coffea.lib.weights.weights import WeightLambda
import numpy as np

# A by-subsample weight that applies to DATA (isMC_only=False) and returns a constant 2.
sf_data_subsample = WeightLambda.wrap_func(
    name="sf_data_subsample",
    function=lambda params, metadata, events, size, shape_variations: np.ones(size) * 2.0,
    has_variations=False,
    isMC_only=False,
)

cfg = Configurator(
    parameters=parameters,
    datasets={
        "jsons": ['datasets/datasets_redirector.json'],
        "filter": {
            "samples": ["DATA_SingleMuon"],
            "samples_exclude": [],
            "year": ['2018'],
        },
        # Two data subsamples with identical selection: 'wtd' carries the by-subsample
        # weight below, 'unwtd' does not, so their ratio isolates the weight factor.
        "subsamples": {
            "DATA_SingleMuon": {
                "wtd": [passthrough],
                "unwtd": [passthrough],
            }
        },
    },

    workflow=BasicProcessor,

    skim=[get_nPVgood(1), eventFlags, goldenJson],
    preselections=[passthrough],
    categories={
        "2jets": [get_nObj_min(2, coll="JetGood")],
    },

    weights={
        "common": {
            "inclusive": ["genWeight", "lumi", "XS", "pileup",
                          "sf_ele_id", "sf_ele_reco",
                          "sf_mu_id", "sf_mu_iso"],
            "bycategory": {},
        },
        # By-subsample weight on DATA: keyed by the fully-qualified subsample name.
        "bysample": {
            "DATA_SingleMuon__wtd": {
                "inclusive": ["sf_data_subsample"],
            }
        },
    },
    weights_classes=common_weights + [sf_data_subsample],

    variations={
        "weights": {
            "common": {
                "inclusive": [],
                "bycategory": {},
            },
            "bysample": {},
        },
    },

    variables={
        **count_hist("JetGood"),
    },

    columns={
        # Applied to every subsample of DATA_SingleMuon, so both wtd and unwtd export a
        # 'weight' column (which folds in the by-subsample weight).
        "bysample": {
            "DATA_SingleMuon": {
                "bycategory": {
                    "2jets": [ColOut("JetGood", ["pt"])],
                }
            }
        }
    },
)
