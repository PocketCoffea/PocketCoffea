from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nPVgood, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.columns_manager import ColOut

import workflow
from workflow import PromptMVAProcessor

import cloudpickle
cloudpickle.register_pickle_by_value(workflow)

import os
localdir = os.path.dirname(os.path.abspath(__file__))

from pocket_coffea.lib.weights.common import common_weights
from pocket_coffea.parameters import defaults

default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir + "/params")

parameters = defaults.merge_parameters_from_files(
    default_parameters,
    f"{localdir}/params/object_preselection.yaml",
    f"{localdir}/params/triggers.yaml",
    update=True,
)

cfg = Configurator(
    parameters=parameters,
    datasets={
        "jsons": ["datasets/datasets_redirector.json"],
        "filter": {
            "samples": ["TTTo2L2Nu"],
            "samples_exclude": [],
            "year": ["2023_postBPix"],
        },
    },

    workflow=PromptMVAProcessor,

    skim=[get_nPVgood(1), eventFlags],

    preselections=[passthrough],
    categories={
        "baseline": [passthrough],
    },

    weights={
        "common": {
            "inclusive": ["genWeight", "lumi", "XS"],
        },
    },
    weights_classes=common_weights,

    variations={
        "weights": {"common": {"inclusive": []}},
    },

    variables={
        **count_hist("ElectronGood"),
        **count_hist("MuonGood"),
        **count_hist("JetGood"),
    },

    columns={
        "common": {
            "inclusive": [
                ColOut("ElectronGood", ["mvaTTH_redo"]),
                ColOut("MuonGood", ["mvaTTH_redo"]),
            ]
        }
    },
)
