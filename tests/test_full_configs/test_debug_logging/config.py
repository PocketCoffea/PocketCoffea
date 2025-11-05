# Test configuration for super verbose debug logging feature
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.cut_functions import get_nObj_min, get_HLTsel, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

import workflow
from workflow import BasicProcessor

# Register custom modules in cloudpickle to propagate them to dask workers
import cloudpickle
cloudpickle.register_pickle_by_value(workflow)

import os
localdir = os.path.dirname(os.path.abspath(__file__))

# Creating weights configuration
from pocket_coffea.lib.weights.common import common_weights

# Loading default parameters
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir+"/params")

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                    f"{localdir}/params/object_preselection.yaml",
                                                    f"{localdir}/params/triggers.yaml",
                                                   update=True)
cfg = Configurator(
    parameters = parameters,
    datasets = {
        "jsons": [f'{localdir}/datasets/datasets_redirector.json'],
        "filter" : {
            "samples": ['TTTo2L2Nu'],
            "samples_exclude" : [],
            "year": ['2018']
        }
    },

    workflow = BasicProcessor,
    
    # Workflow options with super debug enabled
    workflow_options = {
        "super_debug": {
            "enabled": True,
            "output_dir": "debug_logs",
            "log_filename": "test_debug.log",
            "sample_size": 5,  # Small sample for testing
            "check_zeros": True,
            "check_nans": True,
            "check_infs": True,
        }
    },

    skim = [get_nPVgood(1), eventFlags, goldenJson], 

    preselections = [passthrough],
    
    categories = {
        "baseline": [passthrough],
        "1jet": [get_nObj_min(1, coll="JetGood")],
        "2jet": [get_nObj_min(2, coll="JetGood")],
    },

    weights = {
        "common": {
            "inclusive": ["genWeight","lumi","XS","pileup"],
            "bycategory": {
                "1jet": ["sf_mu_id"],
                "2jet": ["sf_mu_id"],
            },
       },
    },
    
    # Passing a list of WeightWrapper objects
    weights_classes = common_weights,
    
    # No calibrators for simplicity
    calibrators = default_calibrators_sequence,
    
    variations = {
        "weights": {
            "common": {
                "inclusive": ["pileup"],
                "bycategory" : {
                    "1jet": ["sf_mu_id"],
                    "2jet": ["sf_mu_id"],
                }
            },
        },
    },

    variables = {
        **jet_hists(),
        **count_hist("JetGood"),
    },

    columns = {},
)
