from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nObj_min, get_HLTsel, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
import workflow
from workflow import BasicProcessor
import cloudpickle
cloudpickle.register_pickle_by_value(workflow)
import os
localdir = os.path.dirname(os.path.abspath(__file__))
from pocket_coffea.lib.weights.common import common_weights
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir+"/params")
parameters = defaults.merge_parameters_from_files(default_parameters,
                                                    f"{localdir}/params/object_preselection.yaml",
                                                    f"{localdir}/params/triggers.yaml",
                                                   update=True)
from pocket_coffea.lib.calibrators.common.common import JetsCalibrator

cfg = Configurator(
    parameters = parameters,
    datasets = {
        "jsons": ['datasets/datasets_cern.json'],
        "filter" : {
            "samples": ['TTTo2L2Nu'],
            "samples_exclude" : [],
            "year": ['2018']
        },
    },
    workflow = BasicProcessor,
    workflow_options = {"skim_mode": "presel_any_variation"},
    
    # We keep skim very loose
    skim = [get_nPVgood(1), eventFlags, get_HLTsel()],
    save_skimmed_files = "./skim_presel_any/",

    # Preselection is tighter to ensure some events fail nominal but might pass JES Up/Down
    preselections = [get_nObj_min(5, minpt=30., coll="JetGood")],
    categories = {
        "baseline": [passthrough],
    },
    weights = {
        "common": {
            "inclusive": ["genWeight", "lumi", "XS", "pileup"],
            "bycategory": {},
       },
        "bysample": {}
    },
    weights_classes = common_weights,
    
    # Enable JES shape variations so loop_over_variations runs them
    calibrators = [ JetsCalibrator ],
    variations = {
        "weights": {"common": {"inclusive": [], "bycategory": {}}, "bysample": {}},
        "shape": {"common": {"inclusive": ["jet_calibration"]}, "bysample": {}}
    },
    variables = {},
    columns = {},
)
