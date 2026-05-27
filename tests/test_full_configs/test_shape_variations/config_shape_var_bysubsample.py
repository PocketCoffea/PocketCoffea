from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_HLTsel, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.calibrators.common.common import JetsCalibrator
from workflow_shape_variations import BasicProcessor
from custom_cut_functions import *
import os
localdir = os.path.dirname(os.path.abspath(__file__))

from pocket_coffea.lib.weights.common import common_weights
from pocket_coffea.parameters import defaults

default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir+"/params")

parameters = defaults.merge_parameters_from_files(
    default_parameters,
    f"{localdir}/params/object_preselection_run2.yaml",
    f"{localdir}/params/triggers.yaml",
    update=True,
)

cfg = Configurator(
    parameters=parameters,
    datasets={
        "jsons": ["datasets/datasets_cern.json"],
        "filter": {
            "samples": ["TTTo2L2Nu"],
            "samples_exclude": [],
            "year": ["2018"],
        },
        # Two subsamples with identical cuts: ele gets JEC shape variations, ele2 does not.
        # This exercises the per-subsample shape variation axis construction and filling.
        "subsamples": {
            "TTTo2L2Nu": {
                "ele":  [get_nObj_min(1, coll="ElectronGood"), get_nObj_eq(0, coll="MuonGood")],
                "ele2": [get_nObj_min(1, coll="ElectronGood"), get_nObj_eq(0, coll="MuonGood")],
            }
        },
    },

    workflow=BasicProcessor,

    skim=[get_nPVgood(1), eventFlags, goldenJson,
          get_HLTsel(primaryDatasets=["SingleMuon", "SingleEle"])],

    preselections=[passthrough],
    categories={
        "baseline": [passthrough],
        "1btag": [get_nObj_min(1, coll="BJetGood")],
    },

    weights={
        "common": {
            "inclusive": ["genWeight", "lumi", "XS", "pileup",
                          "sf_ele_id", "sf_ele_reco",
                          "sf_mu_id", "sf_mu_iso"],
            "bycategory": {
                "1btag": ["sf_btag"],
            },
        },
        "bysample": {},
    },
    weights_classes=common_weights,
    calibrators=[JetsCalibrator],

    variations={
        "weights": {
            "common": {
                "inclusive": ["pileup", "sf_ele_id", "sf_ele_reco",
                              "sf_mu_id", "sf_mu_iso"],
                "bycategory": {
                    "1btag": ["sf_btag"],
                },
            },
            "bysample": {},
        },
        "shape": {
            # No shape variations for the full sample or for ele2.
            # Only ele gets jet calibration shape variations.
            "common": {
                "inclusive": [],
            },
            "bysample": {
                "TTTo2L2Nu__ele": {
                    "inclusive": ["jet_calibration"],
                },
            },
        },
    },

    variables={
        **jet_hists(),
        **count_hist("JetGood"),
    },
)
