from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_HLTsel, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.calibrators.common.common import JetsCalibrator
from pocket_coffea.lib.weights.weights import WeightLambda
from workflow_shape_variations import BasicProcessor
from custom_cut_functions import *
import os
import numpy as np
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

# Custom weights with known nominal/up/down values for ratio checks in the test.
# Names are unique (sf_sv_C/D) to avoid conflicts with identically-valued weights
# in the subsamples test configs that share the same global WeightLambda registry.
# sf_sv_C: inclusive for TTTo2L2Nu__ele  (nominal=2, up=4, down=0.5)
# sf_sv_D: bycategory 1btag for TTTo2L2Nu__ele  (nominal=3, up=5, down=0.7)
my_custom_sf_C = WeightLambda.wrap_func(
    name="sf_sv_C",
    function=lambda params, metadata, events, size, shape_variations: (
        np.ones(size) * 2.0, np.ones(size) * 4.0, np.ones(size) * 0.5,
    ),
    has_variations=True,
)

my_custom_sf_D = WeightLambda.wrap_func(
    name="sf_sv_D",
    function=lambda params, metadata, events, size, shape_variations: (
        np.ones(size) * 3.0, np.ones(size) * 5.0, np.ones(size) * 0.7,
    ),
    has_variations=True,
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
        # ele and ele2 have identical selection cuts.
        # ele has subsample-specific weight SFs; ele2 does not.
        # Both subsamples share the common JES shape variation.
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
        "bysample": {
            "TTTo2L2Nu__ele": {
                "inclusive": ["sf_sv_C"],
                "bycategory": {
                    "1btag": ["sf_sv_D"],
                },
            },
        },
    },
    weights_classes=common_weights + [my_custom_sf_C, my_custom_sf_D],
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
            "bysample": {
                "TTTo2L2Nu__ele": {
                    "inclusive": ["sf_sv_C"],
                    "bycategory": {
                        "1btag": ["sf_sv_D"],
                    },
                },
            },
        },
        "shape": {
            # JES is a common shape variation: both ele and ele2 receive it.
            "common": {
                "inclusive": ["jet_calibration"],
            },
        },
    },

    variables={
        **jet_hists(),
        **count_hist("JetGood"),
    },
)
