from parameters.cuts.baseline_cuts import dilepton_presel, passthrough
from lib.cut_functions import count_objects_gt
from lib.cut_definition import Cut

cfg =  {

    "dataset" : {
        "jsons": ["datasets/RunIISummer20UL18_local.json"],
        "filter" : {
            "samples": ["ttHTobb"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : "mem",
    "output"   : "output/mem",

    # Executor parameters
    "run_options" : {
        "executor"     : "iterative",
        "workers"      : 20,
        "scaleout"     : 20,
        "chunk"        : 50000,
        "max"          : None,
        "skipbadfiles" : None,
        "voms"         : None,
        "limit"        : 4,
    },

    # Cuts and plots settings
    "finalstate" : "dilepton",
    "preselections" : [dilepton_presel],
    "categories": {
        "SR" : [   Cut(name="fully_matched",
                       params={"object": "BQuarkMatched",
                               "value": 4},
                       function=count_objects_gt)],
    },
    
    "variables" : {
        "muon_pt" : {'binning' : {'n_or_arr' : 200, 'lo' : 0, 'hi' : 2000}, 'xlim' : (0,500),  'xlabel' : "$p_{T}^{\mu}$ [GeV]"},
        "muon_eta" : None,
        "muon_phi" : None,
        "electron_pt" : None,
        "electron_eta" : None,
        "electron_phi" : None,
        "jet_pt" : None,
        "jet_eta" : None,
        "jet_phi" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "njet" : None,
        "nbjet" : None,
        "nbquark" : None,
        "bquark_pt" : None,
        "bquark_eta" : None,
        "bquark_phi" : None,
        "bquark_drMatchedJet" : None,

    },
    "variables2d" : {},
    "scale" : "log"
}
