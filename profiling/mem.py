from PocketCoffea.parameters.cuts.baseline_cuts import dilepton_presel, passthrough
from PocketCoffea.lib.cut_functions import count_objects_gt, get_nJets_min
from PocketCoffea.lib.cut_definition import Cut
from config.parton_matching.functions import *
from PocketCoffea.workflows.parton_matching  import PartonMatchingProcessor

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
    "workflow" : PartonMatchingProcessor,
    "output"   : "output/mem",

    # Executor parameters
    "run_options" : {
        "executor"     : "iterative",
        "workers"      : 12,
        "scaleout"     : 10,
        "chunk"        : 100000,
        "max"          : None,
        "skipbadfiles" : None,
        "voms"         : None,
        "limit"        : 5,
    },

    # Cuts and plots settings
    "finalstate" : "dilepton",
    "preselections" : [dilepton_presel],
    "categories": {
        "SR" : [passthrough],
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
        "parton_pt": None,
        "parton_eta": None,
        "parton_pdgId" : None,
       # "parton_dRMatchedJet": None,
     },
    "variables2d" : {},
    "scale" : "log"
}


