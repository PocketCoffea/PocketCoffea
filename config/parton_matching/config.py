from PocketCoffea.parameters.cuts.baseline_cuts import dilepton_presel, semileptonic_presel, passthrough
from PocketCoffea.lib.cut_functions import count_objects_gt, get_nJets_min
from PocketCoffea.lib.cut_definition import Cut
from config.parton_matching.functions import *
from PocketCoffea.workflows.parton_matching  import PartonMatchingProcessor
cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_2018_local.json",
                  "datasets/backgrounds_MC_2018_local.json"],
        "filter" : {
            "samples": ["TTToSemiLeptonic", "ttHTobb"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : PartonMatchingProcessor,
    "output"   : "output/parton_matching",

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 30,
        "partition"      : "standard",
        "walltime"       : "03:00:00",
        "mem_per_worker" : "3GB", # GB
        "exclusive"      : False,
        "chunk"          : 100000,
        "retries"        : 30,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : 100,
    },
    
    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "preselections" : [semileptonic_presel],

    "categories": {
        "4j_3bjets" : [passthrough],
        "4j_4bjets" : [getNjetNb_cut(4,4)],
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
        "nparton" : None,
        "parton_pt" : None,
        "parton_eta" : None,
        "parton_phi" : None,
#        "parton_dRMatchedJet" : None,
        "parton_pdgId": None,

    },
     "variables2d" : {

        "Njet_Nparton_matched": {
            "Njet": { 'binning': {"n_or_arr": 15, 'lo': 0, 'hi':15}, "xlabel": "N jets"},
            "Nparton" : { 'binning': {"n_or_arr": 15, 'lo': 0, 'hi':15}, "ylabel": "N partons"}
     }
     },
    "scale" : "log"
}
