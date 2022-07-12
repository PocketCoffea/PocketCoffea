from PocketCoffea.parameters.cuts.baseline_cuts import dilepton_presel, semileptonic_presel, passthrough
from PocketCoffea.lib.cut_functions import  get_nObj
from PocketCoffea.workflows.base  import ttHbbBaseProcessor
cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_2018_local.json",
                  "datasets/backgrounds_MC_2018_local.json",
                  "datasets/DATA_SingleMuon_local.json"],
        "filter" : {
            "samples": ["TTToSemiLeptonic", "ttHTobb"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test_skim",

    "run_options" : {
        "executor"       : "dask/slurm",
        #"executor": "futures",
        "workers"        : 1,
        "scaleout"       : 100,
        "partition"      : "standard",
        "walltime"       : "3:00:00",
        "mem_per_worker" : "3GB", # GB
        "exclusive"      : False,
        "chunk"          : 200000,
        "retries"        : 30,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },

    
    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [],
    "preselections" : [passthrough],

    "categories": {
        "1j" : [get_nObj(1, 20, "Jet")],
        "2j" : [get_nObj(2, 20, "Jet")],
        "3j" : [get_nObj(3, 20, "Jet")],
        "4j" : [get_nObj(4, 20, "Jet")],
        "1jgood" : [get_nObj(1, 20, "JetGood")],
        "2jgood" : [get_nObj(2, 20, "JetGood")],
        "3jgood" : [get_nObj(3, 20, "JetGood")],
        "4jgood" : [get_nObj(4, 20, "JetGood")],
        "1bjetgood" : [get_nObj(1, 20, "BJetGood")],
        "2bjetgood" : [get_nObj(2, 20, "BJetGood")],
        "3bjetgood" : [get_nObj(3, 20, "BJetGood")],
        "4bjetgood" : [get_nObj(4, 20, "BJetGood")],
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
    },
     "variables2d" : {
     },
    "scale" : "log"
}
