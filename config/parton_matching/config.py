from PocketCoffea.parameters.cuts.baseline_cuts import dilepton_presel, semileptonic_presel, passthrough
from PocketCoffea.lib.cut_functions import get_nObj, get_nBtag
from PocketCoffea.lib.cut_definition import Cut
from config.parton_matching.functions import *
from PocketCoffea.workflows.parton_matching  import PartonMatchingProcessor
cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_2018_local.json",
                  "datasets/backgrounds_MC_2018_local.json"],
        "filter" : {
            "samples": ["TTToSemiLeptonic"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : PartonMatchingProcessor,
    "output"   : "output/parton_matching",


    # "run_options" : {
    #     "executor"       : "iterative",
    #     "workers"        : 1,
    #     "scaleout"       : 60,
    #     "partition"      : "standard",
    #     "walltime"       : "03:00:00",
    #     "mem_per_worker" : "4GB", # GB
    #     "exclusive"      : False,
    #     "chunk"          : 500000,
    #     "retries"        : 30,
    #     "max"            : None,
    #     "skipbadfiles"   : None,
    #     "voms"           : None,
    #     "limit"          : 10,
    # },
    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 50,
        "partition"      : "standard",
        "walltime"       : "02:00:00",
        "mem_per_worker" : "5GB", # GB
        "exclusive"      : False,
        "chunk"          : 100000,
        "retries"        : 30,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },


    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [ get_nBtag(3, 20., "Jet") ],
    "preselections" : [semileptonic_presel],

    "categories": {
        "4j" : [passthrough],
        "5j" : [ get_nObj(5, coll="JetGood")],
        "6j" : [ get_nObj(6, coll="JetGood")],
        "7j" : [ get_nObj(7, coll="JetGood")],
        "8j" : [ get_nObj(8, coll="JetGood")],
    },
    
    "variables" : {
        "muon_pt" : {'binning' : {'n_or_arr' : 100, 'lo' : 0, 'hi' : 500}, 'xlim' : (0,500),  'xlabel' : "$p_{T}^{\mu}$ [GeV]"},
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
        "Njet_Nparton_total": {
            "Njet": { 'binning': {"n_or_arr": 11, 'lo': 4, 'hi':15}, "xlabel": "N jets"},
            "Nparton" : { 'binning': {"n_or_arr": 13, 'lo': 2, 'hi':15}, "ylabel": "N partons"}
        },
        "Njet_Nparton_matched": {
            "Njet": { 'binning': {"n_or_arr": 6, 'lo': 2, 'hi':8}, "xlabel": "N jets"},
            "Nparton" : { 'binning': {"n_or_arr": 6, 'lo': 2, 'hi':8}, "ylabel": "N partons"}
        },
        "Nparton_Nparton_matched": {
            "Nparton": { 'binning': {"n_or_arr": 6, 'lo': 2, 'hi':8}, "xlabel": "N partons"},
            "Nparton_matched" : { 'binning': {"n_or_arr": 6, 'lo': 2, 'hi':8}, "ylabel": "N partons matched"}
        }
     },
    "scale" : "log"
}
