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
            "samples": ["TTToSemiLeptonic","ttHTobb"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : PartonMatchingProcessor,
    "output"   : "output/parton_matching_dR03",
    "workflow_extra_options": {"deltaR": 0.3},

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
        "scaleout"       : 100,
        "partition"      : "standard",
        "walltime"       : "05:00:00",
        "mem_per_worker" : "12GB", # GB
        "exclusive"      : False,
        "chunk"          : 200000,
        "retries"        : 30,
        "treereduction"  : 5,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },


    # Cuts and plots settings
    "finalstate" : "semileptonic_partonmatching",
    "skim": [ get_nObj(4, 15., "Jet"),
              get_nBtag(3, 15., "Jet") ],
    "preselections" : [semileptonic_presel],

    "categories": {
        "4j" : [passthrough],
        #"5j" : [ get_nObj(5, coll="JetGood")],
        #"6j" : [ get_nObj(6, coll="JetGood")],
        #"7j" : [ get_nObj(7, coll="JetGood")],
        #"8j" : [ get_nObj(8, coll="JetGood")],
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
        "npartonmatched" : {'binning' : {'n_or_arr' : 20, 'lo' : 0, 'hi' : 20}, 'xlim' : (0,20),  'xlabel' : "N. Partons matched"},
        "partonmatched_pt" : {'binning' : {'n_or_arr' : 100, 'lo' : 0, 'hi' : 500}, 'xlim' : (0,500),  'xlabel' : "$p_{T}$ [GeV] matched parton"},
        "partonmatched_eta" : {'binning' : {'n_or_arr' :50, 'lo' : -3, 'hi' : 3}, 'xlim' : (-3,3),  'xlabel' : "$\eta$ matched parton"},
#        "parton_dRMatchedJet" : None,
        "partonmatched_pdgId": {'binning' : {'n_or_arr' : 50, 'lo' : -25, 'hi' : 25},  'xlim' : (0,1),    'xlabel' : r'Parton matched pdgId'},

    },
     "variables2d" : {
        "Njet_Nparton_total": {
            "Njet": { 'binning': {"n_or_arr": 11, 'lo': 4, 'hi':15}, "xlabel": "N jets"},
            "Nparton" : { 'binning': {"n_or_arr": 11, 'lo': 4, 'hi':15}, "ylabel": "N partons"}
        },
        "Njet_Nparton_matched": {
            "Njet": { 'binning': {"n_or_arr": 11, 'lo': 4, 'hi':15}, "xlabel": "N jets"},
            "Nparton_matched" : { 'binning': {"n_or_arr": 15, 'lo': 0, 'hi':15}, "ylabel": "N partons matched"}
        },
        "Nparton_Nparton_matched": {
            "Nparton": { 'binning': {"n_or_arr": 11, 'lo': 4, 'hi':15}, "xlabel": "N partons"},
            "Nparton_matched" : { 'binning': {"n_or_arr": 15, 'lo': 0, 'hi':15}, "ylabel": "N partons matched"}
        }
     },
    "scale" : "log"
}
