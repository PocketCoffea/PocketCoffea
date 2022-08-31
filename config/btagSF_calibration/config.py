from PocketCoffea.parameters.cuts.baseline_cuts import semileptonic_presel_nobtag, passthrough
from config.parton_matching.functions import *
from PocketCoffea.lib.cut_definition import Cut
from PocketCoffea.lib.cut_functions import get_nObj, get_nBtag
from PocketCoffea.workflows.btag_sf_calibration import BtagSFCalibration

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
    "workflow" : BtagSFCalibration,
    "output"   : "output/btagSF_calibration",
    "workflow_extra_options": {},
    "split_eras" :False,
    "triggerSF" : "PocketCoffea/parameters/semileptonic_triggerSF/triggerSF_2018UL_Ele32_EleHT/sf_trigger_electron_etaSC_vs_electron_pt_2018_Ele32_EleHT_pass_v03.coffea",

    # "run_options" : {
    #     "executor"       : "iterative",
    #     "workers"        : 1,
    #     "scaleout"       : 60,
    #     "partition"      : "standard",
    #     "walltime"       : "03:00:00",
    #     "mem_per_worker" : "4GB", # GB
    #     "exclusive"      : False,
    #     "chunk"          : 200000,
    #     "retries"        : 30,
    #     "max"            : None,
    #     "skipbadfiles"   : None,
    #     "voms"           : None,
    #     "limit"          : 1,
    # },
   "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 100,
        "partition"      : "standard",
        "walltime"       : "05:00:00",
        "mem_per_worker" : "10GB", # GB
        "exclusive"      : False,
        "chunk"          : 250000,
        "retries"        : 30,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },


    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [ get_nObj(4, 15., "Jet")],
    "preselections" : [semileptonic_presel_nobtag],

    "categories": {
        "no_btagSF" : [passthrough],
        "btagSF" : [passthrough],
        "btagSF_calib": [passthrough],
        "2b" : [ get_nObj(2, coll="BJetGood")], 
        "3b" :  [ get_nObj(3, coll="BJetGood")],
        "2b_btagSF" : [ get_nObj(2, coll="BJetGood")], 
        "3b_btagSF" :  [ get_nObj(3, coll="BJetGood")],
        "2b_btagSF_calib" : [ get_nObj(2, coll="BJetGood")], 
        "3b_btagSF_calib" :  [ get_nObj(3, coll="BJetGood")],
    },

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup", "sf_ele_reco_id", "sf_mu_id_iso"],
            "bycategory" : {
                "btagSF" : ["sf_btag"],
                "2b_btagSF" : ["sf_btag"],
                "3b_btagSF" : ["sf_btag"],
                "btagSF_calib" : ["sf_btag","sf_btag_calib"],
                "2b_btagSF_calib" : ["sf_btag","sf_btag_calib"],
                "3b_btagSF_calib" : ["sf_btag","sf_btag_calib"],
            }
        },
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
        "jet_btagDeepFlavB" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "njet" : None,
        "nbjet" : None,
        "Ht" : {'binning' : {'n_or_arr' : 100, 'lo' : 0, 'hi' : 2500}, 'xlim':(0, 500), 'xlabel' : "$H_T$ Jets [GeV]"},
    },
     "variables2d" : {
         "Njet_Ht": {
             "Njet": {'binning': {"n_or_arr": [4,5,6,7,8,9,17]}, "xlabel":"N Jets"},
             "Ht": {'binning': {"n_or_arr": [0,500,750,1000,1250,1500, 2000]}, "ylabel":"$H_T$ Jets"}
         }
     },
    "scale" : "log"
}
