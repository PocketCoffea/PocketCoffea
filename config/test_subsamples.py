from pocket_coffea.parameters.cuts.preselection_cuts import *
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nBtag, get_HLTsel, get_nObj_less
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.columns_manager import ColOut
from pocket_coffea.parameters.btag import btag_variations
from pocket_coffea.lib.weights_manager import WeightCustom
import numpy as np


cfg =  {
    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_local.json",
                  "datasets/backgrounds_MC_TTbb_local.json",
                  "datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/DATA_SingleMuon_local.json",
                  "datasets/DATA_SingleEle.json"],
        "filter" : {
            "samples": [
                # "TTToSemiLeptonic",
                "TTbbSemiLeptonic",
                "ttHTobb",
                "DATA_SingleMu",
                # "DATA_SingleEle"
            ],
            "samples_exclude" : [],
            "year": ['2018']
        },
        "subsamples": {
            "TTbbSemiLeptonic":
            {
                "tt<3b": [get_nObj_less(3, coll="BJetGood")],
                "tt+3b": [get_nObj_eq(3, coll="BJetGood")],
                "tt+4b": [get_nObj_eq(4, coll="BJetGood")],
                "tt>4b": [get_nObj_min(5, coll="BJetGood")],
            },
            "ttHTobb":{
                "ttH<4b": [get_nObj_less(4, coll="BJetGood")],
                "ttH+4b": [get_nObj_min(4, coll="BJetGood")],
            }
        }
    },
    

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test_subsamples",
    "worflow_options" : {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 100,
        "queue"          : "short",
        "walltime"       : "00:40:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 400000,
        "retries"        : 50,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
        "adapt"          : False,
        "env"            : "conda"
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj_min(4, 15., "Jet"),
             get_HLTsel("semileptonic")],
    "preselections" : [semileptonic_presel],
    "categories": {
        "baseline": [passthrough],
    },

    

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_jet_puId", 
                          ],
            "bycategory" : {
            }
        },
        "bysample": {
        }
    },

    "variations": {
        "weights": {
            "common": {
                "inclusive": [  "pileup",
                                "sf_ele_reco", "sf_ele_id",
                                "sf_mu_id", "sf_mu_iso", "sf_jet_puId",
                                *[ f"sf_btag_{b}" for b in btag_variations["2018"]]                               
                              ],
                "bycategory" : {
                }
            },
        "bysample": {
        }    
        },
        
    },

   "variables":
    {

        **ele_hists(coll="ElectronGood", pos=0),
        **muon_hists(coll="MuonGood", pos=0),
    },

    "columns": {
        "common": {
            "inclusive": [ ColOut("JetGood", ["pt", "eta","phi"]),
                           ColOut("ElectronGood", ["pt","eta","phi"])]
        },
        "bysample":{
            "ttHTobb": {
                "inclusive": [ColOut("MuonGood", ["pt", "eta", "phi"])]
            },
            "ttH+4b":{
                "inclusive": [ColOut("BJetGood", ["pt", "eta", "phi"])]
            }
        }
    }
}
