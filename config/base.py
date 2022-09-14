from PocketCoffea.parameters.cuts.baseline_cuts import semileptonic_presel, passthrough
from PocketCoffea.workflows.base import ttHbbBaseProcessor
from PocketCoffea.lib.cut_functions import get_nObj, get_nBtag
from PocketCoffea.parameters.histograms import *

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
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test_base",
    "worflow_options" : {}

    # Executor parameters
        "run_options" : {
        "executor"       : "iterative",
        "workers"        : 1,
        "scaleout"       : 60,
        "partition"      : "standard",
        "walltime"       : "03:00:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 200000,
        "retries"        : 30,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : 1}


    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj(4, 15., "Jet")],
    "preselections" : [semileptonic_presel],
    "categories": {
        "SR" : [passthrough],
        "CR" : [passthrough]
    },


    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup", "sf_ele_reco_id", "sf_mu_id_iso","sf_btag", "sf_btag_calib","sf_jet_puId"],
            "bycategory" : {
            }
        },
        "bysample": {
        }
    },

    "variations": {
        "weights": {
            "common": {
                "inclusive": ["pileup", "sf_ele_reco", "sf_ele_id", "sf_mu_id", "sf_mu_iso", "sf_jet_puId"],
                "bycategory" : {
                }
            },
        "bysample": {
        }    
        },
        
    },

   "variables":
    {
        **default_hists_jet("jet", coll="JetGood"),
        **default_hists_ele("ele", coll="EleGood")

    }
}
