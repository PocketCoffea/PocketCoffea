from PocketCoffea.parameters.cuts.baseline_cuts import *
from PocketCoffea.workflows.base import ttHbbBaseProcessor
from PocketCoffea.lib.cut_functions import get_nObj, get_nBtag
from PocketCoffea.parameters.histograms import *
from PocketCoffea.parameters.btag import btag_variations

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
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test_base",
    "worflow_options" : {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 100,
        "partition"      : "standard",
        "walltime"       : "05:00:00",
        "mem_per_worker" : "10GB", # GB
        "exclusive"      : False,
        "chunk"          : 400000,
        "retries"        : 30,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj(4, 15., "Jet")],
    "preselections" : [semileptonic_presel_nobtag],
    "categories": {
        "baseline": [passthrough],
        "1b" : [ get_nBtag(1, coll="BJetGood")],
        "2b" : [ get_nBtag(2, coll="BJetGood")],
        "3b" : [ get_nBtag(3, coll="BJetGood")],
        "4b" : [ get_nBtag(4, coll="BJetGood")]
    },


    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_btag_calib","sf_jet_puId"
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
                "inclusive": [ "pileup", "sf_ele_reco", "sf_ele_id",
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
        **jet_hists(coll="JetGood"),
        **jet_hists(coll="BJetGood"),
        **ele_hists(coll="ElectronGood"),
        **muon_hists(coll="MuonGood"),
        **count_hist("nJets", coll="JetGood",bins=10, start=4, stop=14),
        **count_hist("nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **jet_hists(coll="JetGood", pos=0),
        **jet_hists(coll="JetGood", pos=1),
        **jet_hists(coll="JetGood", pos=2),
        **jet_hists(coll="JetGood", pos=3),
        **jet_hists(coll="JetGood", pos=4),
        **jet_hists(coll="BJetGood", pos=0),
        **jet_hists(coll="BJetGood", pos=1),
        **jet_hists(coll="BJetGood", pos=2),
        **jet_hists(coll="BJetGood", pos=3),
        **jet_hists(coll="BJetGood", pos=4),
    }
}
