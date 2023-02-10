from pocket_coffea.parameters.cuts.preselection_cuts import *
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nBtag, get_HLTsel
from pocket_coffea.parameters.histograms import *
from pocket_coffea.parameters.btag import btag_variations
from pocket_coffea.lib.weights_manager import WeightCustom
from pocket_coffea.lib.categorization import StandardSelection, CartesianSelection, MultiCut
from pocket_coffea.lib.cut_definition import Cut
import numpy as np


jet_eta_cut = lambda events, params, **kwargs: abs(events[params["coll"]].eta) <= params["eta_max"]

cfg =  {
    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_local.json",
                  "datasets/backgrounds_MC_TTbb_local.json",
                  "datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/DATA_SingleMuon_local.json",
                  "datasets/DATA_SingleEle.json"],
        "filter" : {
            "samples": [
                 "TTToSemiLeptonic",
                # "TTbbSemiLeptonic",
                "ttHTobb",
                #"DATA_SingleMu", "DATA_SingleEle"
            ],
            "samples_exclude" : [],
            "year": ['2018']
        },
    },
    

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test_categorization",
    "workflow_options" : {},

    "run_options" : {
        "executor"       : "futures",
        "workers"        : 8,
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
        "limit"          : 8,
        "adapt"          : False,
        "env"            : "conda"
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj_min(4, 15., "Jet"),
             get_HLTsel("semileptonic")],
    "preselections" : [semileptonic_presel],


    "categories": CartesianSelection(
        multicuts = [
            MultiCut(name="Njets",
                     cuts=[
                         get_nObj_eq(4, 15., "JetGood"),
                         get_nObj_eq(5, 15., "JetGood"),
                         get_nObj_min(6, 15., "JetGood"),
                     ],
                     cuts_names=["4j","5j","6j"]),
            MultiCut(name="Nbjet",
                    cuts=[
                        Cut("jet_eta",{"coll":"JetGood", "eta_max":0.5}, jet_eta_cut, collection="JetGood"),
                        Cut("jet_eta",{"coll":"JetGood", "eta_max":1}, jet_eta_cut, collection="JetGood"),
                        Cut("jet_eta",{"coll":"JetGood", "eta_max":1.5}, jet_eta_cut, collection="JetGood")
                    ],
                     cuts_names=["jeta0.5", "jeta1","jeta1.5"])
        ],
        common_cats = StandardSelection({
            "inclusive": [passthrough],
            "4jets_40pt" : [get_nObj_min(4, 40., "JetGood")]
        })
    ),

    

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso", 
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
                                "sf_mu_id", "sf_mu_iso",                             
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
        **jet_hists(coll="JetGood", pos=0),
        **count_hist(name="nJets", coll="JetGood",bins=10, start=4, stop=14),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        
    }
}
