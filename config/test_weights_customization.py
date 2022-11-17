from pocket_coffea.parameters.cuts.baseline_cuts import dilepton_presel, semileptonic_presel, passthrough
from pocket_coffea.lib.cut_functions import get_nObj, get_nBtag
from pocket_coffea.lib.cut_definition import Cut
from config.parton_matching.functions import *
from pocket_coffea.workflows.base import ttHbbBaseProcessor
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
    "output"   : "output/test_weights_custom",
    "workflow_extra_options": {},
    "split_eras" :False,
     "triggerSF" : "pocket_coffea/parameters/semileptonic_triggerSF/triggerSF_2018UL_Ele32_EleHT/sf_trigger_electron_etaSC_vs_electron_pt_2018_Ele32_EleHT_pass_v03.coffea",

    "run_options" : {
        "executor"       : "futures",
        "workers"        : 1,
        "scaleout"       : 60,
        "partition"      : "standard",
        "walltime"       : "03:00:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 100000,
        "retries"        : 30,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : 2,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [ get_nObj(4, 20., "Jet"),
              get_nBtag(3, 20., "Jet") ],
    "preselections" : [semileptonic_presel],

    "categories": {
        "inclusive" : [passthrough],
        "ele_reco_id" : [passthrough]
    },


    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup","sf_mu_id_iso"],
            "bycategory" : {    
            }
        },
        "bysample": {
            "ttHTobb" : {
                "bycategory" : {
                    "ele_reco_id": ["sf_ele_reco_id"],
                }
            }
        }
    },

    
    "variables" : {
        **jet_hists(coll="JetGood"),
        **jet_hists(coll="BJetGood"),
        **ele_hists(coll="ElectronGood"),
        **muon_hists(coll="MuonGood"),
        **count_hist(name="nJets", coll="JetGood",bins=10, start=4, stop=14),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **jet_hists(coll="JetGood", pos=0),
        **jet_hists(coll="JetGood", pos=1),
        **jet_hists(coll="JetGood", pos=2),
        **jet_hists(coll="JetGood", pos=3),
        **jet_hists(coll="JetGood", pos=4),
        **jet_hists(name="bjet",coll="BJetGood", pos=0),
        **jet_hists(name="bjet",coll="BJetGood", pos=1),
        **jet_hists(name="bjet",coll="BJetGood", pos=2),
        **jet_hists(name="bjet",coll="BJetGood", pos=3),
        **jet_hists(name="bjet",coll="BJetGood", pos=4),

     },
}
