from pocket_coffea.parameters.cuts.preselection_cuts import semileptonic_presel_nobtag, passthrough
from config.parton_matching.functions import *
from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nBtag
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.parameters.histograms import *

cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_local.json",
                  "datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/backgrounds_MC_local.json"],
        "filter" : {
            "samples": ["ttHTobb",
                        "TTToSemiLeptonic",
                        "TTTo2L2Nu",
                        "SingleTop",
                        "WJetsToLNu_HT",
                        ],
            "samples_exclude" : [],
            "year": ["2017"]
        }
    },

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/sf_btag_calib/btagSF_calibrated_2017",
    "workflow_extra_options": {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 300,
        "queue"          : "standard",
        "walltime"       : "06:00:00",
        "mem_per_worker" : "6GB", # GB
        "exclusive"      : False,
        "chunk"          : 400000,
        "retries"        : 30,
        "treereduction"  : 20,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
        "adapt"          : False,
    },


    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [ get_nObj_min(4, 15., "Jet")],
    "preselections" : [semileptonic_presel_nobtag],

    "categories": {
        "no_btagSF" : [passthrough],
        "btagSF" : [passthrough],
        "btagSF_calib" : [passthrough],
    },

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup",
                          "sf_ele_id", "sf_ele_reco", "sf_ele_trigger",
                          "sf_mu_id", "sf_mu_iso", "sf_mu_trigger"],
             "bycategory" : {
                "btagSF" : ["sf_btag"],
                "btagSF_calib": ["sf_btag","sf_btag_calib"]
            }
        },
    },
    "variations": {
        "weights": {
            "common": {
                "inclusive": [],
                "bycategory": {}
            }
        }
    },

    "variables": {
        
        **jet_hists(name="jet",coll="JetGood"),
        **jet_hists(name="bjet", coll="BJetGood"),
        **count_hist(name="nJets", coll="JetGood",bins=20, start=0, stop=20),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **jet_hists(name="jet", coll="JetGood", pos=0),
        **jet_hists(name="jet", coll="JetGood", pos=1),
        **jet_hists(name="jet", coll="JetGood", pos=2),
        **jet_hists(name="jet", coll="JetGood", pos=3),
        **jet_hists(name="jet", coll="JetGood", pos=4),
        **jet_hists(name="bjet",coll="BJetGood", pos=0),
        **jet_hists(name="bjet",coll="BJetGood", pos=1),
        **jet_hists(name="bjet",coll="BJetGood", pos=2),
        **jet_hists(name="bjet",coll="BJetGood", pos=3),

        "jets_Ht" : HistConf(
          [Axis(coll="events", field="JetGood_Ht", bins=100, start=0, stop=2500,
                label="Jets $H_T$ [GeV]")]  
        ),
        
        # 2D plots
        "Njet_Ht": HistConf(
            [
                Axis(coll="events", field="nJetGood",bins=[4,5,6,7,8,9,11,20],
                     type="variable",   label="N. Jets (good)"),
                Axis(coll="events", field="JetGood_Ht",
                     bins=[0,500,650,800,1000,1200,1400,1600, 1800, 2000, 5000],
                     type="variable",
                     label="Jets $H_T$ [GeV]"),
            ]
        ),

        "Njet_Ht_finerbins": HistConf(
            [
                Axis(coll="events", field="nJetGood",bins=[4,5,6,7,8,9,11,20],
                     type="variable",   label="N. Jets (good)"),
                Axis(coll="events", field="JetGood_Ht",
                     bins=[0,300,500,600,700,800,900, 1000,1100, 1200, 1300,
                           1400,1600, 1800, 2000, 3500, 5000],
                     type="variable",
                     label="Jets $H_T$ [GeV]"),
            ]
        ),
    }
}
