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
            "samples": ["ttHTobb","TTToSemiLeptonic"],
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
        "scaleout"       : 30,
        "partition"      : "short",
        "walltime"       : "00:30:00",
        "mem_per_worker" : "5GB", # GB
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
    "skim": [get_nObj(4, 15., "Jet") ],
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
                          "sf_btag", "sf_btag_calib","sf_jet_puId", 
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
        **jet_hists(name="bjet",coll="BJetGood", pos=0),
        **jet_hists(name="bjet",coll="BJetGood", pos=1),
        **jet_hists(name="bjet",coll="BJetGood", pos=2),
        **jet_hists(name="bjet",coll="BJetGood", pos=3),
        **jet_hists(name="bjet",coll="BJetGood", pos=4),

        ##2D plots
        "jet_eta_pt_leading": HistConf(
            [
                Axis(coll="JetGood", field="pt", pos=0, bins=40, start=0, stop=1000,
                     label="Leading jet $p_T$"),
                Axis(coll="JetGood", field="eta", pos=0, bins=40, start=-2.4, stop=2.4,
                     label="Leading jet $\eta$"),
            ]
        ),
        "jet_eta_pt_all": HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=1000,
                     label="Leading jet $p_T$"),
                Axis(coll="JetGood", field="eta", bins=40, start=-2.4, stop=2.4,
                     label="Leading jet $\eta$")
            ]
        ),

        # Metadata of the processing
        "events_per_chunk" : HistConf(
               axes=[
                   Axis(field='nEvents_initial',
                        bins=100, start=0, stop=500000,                       
                        label="Number of events in the chunk",
                        ), 
                   Axis(field='nEvents_after_skim',
                        bins=100, start=0, stop=500000,                       
                        label="Number of events after skim per chunk",
                        ), 
                   Axis(field='nEvents_after_presel',
                        bins=100, start=0, stop=500000,
                        label="Number of events after preselection per chunk",
                        )
               ],
            storage="int64",
            autofill=False,
            metadata_hist = True,
            no_weights=True,
            only_categories=["baseline"],
            variations=False,),
    }
}
