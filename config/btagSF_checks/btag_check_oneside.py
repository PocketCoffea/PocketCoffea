from pocket_coffea.parameters.cuts.preselection_cuts import semileptonic_presel_nobtag, passthrough
from config.parton_matching.functions import *
from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.cut_functions import *
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.parameters.histograms import *
from pocket_coffea.parameters.btag import btag_variations

def get_c_flavour_cut(nc):
    return Cut(
        name = f"N{nc}jets_cflavour",
        params = {},
        function= lambda events, params, **kwargs: ak.sum(events.JetGood.hadronFlavour==4, axis=1) == nc
    )

jet_c0 = get_c_flavour_cut(0)
jet_c1 = get_c_flavour_cut(1)
jet_c2 = get_c_flavour_cut(2)
jet_c3 = get_c_flavour_cut(3)

cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb.json",
                  "datasets/backgrounds_MC_ttbar.json",
                  "datasets/backgrounds_MC.json",
                  "datasets/DATA_SingleMuon.json",
                  "datasets/DATA_SingleEle.json"],
        "filter" : {
            "samples": [
                "ttHTobb",
                "TTToSemiLeptonic",
                        # "TTTo2L2Nu",
                        # "ST_s-channel_4f_leptonDecays",
                        #  "ST_t-channel_top_4f_InclusiveDecays",
                        #  "ST_t-channel_antitop_4f_InclusiveDecays",
                        #  "ST_tW_top_5f_NoFullyHadronicDecays",
                        #  "ST
                         # _tW_antitop_5f_NoFullyHadronicDecays",
                 # "DATA_SingleEle",
                  # "DATA_SingleMuon"
                        ],
            "samples_exclude" : [],
            "year": ["2018"]
        },
        "subsamples" : {'DATA_SingleEle'  : {'DATA_SingleEle' : [get_HLTsel("semileptonic", primaryDatasets=["SingleEle"])]},
                        'DATA_SingleMuon' : {'DATA_SingleMuon' : [get_HLTsel("semileptonic", primaryDatasets=["SingleMuon"]),
                                                        get_HLTsel("semileptonic", primaryDatasets=["SingleEle"], invert=True)]}
             }
    },

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/sf_btag_checks",
    "workflow_extra_options": {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 300,
        "queue"          : "standard",
        "walltime"       : "03:00:00",
        "mem_per_worker" : "6GB", # GB
        "exclusive"      : False,
        "chunk"          : 250000,
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
    "skim": [ get_nObj_min(4, 15., "Jet"),
             get_HLTsel("semileptonic")],
    "preselections" : [semileptonic_presel_nobtag],
    
    "categories": {
        "no_btagSF" : [passthrough],
        "btagSF" : [passthrough],
        "btagSF_calib":[passthrough],
        
        "1b": [get_nBtagEq(1, coll="BJetGood")],
        "2b": [get_nBtagEq(2, coll="BJetGood")],
        "3b": [get_nBtagEq(3, coll="BJetGood")],
        ">=4b": [get_nBtagMin(4, coll="BJetGood")],
        "1b_btagSF": [get_nBtagEq(1, coll="BJetGood")],
        "2b_btagSF": [get_nBtagEq(2, coll="BJetGood")],
        "3b_btagSF": [get_nBtagEq(3, coll="BJetGood")],
        ">=4b_btagSF": [get_nBtagMin(4, coll="BJetGood")],
        "1b_btagSF_calib": [get_nBtagEq(1, coll="BJetGood")],
        "2b_btagSF_calib": [get_nBtagEq(2, coll="BJetGood")],
        "3b_btagSF_calib": [get_nBtagEq(3, coll="BJetGood")],
        ">=4b_btagSF_calib": [get_nBtagMin(4, coll="BJetGood")],

         "1b0c_btagSF": [get_nBtagEq(1, coll="BJetGood"), jet_c0],
        "2b0c_btagSF": [get_nBtagEq(2, coll="BJetGood"), jet_c0],
        "3b0c_btagSF": [get_nBtagEq(3, coll="BJetGood"), jet_c0],
        ">=4b0c_btagSF": [get_nBtagMin(4, coll="BJetGood"), jet_c0],

        "1b1c_btagSF": [get_nBtagEq(1, coll="BJetGood"), jet_c1],
        "2b1c_btagSF": [get_nBtagEq(2, coll="BJetGood"), jet_c1],
        "3b1c_btagSF": [get_nBtagEq(3, coll="BJetGood"), jet_c1],
        ">=4b1c_btagSF": [get_nBtagMin(4, coll="BJetGood"), jet_c1],
        
        "1b2c_btagSF": [get_nBtagEq(1, coll="BJetGood"), jet_c2],
        "2b2c_btagSF": [get_nBtagEq(2, coll="BJetGood"), jet_c2],
        "3b2c_btagSF": [get_nBtagEq(3, coll="BJetGood"), jet_c2],
        ">=4b2c_btagSF": [get_nBtagMin(4, coll="BJetGood"), jet_c2],
        
        "1b0c_btagSF_calib": [get_nBtagEq(1, coll="BJetGood"), jet_c0],
        "2b0c_btagSF_calib": [get_nBtagEq(2, coll="BJetGood"), jet_c0],
        "3b0c_btagSF_calib": [get_nBtagEq(3, coll="BJetGood"), jet_c0],
        ">=4b0c_btagSF_calib": [get_nBtagMin(4, coll="BJetGood"), jet_c0],

        "1b1c_btagSF_calib": [get_nBtagEq(1, coll="BJetGood"), jet_c1],
        "2b1c_btagSF_calib": [get_nBtagEq(2, coll="BJetGood"), jet_c1],
        "3b1c_btagSF_calib": [get_nBtagEq(3, coll="BJetGood"), jet_c1],
        ">=4b1c_btagSF_calib": [get_nBtagMin(4, coll="BJetGood"), jet_c1],
        
        "1b2c_btagSF_calib": [get_nBtagEq(1, coll="BJetGood"), jet_c2],
        "2b2c_btagSF_calib": [get_nBtagEq(2, coll="BJetGood"), jet_c2],
        "3b2c_btagSF_calib": [get_nBtagEq(3, coll="BJetGood"), jet_c2],
        ">=4b2c_btagSF_calib": [get_nBtagMin(4, coll="BJetGood"), jet_c2]

        
        
    },

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup", "sf_jet_puId",
                          "sf_ele_id", "sf_ele_reco", "sf_ele_trigger",
                          "sf_mu_id", "sf_mu_iso", "sf_mu_trigger"],
             "bycategory" : {
                "btagSF" : ["sf_btag"],
                 "btagSF_calib" : ["sf_btag", "sf_btag_calib"],
                 "1b_btagSF": ["sf_btag"],
                 "2b_btagSF": ["sf_btag"],
                 "3b_btagSF": ["sf_btag"],
                 ">=4b_btagSF": ["sf_btag"],
                 "1b_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "2b_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "3b_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 ">=4b_btagSF_calib": ["sf_btag","sf_btag_calib"],

                 "1b0c_btagSF": ["sf_btag"],
                 "2b0c_btagSF": ["sf_btag"],
                 "3b0c_btagSF": ["sf_btag"],
                 ">=4b0c_btagSF": ["sf_btag"],
                  "1b1c_btagSF": ["sf_btag"],
                 "2b1c_btagSF": ["sf_btag"],
                 "3b1c_btagSF": ["sf_btag"],
                 ">=4b1c_btagSF": ["sf_btag"],
                  "1b2c_btagSF": ["sf_btag"],
                 "2b2c_btagSF": ["sf_btag"],
                 "3b2c_btagSF": ["sf_btag"],
                 ">=4b2c_btagSF": ["sf_btag"],
                 
                 "1b0c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "2b0c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "3b0c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 ">=4b0c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                  "1b1c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "2b1c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "3b1c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 ">=4b1c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                  "1b2c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "2b2c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 "3b2c_btagSF_calib": ["sf_btag","sf_btag_calib"],
                 ">=4b2c_btagSF_calib": ["sf_btag","sf_btag_calib"],
             }
        },
    },
    "variations": {
        "weights": {
            "common": {
                "inclusive": [ "sf_jet_puId"],
                "bycategory": {
                    "btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                     "1b_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b0c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b0c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b0c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b0c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b1c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b1c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b1c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b1c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b2c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b2c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b2c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b2c_btagSF_calib": [ f"sf_btag_{b}" for b in btag_variations["2018"]],

                     "1b0c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b0c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b0c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b0c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b1c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b1c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b1c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b1c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "1b2c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "2b2c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    "3b2c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]],
                    ">=4b2c_btagSF": [ f"sf_btag_{b}" for b in btag_variations["2018"]]
                }
            }
        },
        "shape":{
            "common": {
                "inclusive": ["JER", 
                              "JES_Total"
                              ]
            }
            
        }
    },

    "variables": {
        
       # **jet_hists(name="jet",coll="JetGood"),
        #**jet_hists(name="bjet", coll="BJetGood"),
        **count_hist(name="nJets", coll="JetGood",bins=20, start=0, stop=20),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **count_hist(name="nCJets_gen", coll="JetGoodCFlavour",bins=10, start=0, stop=10),
        **count_hist(name="nBJets_gen", coll="JetGoodBFlavour",bins=10, start=0, stop=10),
        **jet_hists(name="jet", coll="JetGood", pos=0),
        **jet_hists(name="jet", coll="JetGood", pos=1),
        **jet_hists(name="jet", coll="JetGood", pos=2),
        **jet_hists(name="jet", coll="JetGood", pos=3),
        **jet_hists(name="jet", coll="JetGood", pos=4),
        **jet_hists(name="bjet",coll="BJetGood", pos=0),
        **jet_hists(name="bjet",coll="BJetGood", pos=1),
        **jet_hists(name="bjet",coll="BJetGood", pos=2),
        **jet_hists(name="bjet",coll="BJetGood", pos=3),
        **jet_hists(name="bjet",coll="BJetGood", pos=4),

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

# cfg["variables"] = { k:v  for k , v in cfg["variables"].items() if k in ["nCJets_gen", "nBJets_gen"]}
