from pocket_coffea.parameters.cuts.preselection_cuts import *
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_HLTsel, get_nBtag, get_nElectron, get_nMuon
from pocket_coffea.parameters.histograms import *
from pocket_coffea.parameters.btag import btag_variations
from pocket_coffea.parameters.lepton_scale_factors import sf_ele_trigger_variations
from config.datamc.plots import cfg_plot

samples = ["ttHTobb",
           "TTToSemiLeptonic",
           "TTTo2L2Nu",
           "ST_s-channel_4f_leptonDecays",
           "ST_t-channel_top_4f_InclusiveDecays",
           "ST_t-channel_antitop_4f_InclusiveDecays",
           "ST_tW_top_5f_NoFullyHadronicDecays",
           "ST_tW_antitop_5f_NoFullyHadronicDecays",
           "DATA_SingleEle",
           "DATA_SingleMuon"]

subsamples = {'DATA_SingleEle'  : {'DATA_SingleEle' : [get_HLTsel("semileptonic", primaryDatasets=["SingleEle"])]},
              'DATA_SingleMuon' : {'DATA_SingleMuon' : [get_HLTsel("semileptonic", primaryDatasets=["SingleMuon"]),
                                                        get_HLTsel("semileptonic", primaryDatasets=["SingleEle"], invert=True)]}
             }

cfg =  {
    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_local.json",
                  "datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/backgrounds_MC_local.json",
                  "datasets/DATA_SingleMuon_local.json",
                  "datasets/DATA_SingleEle_local.json",],
        "filter" : {
            "samples": samples,
            "samples_exclude" : [],
            "year": ['2018']
        },
        "subsamples": subsamples
    },

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/datamc/ttHbb_ttbar_ST_datamc_btagcalibrated_SingleMuon_bugfix",
    "worflow_options" : {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 150,
        "queue"          : "standard",
        "walltime"       : "12:00:00",
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
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj_min(4, 15., "Jet"),
             get_HLTsel("semileptonic", primaryDatasets=["SingleEle", "SingleMuon"]) ],
    "preselections" : [semileptonic_presel_nobtag],
    "categories": {
        "baseline": [passthrough],
        "SingleEle_1b" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(1, coll="BJetGood") ],
        "SingleEle_2b" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(2, coll="BJetGood") ],
        "SingleEle_3b" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(3, coll="BJetGood") ],
        "SingleEle_4b" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(4, coll="BJetGood") ],
        "SingleMuon_1b" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(1, coll="BJetGood") ],
        "SingleMuon_2b" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(2, coll="BJetGood") ],
        "SingleMuon_3b" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(3, coll="BJetGood") ],
        "SingleMuon_4b" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(4, coll="BJetGood") ],
        "SingleEle_1b_btagcalibrated" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(1, coll="BJetGood") ],
        "SingleEle_2b_btagcalibrated" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(2, coll="BJetGood") ],
        "SingleEle_3b_btagcalibrated" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(3, coll="BJetGood") ],
        "SingleEle_4b_btagcalibrated" : [ get_nElectron(1, coll="ElectronGood"), get_nBtag(4, coll="BJetGood") ],
        "SingleMuon_1b_btagcalibrated" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(1, coll="BJetGood") ],
        "SingleMuon_2b_btagcalibrated" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(2, coll="BJetGood") ],
        "SingleMuon_3b_btagcalibrated" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(3, coll="BJetGood") ],
        "SingleMuon_4b_btagcalibrated" : [ get_nMuon(1, coll="MuonGood"), get_nBtag(4, coll="BJetGood") ]
    },

    

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id", "sf_ele_trigger",
                          "sf_mu_id","sf_mu_iso", "sf_mu_trigger",
                          "sf_btag",
                          "sf_jet_puId"
                          ],
            "bycategory" : {
                "SingleEle_1b_btagcalibrated" : ["sf_btag_calib"],
                "SingleEle_2b_btagcalibrated" : ["sf_btag_calib"],
                "SingleEle_3b_btagcalibrated" : ["sf_btag_calib"],
                "SingleEle_4b_btagcalibrated" : ["sf_btag_calib"],
                "SingleMuon_1b_btagcalibrated" : ["sf_btag_calib"],
                "SingleMuon_2b_btagcalibrated" : ["sf_btag_calib"],
                "SingleMuon_3b_btagcalibrated" : ["sf_btag_calib"],
                "SingleMuon_4b_btagcalibrated" : ["sf_btag_calib"],
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
                                "sf_mu_id", "sf_mu_iso", "sf_mu_trigger",
                                "sf_jet_puId"
                              ] + [ f"sf_btag_{b}" for b in btag_variations["2018"]]
                              + [f"sf_ele_trigger_{v}" for v in sf_ele_trigger_variations["2018"]],
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
        **ele_hists(coll="ElectronGood", pos=1),
        **muon_hists(coll="MuonGood", pos=0),
        **muon_hists(coll="MuonGood", pos=1),
        **count_hist(name="nLepton", coll="JetGood",bins=10, start=4, stop=14),
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

    "plot_options": cfg_plot
}
