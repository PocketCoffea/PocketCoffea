from PocketCoffea.parameters.cuts.baseline_cuts import semileptonic_presel, passthrough
from PocketCoffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nBtag
from PocketCoffea.lib.cut_definition import Cut
from PocketCoffea.workflows.lhe_categories_signal_studies  import LHESignalStudies
from PocketCoffea.parameters.histograms import *
from config.lhe_signal_studies.functions import *

cfg =  {

    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_2018_local.json",
                  "datasets/backgrounds_MC_2018_local.json"],
        "filter" : {
            "samples": ["ttHTobb"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : LHESignalStudies,
    "output"   : "output/lhe_categories_signal_studied",
    "workflow_extra_options": {"lep_lhe_min_dR": 0.4},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 30,
        "partition"      : "short",
        "walltime"       : "01:00:00",
        "mem_per_worker" : "6GB", # GB
        "exclusive"      : False,
        "chunk"          : 500000,
        "retries"        : 30,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },


    # Cuts and plots settings
    "finalstate" : "semileptonic",

    "skim": [],
    "preselections" : [passthrough],

    "categories": {
        "incl": [passthrough],
        "presel" : [semileptonic_presel],

        "presel_semilep": [semilep_lhe, semileptonic_presel],
        "presel_dilep": [dilep_lhe, semileptonic_presel],
        "presel_had": [had_lhe, semileptonic_presel],

        "1lep": [get_nObj_eq(1, minpt=26, coll="LeptonGood") ],
        "1ele": [get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1mu":  [get_nObj_eq(1, minpt=26, coll="MuonGood") ],

        "0lep_semilep": [semilep_lhe, get_nObj_eq(0, coll="LeptonGood") ], 
        "0lep_dilep":   [dilep_lhe,   get_nObj_eq(0, coll="LeptonGood") ],
        "0lep_had":     [had_lhe,     get_nObj_eq(0, coll="LeptonGood") ],
        
        "1ele_semilep": [semilep_lhe, get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1ele_dilep":   [dilep_lhe,   get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1ele_had":     [had_lhe,     get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1mu_semilep": [semilep_lhe, get_nObj_eq(1, minpt=26, coll="MuonGood") ], 
        "1mu_dilep":   [dilep_lhe,   get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1mu_had":     [had_lhe,     get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1lep_semilep": [semilep_lhe, get_nObj_eq(1, minpt=26, coll="LeptonGood") ], 
        "1lep_dilep":   [dilep_lhe,   get_nObj_eq(1, minpt=26, coll="LeptonGood") ],      
        "1lep_had":     [had_lhe,     get_nObj_eq(1, minpt=26, coll="LeptonGood") ],


        "1ele_semilep_notau": [semilep_notau_lhe, get_nObj_eq(1, minpt=30, coll="ElectronGood") ], 
        "1muon_semilep_notau": [semilep_notau_lhe, get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1lep_semilep_notau":   [semilep_notau_lhe,   get_nObj_eq(1, minpt=26, coll="LeptonGood") ],
        
        "1ele_dilep_notau":   [dilep_notau_lhe,   get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1mu_dilep_notau":   [dilep_notau_lhe,   get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1lep_dilep_notau":   [dilep_notau_lhe,   get_nObj_eq(1, minpt=26, coll="LeptonGood") ],

        "1ele_dilep_onetau":   [dilep_onetau_lhe,   get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1mu_dilep_onetau":   [dilep_onetau_lhe,   get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1lep_dilep_onetau":   [dilep_onetau_lhe,   get_nObj_eq(1, minpt=26, coll="LeptonGood") ],

        "1ele_dilep_twotau":   [dilep_twotau_lhe,   get_nObj_eq(1, minpt=30, coll="ElectronGood") ],
        "1mu_dilep_twotau":   [dilep_twotau_lhe,   get_nObj_eq(1, minpt=26, coll="MuonGood") ],
        "1lep_dilep_twotau":   [dilep_twotau_lhe,   get_nObj_eq(1, minpt=26, coll="LeptonGood") ],
        
        "2lep":         [dilep_custom_selection], 
        "2lep_semilep": [semilep_lhe, dilep_custom_selection], 
        "2lep_dilep":   [dilep_lhe,   dilep_custom_selection],
        "2lep_had":     [had_lhe,     dilep_custom_selection],
        
    },

   
    "variables" : {

        **ele_hists(name="allEle", coll="Electron", pos=0),
        **ele_hists(name="allEle", coll="Electron", pos=1),
        **muon_hists(name="allMuon", coll="Muon", pos=0),
        **muon_hists(name="allMuon", coll="Muon", pos=1),
        
        **ele_hists(coll="ElectronGood", pos=0),
        **ele_hists(coll="ElectronGood", pos=1),
        **muon_hists(coll="MuonGood", pos=0),
        **muon_hists(coll="MuonGood", pos=1),
        **lepton_hists(coll="LeptonGood", pos=0),
        **lepton_hists(coll="LeptonGood", pos=1),

        **ele_hists(coll="LHE_Electron", pos=0),
        **ele_hists(coll="LHE_Electron", pos=1),
        **muon_hists(coll="LHE_Muon", pos=0),
        **muon_hists(coll="LHE_Muon", pos=1),
        **lepton_hists(coll="LHE_Tau", pos=0),
        **lepton_hists(coll="LHE_Tau", pos=1),
        **lepton_hists(coll="LHE_Lepton", pos=0),
        **lepton_hists(coll="LHE_Lepton", pos=1),
        # Reco not-cleaned leptons matched to the LHE candidate
        **ele_hists(coll="LHE_matched_ele", pos=0),
        **ele_hists(coll="LHE_matched_ele", pos=1),
        **muon_hists(coll="LHE_matched_muon", pos=0),
        **muon_hists(coll="LHE_matched_muon", pos=1),
        **lepton_hists(coll="LHE_matched_tau", pos=0),
        **lepton_hists(coll="LHE_matched_tau", pos=1),

        "lhe_matched_ele_1_mvaId" : HistConf([Axis(coll="LHE_matched_ele", field="mvaFall17V2Iso_WP80", pos=0, type="int", start=0, stop=2, label="mvaid")]),
        "lhe_matched_ele_2_mvaId" : HistConf([Axis(coll="LHE_matched_ele", field="mvaFall17V2Iso_WP80", pos=1, type="int", start=0, stop=2, label="mvaid")]),
        "lhe_matched_muon_1_mvaId" : HistConf([Axis(coll="LHE_matched_muon", field="tightId", pos=0, type="int", start=0, stop=2, label="mvaid")]),
        "lhe_matched_muon_2_mvaId" : HistConf([Axis(coll="LHE_matched_muon", field="tightId", pos=1, type="int", start=0, stop=2, label="mvaid")]),
        
        **count_hist(name="nele_lhe", coll="LHE_Electron",bins=4, start=0, stop=4),
        **count_hist(name="nmuon_lhe", coll="LHE_Muon",bins=4, start=0, stop=4),
        **count_hist(name="ntau_lhe", coll="LHE_Muon",bins=4, start=0, stop=4),
        **count_hist(name="nlepton_lhe", coll="LHE_Lepton",bins=4, start=0, stop=4),
        
        **count_hist(name="nele_incl", coll="ElectronGood",bins=8, start=0, stop=8),
        **count_hist(name="nmuon_incl", coll="MuonGood",bins=8, start=0, stop=8),
        **count_hist(name="nlepton_incl", coll="LeptonGood",bins=8, start=0, stop=8),
        **count_hist(name="nJets", coll="JetGood",bins=14, start=0, stop=14),


        "nlepLHE_nlepReco" : HistConf(
            [Axis(field="nLHE_Lepton", start=0, stop=6, label="Number of LHE Leptons", type="int"),
             Axis(field="nLHE_Electron", start=0, stop=6, label="Number of LHE Electron",type="int"),
             Axis(field="nLHE_Muon", start=0, stop=6, label="Number of LHE Muon",type="int"),
             Axis(field="nLHE_Tau", start=0, stop=6, label="Number of LHE Tau",type="int"),
             Axis(field="nLeptonGood", start=0, stop=6, label="Number of Lepton Good",type="int"),
             Axis(field="nElectronGood", start=0, stop=6, label="Number of Lepton Good",type="int"),
             Axis(field="nMuonGood", start=0, stop=6, label="Number of Lepton Good",type="int")],
            only_categories=["1lep","1lep_semilep_notau","1"]
        ),

    },
    
       "weights": {
        "common": { 
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_jet_puId", 
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
                "inclusive": [ ],
                "bycategory" : {
                }
            },
        "bysample": {
        }    
        },
        
    },
 
}
