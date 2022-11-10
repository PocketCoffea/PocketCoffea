from pocket_coffea.parameters.cuts.baseline_cuts import semileptonic_presel, passthrough
from pocket_coffea.workflows.base import ttHbbBaseProcessor

cfg =  {

    "dataset" : {
        "jsons": ["datasets/RunIISummer20UL18_t3_local.json"],
        "filter" : {
            "samples": ["ttHTobb", "TTToSemiLeptonic", "TTTo2L2Nu",
                        "ST_s-channel_4f_leptonDecays",
                        "ST_t-channel_top_4f_InclusiveDecays", "ST_t-channel_antitop_4f_InclusiveDecays",
                        "ST_tW_top_5f_NoFullyHadronicDecays", "ST_tW_antitop_5f_NoFullyHadronicDecays"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/semileptonic/semileptonic_ST",

    # Executor parameters
    "run_options" : {
        "executor"       : "futures",
        "workers"        : 12,
        "scaleout"       : 10,
        "partition"      : "standard",
        "walltime"       : "12:00:00",
        "mem_per_worker" : None, # GB
        "exclusive"      : True,
        "chunk"          : 50000,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : 2,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "preselections" : [semileptonic_presel],
    "categories": {
        "SR" : [passthrough],
        "CR" : [passthrough]
    },
    
    "variables" : {
        "muon_pt" : {'binning' : {'n_or_arr' : 200, 'lo' : 0, 'hi' : 2000}, 'xlim' : (0,500),  'xlabel' : "$p_{T}^{\mu}$ [GeV]"},
        "muon_eta" : None,
        "muon_phi" : None,
        "electron_pt" : None,
        "electron_eta" : None,
        "electron_phi" : None,
        "jet_pt" : None,
        "jet_eta" : None,
        "jet_phi" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "njet" : None,
        "nbjet" : None,
    },
    "variables2d" : {},
    "scale" : "log"
}
