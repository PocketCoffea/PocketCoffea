from PocketCoffea.parameters.cuts.baseline_cuts import semileptonic_triggerSF_presel, passthrough
from config.semileptonic_triggerSF.functions import get_trigger_passfail
from PocketCoffea.workflows.semileptonic_triggerSF import semileptonicTriggerProcessor

cfg =  {

    "dataset" : {
        "jsons": ["datasets/backgrounds_MC_local.json", "datasets/DATA_SingleMuon_local.json"],
        "filter" : {
            "samples": ["TTToSemiLeptonic", "TTTo2L2Nu", "DATA"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : semileptonicTriggerProcessor,
    "output"   : "output/semileptonic_triggerSF",

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
    "finalstate" : "semileptonic_triggerSF",
    "preselections" : [semileptonic_triggerSF_presel],
    "categories": {
        "Ele32_WPTight_Gsf_pass" : [get_trigger_passfail("Ele32_WPTight_Gsf", "pass")],
        "Ele32_WPTight_Gsf_fail" : [get_trigger_passfail("Ele32_WPTight_Gsf", "fail")],
        "inclusive" : [passthrough],
    },

    # List of triggers for SF measurement
    "triggers_to_measure" : ["Ele32_WPTight_Gsf"],
    
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
