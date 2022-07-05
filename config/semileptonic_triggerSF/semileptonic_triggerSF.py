from PocketCoffea.parameters.cuts.baseline_cuts import semileptonic_triggerSF_presel, passthrough
from config.semileptonic_triggerSF.functions import get_trigger_passfail
from PocketCoffea.workflows.semileptonic_triggerSF import semileptonicTriggerProcessor
from math import pi

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
    "output"   : "output/semileptonic_triggerSF_2018_lumimask",

    # Executor parameters
    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 200,
        "partition"      : "short",
        "walltime"       : "1:00:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 100000,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic_triggerSF",
    "preselections" : [semileptonic_triggerSF_presel],
    "categories": {
        "Ele32_EleHT_pass" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "pass")],
        "Ele32_EleHT_fail" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "fail")],
        "inclusive" : [passthrough],
    },

    # List of triggers for SF measurement
    "triggers_to_measure" : ["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"],
    
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
    "variables2d" : {
        'electron_etaSC_vs_electron_pt' : {
            'electron_pt'               : {'binning' : {'n_or_arr' : [0, 25, 35, 45, 55, 100, 500, 2000]},  'xlim' : (20,500),    'xlabel' : "Electron $p_{T}$ [GeV]",
                                           'xticks' : [20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500]},
            'electron_etaSC'            : {'binning' : {'n_or_arr' : [-2.5, -1.5660, -1.4442, -0.8, 0.0, 0.8, 1.4442, 1.5660, 2.5]}, 'ylim' : (-2.5,2.5), 'ylabel' : "Electron Supercluster $\eta$"},
        },
        'electron_phi_vs_electron_pt' : {
            'electron_pt'               : {'binning' : {'n_or_arr' : [0, 25, 35, 45, 55, 100, 500, 2000]},  'xlim' : (20,500),    'xlabel' : "Electron $p_{T}$ [GeV]",
                                           'xticks' : [20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500]},
            'electron_phi'              : {'binning' : {'n_or_arr' : 12, 'lo' : -pi, 'hi' : pi}, 'ylim' : (-pi,pi), 'ylabel' : "$\phi_{e}$"},
        },
        'electron_etaSC_vs_electron_phi' : {
            'electron_phi'              : {'binning' : {'n_or_arr' : 12, 'lo' : -pi, 'hi' : pi}, 'xlim' : (-pi,pi),    'xlabel' : "$\phi_{e}$"},
            'electron_etaSC'            : {'binning' : {'n_or_arr' : [-2.5, -1.5660, -1.4442, -0.8, 0.0, 0.8, 1.4442, 1.5660, 2.5]}, 'ylim' : (-2.5,2.5), 'ylabel' : "Electron Supercluster $\eta$"},
        },
    },
    "plot_options" : {
        "scale" : "log",
        "fontsize" : 14,
    }
}
