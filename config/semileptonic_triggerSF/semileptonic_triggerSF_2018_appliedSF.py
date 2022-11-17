from pocket_coffea.parameters.cuts.preselection_cuts import semileptonic_triggerSF_presel, passthrough
from pocket_coffea.lib.cut_functions import get_nObj
from pocket_coffea.workflows.semileptonic_triggerSF import semileptonicTriggerProcessor
from config.semileptonic_triggerSF.functions import get_trigger_passfail, get_ht_above, get_ht_below
from config.semileptonic_triggerSF.plot_options import efficiency, scalefactor, ratio, residue
from math import pi
import numpy as np

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
    "output"   : "output/sf_ele_trigger_semilep/semileptonic_triggerSF_2018_appliedSF",
    "output_triggerSF" : "pocket_coffea/parameters/semileptonic_triggerSF/triggerSF_2018_Ele32_EleHT_appliedSF",
    "triggerSF" : None,

    # Executor parameters
    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 125,
        "partition"      : "standard",
        "walltime"       : "12:00:00",
        "mem_per_worker" : "5GB", # GB
        "exclusive"      : False,
        "chunk"          : 50000,
        "retries"        : 30,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic_triggerSF",
    "skim" : [ get_nObj(3, 15., "Jet") ],
    "preselections" : [semileptonic_triggerSF_presel],
    "categories": {
        "Ele32_EleHT_pass_triggerSF" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "pass")],
        "Ele32_EleHT_fail_triggerSF" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "fail")],
        "Ele32_EleHT_pass_lowHT" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "pass"), get_ht_below(400)],
        "Ele32_EleHT_fail_lowHT" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "fail"), get_ht_below(400)],
        "Ele32_EleHT_pass_highHT" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "pass"), get_ht_above(400)],
        "Ele32_EleHT_fail_highHT" : [get_trigger_passfail(["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"], "fail"), get_ht_above(400)],
        "inclusive" : [passthrough],
        "triggerSF" : [passthrough],
    },
    "split_eras" : True,
    "split_ht" : False,

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS", "pileup", "sf_ele_reco_id", "sf_mu_id_iso", "sf_ele_trigger"],
        },
    },
    
    "variables" : {
        "muon_pt" : {'binning' : {'n_or_arr' : 200, 'lo' : 0, 'hi' : 2000}, 'xlim' : (0,500), 'xlabel' : "$p_{T}^{\mu}$ [GeV]"},
        "muon_eta" : None,
        "muon_phi" : None,
        "electron_pt" : {'binning' : {'n_or_arr' : 400, 'lo' : 0, 'hi' : 2000}, 'xlim' : (30,500), 'xlabel' : "$p_{T}^{e}$ [GeV]"},
        "electron_eta" : {'binning' : {'n_or_arr' : [-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5]}, 'xlim' : (-2.5,2.5), 'xlabel' : "Electron $\eta$"},
        "electron_etaSC" : {'binning' : {'n_or_arr' : [-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5]}, 'xlim' : (-2.5,2.5), 'xlabel' : "Electron Supercluster $\eta$"},
        "electron_phi" : {'binning' : {'n_or_arr' : 24, 'lo' : -pi, 'hi' : pi}, 'xlim' : (-pi,pi), 'xlabel' : "$\phi_{e}$"},
        "jet_pt" : None,
        "jet_eta" : None,
        "jet_phi" : None,
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "njet" : None,
        "nbjet" : None,
        "ht" : None,
    },
    "variables2d" : {
        'electron_etaSC_vs_electron_pt' : {
            'pt'               : {'binning' : {'n_or_arr' : [0, 30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500, 2000]},  'xlim' : (30,500),    'xlabel' : "Electron $p_{T}$ [GeV]",
                                           'xticks' : [30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500]},
            'etaSC'            : {'binning' : {'n_or_arr' : [-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5]}, 'ylim' : (-2.5,2.5), 'ylabel' : "Electron Supercluster $\eta$"},
        },
        'electron_phi_vs_electron_pt' : {
            'pt'               : {'binning' : {'n_or_arr' : [0, 30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500, 2000]},  'xlim' : (30,500),    'xlabel' : "Electron $p_{T}$ [GeV]",
                                           'xticks' : [30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500]},
            'phi'              : {'binning' : {'n_or_arr' : 12, 'lo' : -pi, 'hi' : pi}, 'ylim' : (-pi,pi), 'ylabel' : "$\phi_{e}$"},
        },
        'electron_etaSC_vs_electron_phi' : {
            'phi'              : {'binning' : {'n_or_arr' : 12, 'lo' : -pi, 'hi' : pi}, 'xlim' : (-pi,pi),    'xlabel' : "$\phi_{e}$"},
            'etaSC'            : {'binning' : {'n_or_arr' : [-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5]}, 'ylim' : (-2.5,2.5), 'ylabel' : "Electron Supercluster $\eta$"},
        },
    },
    "plot_options" : {
        "only" : "hist_njet",
        #"only" : None,
        "workers" : 16,
        "scale" : "log",
        "fontsize" : 18,
        "fontsize_map" : 10,
        "dpi" : 150,
        "rebin" : {
            'electron_pt'    : {'binning' : {'n_or_arr' : [30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500]}, 'xticks' : [30, 35, 40, 50, 60, 70, 80, 90, 100, 200, 500]},
            'electron_etaSC' : {'binning' : {'n_or_arr' : [-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5]}, 'xticks' : [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]},
            'njet'           : {'binning' : {'n_or_arr' : 7, 'lo' : 4, 'hi' : 11}, 'xlim' : (4,11), 'xlabel' : "$N_{jet}$"},
            'ht'             : {'binning' : {'n_or_arr' : 80, 'lo' : 0, 'hi' : 4000}, 'xlim' : (100,1000), 'xlabel' : "$H_T$"},
        },
        "efficiency" : efficiency,
        "scalefactor" : scalefactor,
        "ratio" : ratio,
        "residue" : residue,
        #"rebin" : {}
    }
}
