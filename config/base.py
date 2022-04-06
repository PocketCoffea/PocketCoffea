from lib.cuts import dilepton

cfg =  {
    # Dataset parameters
    "dataset"  : "datasets/DAS/RunIISummer20UL18.txt",
    "json"     : "datasets/RunIISummer20UL18.json",
    "storage_prefix" : "/pnfs/psi.ch/cms/trivcat/store/user/mmarcheg/ttHbb",

    # Input and output files
    "workflow" : "base",
    "input"    : "datasets/RunIISummer20UL18_local.json",
    "output"   : "output/base",

    # Executor parameters
    "run_options" : {
        "executor"     : "futures",
        "workers"      : 12,
        "scaleout"     : 10,
        "chunk"        : 50000,
        "max"          : None,
        "skipbadfiles" : None,
        "voms"         : None,
        "limit"        : 2,
    },

    # Cuts and plots settings
    "finalstate" : "dilepton",
    "cuts_definition" : {
        "baseline" : {
            "f"   : dilepton,
            "tag" : "dilepton"
        },
    },
    "categories": {
        "baseline" : {"baseline"},
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
        "nmuon" : None,
        "nelectron" : None,
        "nlep" : None,
        "njet" : None,
        "nbjet" : None,
    },
    "variables2d" : {},
    "scale" : "log"
}
