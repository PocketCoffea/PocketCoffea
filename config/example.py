from lib.cuts import dilepton

cfg =  {
    # Dataset parameters
    "dataset"  : "datasets/DAS/RunIISummer20UL18.txt",
    "json"     : "datasets/RunIISummer20UL18.json",
    "prefix"   : "/pnfs/psi.ch/cms/trivcat/store/user/mmarcheg/ttHbb",

    # Input and output files
    "workflow" : "base",
    "input"    : "datasets/RunIISummer20UL18.json",
    "output"   : "histograms/RunIISummer20UL18_limit2.coffea",
    "plots"    : "plots/RunIISummer20UL18_limit2",

    # Executor parameters
    "running_options" : {
        "executor"     : "futures",
        "workers"      : 12,
        "scaleout"     : 10,
        "chunk"        : 50000,
        "max"          : None,
        "limit"        : 2,
        "skipbadfiles" : None,
        "voms"         : None
        },

    # Cuts and plots settings
    "finalstate" : "dilepton",

    "cuts_definition" : {

       "baseline" : {
            "f" : dilepton,
            "tag": "dilepton_1"
       },
       "SR" : {
            "f" : SR,
            "tag": "SR_ddddddd"
       },
       "CR" : {
            "f" : CR_top_stocazzo,
            "tag": "CR_ddddddd"
       },

       "CRbb" : {
            "f" : CR_top_stocazzo,
            "tag": "CR_ddddddd"
       },

       "ptdistocazzo" : {
            "f" : SPLITTAILPTDELw,
            "TAG": "final_v3_approval"
       },

       "ptdistocazzo2" : {
            "f" : SPLITTAILPTDELw,
            "TAG": "final_v3_approval_opposite"
       }
    }

    "categories": {
        "SR" :  ("trigger","baseline",  "SR"),
        "CR1" : ("trigger", "CR"), 
        "CR2" : ("trigger", "CRbb"),
        "SR-A" : ("trigger","baseline",  "SR", "ptdistocazzo"),
        "SR-B" : ("trigger","baseline",  "SR", "ptdistocazzo2"),
    }, 






    "cuts" : [dilepton],
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
        "nmuongood" : None,
        "nelectrongood" : None,
        "nlepgood" : None,
        "njet" : None,
        "nbjet" : None,
        "nfatjet" : None
    },
    "variables2d" : {},
    "scale" : "log"
}
