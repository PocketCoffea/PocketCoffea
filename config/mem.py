from lib.cuts import dilepton

cfg =  {
    # Input and output files
    "workflow" : "mem",
    "input"    : "datasets/baseline_samples_local.json",
    "output"   : "histograms/mem_test.coffea",
    "plots"    : "plots/mem_test",

    # Executor parameters
    "executor"     : "futures",
    "workers"      : 12,
    "scaleout"     : 6,
    "chunk"        : 50000,
    "max"          : None,
    "limit"        : 1,
    "skipbadfiles" : None,

    # Cuts and variables to plot
    "cuts" : [dilepton],
    "variables" : {
        "muon_pt" : None,
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
        "nfatjet" : None,
        "nbquark" : None,
        "bquark_pt" : None,
        "bquark_eta" : None,
        "bquark_phi" : None,
        "bquark_drMatchedJet" : None,
    },
    "variables2d" : {},
    "scale" : "log",
    "normed" : False,
    "scale_ttHbb" : 1000
}
