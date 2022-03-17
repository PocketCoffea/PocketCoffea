from lib.cuts import dilepton

cfg =  {
    # Input and output files
    "workflow" : "mem"
    "input"    : "/work/mmarcheg/PocketCoffea/datasets/baseline_samples_local.json",
    "output"   : "/work/mmarcheg/PocketCoffea/histograms/mem_test.coffea",

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
    "variables" : [
        "muon_pt",
        "muon_eta",
        "muon_phi",
        "electron_pt",
        "electron_eta",
        "electron_phi",
        "jet_pt",
        "jet_eta",
        "jet_phi",
        "nmuon",
        "nelectron",
        "nlep",
        "nmuongood",
        "nelectrongood",
        "nlepgood",
        "njet",
        "nbjet",
        "nfatjet",
        "nbquark",
        "bquark_pt",
        "bquark_eta",
        "bquark_phi",
        "bquark_drMatchedJet",
    ],
    "variables2d" : [
        
    ],
    "scale_ttHbb" : 1000
}
