from lib.cuts import dilepton

cfg =  {
    # Dataset parameters
    "dataset"  : "datasets/DAS/RunIISummer20UL18_ttHbb.txt",
    "json"     : "datasets/RunIISummer20UL18_ttHbb.json",
    "prefix"   : "/pnfs/psi.ch/cms/trivcat/store/user/mmarcheg/ttHbb",

    # Input and output files
    "workflow" : "base",
    "input"    : "datasets/RunIISummer20UL18_ttHbb.json",
    "output"   : "histograms/RunIISummer20UL18_ttHbb.coffea",
    "plots"    : "plots/RunIISummer20UL18_ttHbb",

    # Executor parameters
    #"executor"     : "parsl/slurm",
    "executor"     : "futures",
    "workers"      : 12,
    "scaleout"     : 10,
    "chunk"        : 50000,
    "max"          : None,
    "limit"        : None,
    "skipbadfiles" : None,

    # Cuts and plots settings
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
        "nfatjet" : None
    },
    "variables2d" : {},
    "scale" : "log"
}
