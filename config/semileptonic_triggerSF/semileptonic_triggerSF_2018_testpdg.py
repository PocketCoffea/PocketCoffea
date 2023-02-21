import sys, os
import awkward as ak
from pocket_coffea.parameters.cuts.preselection_cuts import semileptonic_triggerSF_presel, passthrough
from pocket_coffea.workflows.semileptonic_triggerSF import semileptonicTriggerProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min, get_HLTsel
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.cut_definition import Cut
#sys.path.append(os.path.dirname(__file__))
from config.semileptonic_triggerSF.parameters import eras
from config.semileptonic_triggerSF.functions import get_ht_above, get_ht_below
from config.semileptonic_triggerSF.plot_options import efficiency, scalefactor, ratio, residue
from config.datamc.plots import cfg_plot
from math import pi

def dilepton(events, params, year, sample, **kwargs):

    has_one_electron = events.nElectronGood == 1
    has_one_muon = events.nMuonGood == 1

    mask = (
        (events.nLeptonGood == 2) &
        has_one_electron &
        has_one_muon &
        #( has_one_muon & (ak.firsts(events.MuonGood.pt) > params["pt_leading_muon"]) ) &
        (events.nJetGood >= params["njet"])
    )

    # Pad None values with False
    return ak.where(ak.is_none(mask), False, mask)

cfg =  {

    "dataset" : {
        "jsons": ["datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/DATA_SingleMuon_local.json"],
        "filter" : {
            "samples": ["TTToSemiLeptonic",
                        "TTTo2L2Nu",
                        "DATA_SingleMuon"],
            "samples_exclude" : [],
            "year": ["2018"]
        }
    },

    # Input and output files
    "workflow" : semileptonicTriggerProcessor,
    "output"   : "output/test/sf_ele_trigger_semilep/semileptonic_triggerSF_2018_testpdg",
    "workflow_options" : {
        "output_triggerSF" : "pocket_coffea/parameters/semileptonic_triggerSF/triggerSF_2018_testpdg",
        "eras" : eras["2018"],
    },

    # Executor parameters
    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 125,
        "queue"          : "standard",
        "walltime"       : "12:00:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 400000,
        "retries"        : 50,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
        "adapt"          : False,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim" : [ get_nObj_min(3, 15., "Jet"),
               #get_nObj_min(1, 20., "Electron"),
               #get_nObj_min(1, 20., "Muon"),
               get_HLTsel("semileptonic", primaryDatasets=["SingleMuon"]) ],
    "preselections" : [Cut( name="semileptonic_triggerSF", params={"njet": 4, "pt_leading_muon": 26},function=dilepton )],
    "categories": {
        "Ele32_EleHT_pass" : [
            get_nObj_min(1, 26, "MuonGood"),
            get_nObj_min(1, 30, "ElectronGood"),
            get_HLTsel("semileptonic", primaryDatasets=["SingleEle"])
        ],
        "Ele32_EleHT_fail" : [
            get_nObj_min(1, 26, "MuonGood"),
            get_nObj_min(1, 30, "ElectronGood"),
            get_HLTsel("semileptonic", primaryDatasets=["SingleEle"], invert=True)
        ],
        "Ele30Mu26" : [
            get_nObj_min(1, 26, "MuonGood"),
            get_nObj_min(1, 30, "ElectronGood"),
        ],
        "Ele30Mu30" : [
            get_nObj_min(1, 30, "MuonGood"),
            get_nObj_min(1, 30, "ElectronGood"),
        ],
        "inclusive" : [passthrough],
    },

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id", "sf_mu_iso"],#, "sf_mu_trigger"],
            "bycategory" : {
            }
        },
        "bysample": {
        }
    },

    "variations": {
        "weights": {
            "common": {
                "inclusive": [ "pileup" ],
                "bycategory" : {
                }
            },
        "bysample": {
        }    
        },
        "shape": {
            "common":{
                "inclusive": [ "JER" ]
            }
        }
    },
    
    "variables" : {
        **muon_hists(coll="MuonGood"),
        **muon_hists(coll="MuonGood", pos=0),
        **lepton_hists(coll="LeptonGood", pos=0),
        **lepton_hists(coll="LeptonGood", pos=1),
        "ElectronGood_pt" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(0,500))
            ]
        ),
        "ElectronGood_etaSC" : HistConf(
            [
                Axis(coll="ElectronGood", field="etaSC", type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5))
            ]
        ),
        "ElectronGood_phi" : HistConf(
            [
                Axis(coll="ElectronGood", field="phi",
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$"),
            ]
        ),
        "ElectronGood_pt_1" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", pos=0, type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(0,500))
            ]
        ),
        "ElectronGood_etaSC_1" : HistConf(
            [
                Axis(coll="ElectronGood", field="etaSC", pos=0, type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5))
            ]
        ),
        "ElectronGood_phi_1" : HistConf(
            [
                Axis(coll="ElectronGood", field="phi", pos=0,
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$"),
            ]
        ),
        **jet_hists(coll="JetGood"),
        **count_hist(name="nMuons", coll="MuonGood",bins=3, start=0, stop=3),
        **count_hist(name="nElectrons", coll="ElectronGood",bins=3, start=0, stop=3),
        **count_hist(name="nLeptons", coll="LeptonGood",bins=3, start=0, stop=3),
        **count_hist(name="nJets", coll="JetGood",bins=6, start=4, stop=10),
        **count_hist(name="nBJets", coll="BJetGood",bins=6, start=4, stop=10),
        "ht" : HistConf(
            [
                Axis(coll="events", field="JetGood_Ht", bins=40, start=0, stop=2000, label="$H_T$", lim=(0,2000))
            ]
        ),
        "electron_etaSC_pt_leading" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", pos=0, type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(30,500)),
                Axis(coll="ElectronGood", field="etaSC", pos=0, type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5)),
            ]
        ),
        "electron_phi_pt_leading" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", pos=0, type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(30,500)),
                Axis(coll="ElectronGood", field="phi", pos=0,
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$",
                     lim=(-pi,pi)),
            ]
        ),
        "electron_etaSC_phi_leading" : HistConf(
            [
                Axis(coll="ElectronGood", field="phi", pos=0,
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$",
                     lim=(-pi,pi)),
                Axis(coll="ElectronGood", field="etaSC", pos=0, type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5)),
            ]
        ),
        "electron_etaSC_pt_all" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(30,500)),
                Axis(coll="ElectronGood", field="etaSC", type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5)),
            ]
        ),
        "electron_phi_pt_all" : HistConf(
            [
                Axis(coll="ElectronGood", field="pt", type="variable",
                     bins=[30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500],
                     label="Electron $p_{T}$ [GeV]",
                     lim=(30,500)),
                Axis(coll="ElectronGood", field="phi",
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$",
                     lim=(-pi,pi)),
            ]
        ),
        "electron_etaSC_phi_all" : HistConf(
            [
                Axis(coll="ElectronGood", field="phi",
                     bins=12, start=-pi, stop=pi,
                     label="Electron $\phi$",
                     lim=(-pi,pi)),
                Axis(coll="ElectronGood", field="etaSC", type="variable",
                     bins=[-2.5, -2.0, -1.5660, -1.4442, -1.2, -1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442, 1.5660, 2.0, 2.5],
                     label="Electron Supercluster $\eta$",
                     lim=(-2.5,2.5)),
            ]
        ),
    },
    "plot_options" : {
        "only" : "electron_etaSC_pt_leading",
        "workers" : 16,
        "scale" : "log",
        "fontsize" : 20,
        "fontsize_map" : 18,
        "dpi" : 150,
        "rebin" : {
            "ElectronGood_pt" : {
                "xticks" : [30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500]
            },
            "ElectronGood_pt_1" : {
                "xticks" : [30, 35, 40, 50, 60, 70, 80, 90, 100, 130, 200, 500]
            },
            "ElectronGood_eta" : {
                "xticks" : [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
            },
            "ElectronGood_eta_1" : {
                "xticks" : [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
            },
            "ElectronGood_etaSC" : {
                "xticks" : [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
            },
            "ElectronGood_etaSC_1" : {
                "xticks" : [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
            },
        },
        "efficiency" : efficiency,
        "scalefactor" : scalefactor,
        "ratio" : ratio,
        "residue" : residue,
        #"rebin" : {}
    }
}

cfg["plot_options"].update(cfg_plot)
