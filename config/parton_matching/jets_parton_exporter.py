from pocket_coffea.parameters.cuts.preselection_cuts import (
    dilepton_presel,
    semileptonic_presel_nobtag,
    passthrough,
)
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nBtagMin, get_HLTsel
from pocket_coffea.parameters.cuts.preselection_cuts import semileptonic_presel
from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.columns_manager import ColOut
from pocket_coffea.workflows.parton_matching import PartonMatchingProcessor
from pocket_coffea.parameters.histograms import *

import sys, os
sys.path.append(os.path.dirname(__file__))
from functions import *


cfg = {
    "dataset": {
        "jsons": ["datasets/signal_ttHTobb_local.json"],
        "filter": {
            "samples": ["ttHTobb"],
            "samples_exclude": [],
            "year": ["2018"]
        },
    },
    # Input and output files
    "workflow": PartonMatchingProcessor,
    "output": "output/jets_partons_leptons_training_dataset",
    
    "workflow_extra_options": {"parton_jet_min_dR": 0.3},
    
    "run_options": {
        "executor": "dask/slurm",
        "workers": 1,
        "scaleout": 50,
        "queue": "short",
        "walltime": "01:00:00",
        "mem_per_worker": "6GB",  # GB
        "exclusive": False,
        "chunk": 500000,
        "retries": 30,
        "max": None,
        "skipbadfiles": None,
        "voms": None,
        "limit": None,
        "env": "conda",
    },
    # Cuts and plots settings
    "finalstate": "semileptonic",
    "skim": [
        get_HLTsel("semileptonic"),
        get_nObj_min(4, 15.0, "Jet"),
        get_nBtagMin(3, 15.0, "Jet"),
    ],
    "preselections": [semileptonic_presel],
    "categories": {
        "semilep_LHE": [semilep_lhe]},
    
    "weights": {
        "common": {
            "inclusive": [
                "genWeight",
                "lumi",
                "XS",
                "pileup",
                "sf_ele_reco",
                "sf_ele_id",
                "sf_mu_id",
                "sf_mu_iso",
                "sf_btag",
                "sf_jet_puId",
            ],
            "bycategory": {},
        },
        "bysample": {},
    },
    "variations": {
        "weights": {"common": {"inclusive": [], "bycategory": {}}, "bysample": {}},
    },
    
    "variables": {
    },
    "columns": {
        "common": {
            "inclusive": [],
        },
        "bysample": {
            "ttHTobb": {
                "bycategory": {
                    "semilep_LHE": [
                        ColOut("Parton", ["pt", "eta", "phi", "mass", "pdgId", "provenance"]),
                        ColOut(
                            "PartonMatched",
                            ["pt", "eta", "phi","mass", "pdgId", "provenance", "dRMatchedJet"],
                        ),
                        ColOut(
                            "JetGood",
                            ["pt", "eta", "phi", "hadronFlavour", "btagDeepFlavB"],
                        ),
                        ColOut(
                            "JetGoodMatched",
                            [
                                "pt",
                                "eta",
                                "phi",
                                "hadronFlavour",
                                "btagDeepFlavB",
                                "dRMatchedJet",
                            ],
                        ),
                        ColOut("HiggsParton",
                               ["pt","eta","phi","mass","pdgId"], pos_end=1, store_size=False),
                        ColOut("LeptonGood",
                               ["pt","eta","phi"],
                               pos_end=1, store_size=False),
                        ColOut("MET", ["phi","pt","significance"]),
                        ColOut("Generator",["x1","x2","id1","id2","xpdf1","xpdf2"]),
                        ColOut("LeptonParton",["pt","eta","phi","mass","pdgId"])
                    ]
                }
            }
        },
    },
}
