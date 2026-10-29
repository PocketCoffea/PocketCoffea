# Same as config.py but with the analysis-like jet preselection (pt > 30 GeV, |eta| < 2.4):
# the effect of the JER fix on the central jets only. See README.md.
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_functions import get_nObj_min, get_HLTsel, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence
from pocket_coffea.lib.weights.common import common_weights
from pocket_coffea.lib.columns_manager import ColOut
from pocket_coffea.parameters import defaults

from workflow import JERFixProcessor

import os
localdir = os.path.dirname(os.path.abspath(__file__))

default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir + "/params")

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection_central.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  f"{localdir}/params/jets_calibration.yaml",
                                                  update=True)

# Per-jet columns exported for the nominal and every variation: enough to match the jets
# between the two runs (event, run, luminosityBlock + pocket_sortidx = position in NanoAOD)
# and to classify them (pt_raw, pt_gen = -1 for no gen match, eta).
jet_columns = ["pt", "eta", "phi", "mass", "pt_raw", "pt_gen", "pocket_sortidx"]

# pt axis with finer bins at low pt, where the fix acts
pt_bins = [30, 35, 40, 50, 60, 80, 100, 150, 200, 300, 500]
eta_bins = [-2.4, -1.3, 0.0, 1.3, 2.4]

cfg = Configurator(
    parameters=parameters,
    datasets={
        "jsons": [f"{localdir}/datasets/datasets_2024.json"],
        "filter": {
            "samples": ["DATA_SingleEle", "TTTo2L2Nu"],
            "samples_exclude": [],
            "year": ["2024"],
        },
    },
    workflow=JERFixProcessor,

    skim=[get_nPVgood(1), eventFlags, goldenJson,
          get_HLTsel(primaryDatasets=["SingleEle"])],
    preselections=[passthrough],
    categories={
        "baseline": [passthrough],
        "2jets": [get_nObj_min(2, coll="JetGood")],
    },

    # No lepton/btag SFs: they are not needed for the jet comparison
    weights={
        "common": {"inclusive": ["genWeight", "lumi", "XS", "pileup"], "bycategory": {}},
        "bysample": {},
    },
    weights_classes=common_weights,
    calibrators=default_calibrators_sequence,

    variations={
        "weights": {"common": {"inclusive": [], "bycategory": {}}, "bysample": {}},
        "shape": {"common": {"inclusive": ["jet_calibration"]}},
    },

    variables={
        **jet_hists(coll="JetGood"),
        **jet_hists(coll="JetGood", pos=0, name="Jet1"),
        **jet_hists(coll="JetGood", pos=1, name="Jet2"),
        **count_hist("JetGood", bins=12, start=0, stop=12),
        "JetGood_pt_eta": HistConf([
            Axis(coll="JetGood", field="pt", label="jet $p_T$ [GeV]", bins=pt_bins),
            Axis(coll="JetGood", field="eta", label="jet $\\eta$", bins=eta_bins),
        ]),
        "JetGood_pt_over_raw_eta": HistConf([
            Axis(coll="JetGood", field="pt_over_raw", label="jet $p_T$ / $p_T^{raw}$",
                 bins=60, start=0.7, stop=1.6),
            Axis(coll="JetGood", field="eta", label="jet $\\eta$", bins=eta_bins),
        ]),
        "MET_pt": HistConf([Axis(coll="PuppiMET", field="pt", label="PuppiMET $p_T$ [GeV]",
                                 bins=50, start=0, stop=250)]),
        "MET_phi": HistConf([Axis(coll="PuppiMET", field="phi", label="PuppiMET $\\phi$",
                                  bins=32, start=-3.2, stop=3.2)]),
    },

    columns={
        "common": {
            "inclusive": [
                ColOut(collection="events", columns=["event", "run", "luminosityBlock"]),
                ColOut(collection="Jet", columns=jet_columns),
                ColOut(collection="JetGood", columns=jet_columns),
                ColOut(collection="PuppiMET", columns=["pt", "phi"]),
            ]
        }
    },
)
