from copy import deepcopy
from ..lib.hist_manager import Axis, HistConf
import math

default_axis_settings = {
    'muon_pt': {
        "field": "pt",
        "bins": 50,
        "start": 0,
        'stop': 500,
        "lim": (0, 500),
        'label': "$p_{T}^{\mu}$ [GeV]",
    },
    'muon_eta': {
        "field": "eta",
        "bins": 50,
        "start": -2.5,
        'stop': 2.5,
        "lim": (-2.5, 2.5),
        'label': "$\eta_{\mu}$",
    },
    'muon_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "$\phi_{\mu}$",
    },
    'electron_pt': {
        "field": "pt",
        "bins": 50,
        "start": 0,
        'stop': 500,
        "lim": (0, 500),
        'label': "$p_{T}^{e}$ [GeV]",
    },
    'electron_eta': {
        "field": "eta",
        "bins": 50,
        "start": -2.5,
        'stop': 2.5,
        "lim": (-2.5, 2.5),
        'label': "$\eta_{e}$",
    },
    "electron_etaSC": {
        "field": "etaSC",
        "type": "variable",
        "bins": [
            -2.5,
            -2.3,
            -2.1,
            -1.9,
            -1.7,
            -1.5660,
            -1.4442,
            -1.2,
            -1.0,
            -0.8,
            -0.6,
            -0.4,
            -0.2,
            0.0,
            0.2,
            0.4,
            0.6,
            0.8,
            1.0,
            1.2,
            1.4442,
            1.5660,
            1.7,
            1.9,
            2.1,
            2.3,
            2.5,
        ],
        'lim': (-2.5, 2.5),
        'label': "Electron Supercluster $\eta$",
    },
    'electron_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "$\phi_{e}$",
    },
    'lepton_pt': {
        "field": "pt",
        "bins": 50,
        "start": 0,
        'stop': 500,
        "lim": (0, 500),
        'label': "$p_{T}^{\ell}$ [GeV]",
    },
    'lepton_eta': {
        "field": "eta",
        "bins": 50,
        "start": -2.5,
        'stop': 2.5,
        "lim": (-2.5, 2.5),
        'label': "$\eta_{\ell}$",
    },
    'lepton_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "$\phi_{\ell}$",
    },
    'lepton_pdgId': {
        "field": "pdgId",
        "bins": 32,
        "start": -16,
        'stop': 16,
        "lim": (-16, 16),
        'label': "Lepton pdgId",
    },
    'jet_pt': {
        "field": "pt",
        "bins": 100,
        "start": 0,
        'stop': 1000,
        "lim": (0, 500),
        'label': "$p_{T}^{j}$ [GeV]",
    },
    'jet_eta': {
        "field": "eta",
        "bins": 50,
        "start": -2.5,
        'stop': 2.5,
        "lim": (-2.5, 2.5),
        'label': "$\eta_{j}$",
    },
    'jet_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "$\phi_{j}$",
    },
    'jet_btagDeepFlavB': {
        "field": "btagDeepFlavB",
        "bins": 50,
        "start": 0.0,
        'stop': 1.0,
        "lim": (0, 1),
        'label': "AK4 DeepJet b-tag score",
    },
    'fatjet_pt': {
        "field": "pt",
        "bins": 100,
        "start": 0,
        'stop': 1000,
        "lim": (0, 1000),
        'label': r"FatJet $p_{T}$ [GeV]",
    },
    'fatjet_eta': {
        "field": "eta",
        "bins": 50,
        "start": -2.5,
        'stop': 2.5,
        "lim": (-2.5, 2.5),
        'label': r"FatJet $\eta$",
    },
    'fatjet_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': r"FatJet $\phi$",
    },
    'fatjet_mass': {
        "field": "mass",
        "bins": 100,
        "start": 0,
        'stop': 1000,
        "lim": (0, 1000),
        'label': "FatJet mass [GeV]",
    },
    'fatjet_msoftdrop': {
        "field": "msoftdrop",
        "bins": 100,
        "start": 0,
        'stop': 1000,
        "lim": (0, 1000),
        'label': r"FatJet $m_{SD}$ [GeV]",
    },
    'fatjet_rho': {
        "field": "rho",
        "bins": 100,
        "start": -8,
        'stop': 0,
        "lim": (-7, 0),
        'label': r"FatJet $\rho$",
    },
    'fatjet_tau21': {
        "field": "tau21",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': r"$\tau_{21}$",
    },
    'fatjet_btagDDBvLV2': {
        "field": "btagDDBvLV2",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "btagDDBvLV2",
    },
    'fatjet_btagDDCvLV2': {
        "field": "btagDDCvLV2",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "btagDDCvLV2",
    },
    'fatjet_btagDDCvBV2': {
        "field": "btagDDCvBV2",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "btagDDCvBV2",
    },
    'fatjet_particleNetMD_Xbb': {
        "field": "particleNetMD_Xbb",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "particleNetMD_Xbb",
    },
    'fatjet_particleNetMD_Xcc': {
        "field": "particleNetMD_Xcc",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "particleNetMD_Xcc",
    },
    'fatjet_particleNetMD_Xbb_QCD': {
        "field": "particleNetMD_Xbb_QCD",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "particleNetMD Xbb/(Xbb + QCD)",
    },
    'fatjet_particleNetMD_Xcc_QCD': {
        "field": "particleNetMD_Xcc_QCD",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "particleNetMD Xcc/(Xcc + QCD)",
    },
    'fatjet_deepTagMD_ZHbbvsQCD': {
        "field": "deepTagMD_ZHbbvsQCD",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "deepTagMD_ZHbbvsQCD",
    },
    'fatjet_deepTagMD_ZHccvsQCD': {
        "field": "deepTagMD_ZHccvsQCD",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "deepTagMD_ZHccvsQCD",
    },
    'fatjet_btagHbb': {
        "field": "btagHbb",
        "bins": 80,
        "start": 0,
        'stop': 1,
        "lim": (0, 1),
        'label': "btagHbb",
    },
    'sv_summass': {
        "field": "summass",
        "bins": 200,
        "start": 0,
        'stop': 1000,
        "lim": (0, 500),
        'label': r"$\sum({m_{SV}})$ [GeV]",
    },
    'sv_logsummass': {
        "field": "logsummass",
        "bins": 120,
        "start": -6,
        'stop': 6,
        "lim": (-2.5, 6),
        'label': r"log($\sum({m_{SV}})$)",
    },
    'sv_projmass': {
        "field": "projmass",
        "bins": 200,
        "start": 0,
        'stop': 1000,
        "lim": (0, 500),
        'label': r"$m_{SV}^{proj}$ [GeV]",
    },
    'sv_logprojmass': {
        "field": "logprojmass",
        "bins": 120,
        "start": -6,
        'stop': 6,
        "lim": (-2.5, 6),
        'label': r"log($m_{SV}^{proj}$)",
    },
    'sv_sv1mass': {
        "field": "sv1mass",
        "bins": 200,
        "start": 0,
        'stop': 1000,
        "lim": (0, 500),
        'label': r"$m_{SV,1}$ [GeV]",
    },
    'sv_logsv1mass': {
        "field": "logsv1mass",
        "bins": 120,
        "start": -6,
        'stop': 6,
        "lim": (-2.5, 6),
        'label': r"log($m_{SV,1}$)",
    },
    'sv_sumcorrmass': {
        "field": "sumcorrmass",
        "bins": 200,
        "start": 0,
        'stop': 1000,
        "lim": (0, 500),
        'label': r"$\sum({m^{corr}_{SV}})$ [GeV]",
    },
    'sv_logsumcorrmass': {
        "field": "logsumcorrmass",
        "bins": 120,
        "start": -6,
        'stop': 6,
        "lim": (-2.5, 6),
        'label': r"log($\sum({m^{corr}_{SV}})$)",
    },
    'parton_pt': {
        "field": "pt",
        "bins": 150,
        "start": 0,
        'stop': 1500,
        "lim": (0, 500),
        'label': "$p_{T}^{parton}$ [GeV]",
    },
    'parton_eta': {
        "field": "eta",
        "bins": 80,
        "start": -4,
        'stop': 4,
        "lim": (-4, 4),
        'label': "$\eta_{parton}$",
    },
    'parton_phi': {
        "field": "phi",
        "bins": 128,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "$\phi_{parton}$",
    },
    'parton_dRMatchedJet': {
        "field": "dRMatchedJet",
        "bins": 200,
        "start": 0,
        'stop': 5,
        "lim": (0, 1),
        'label': 'min ${\Delta}R_{parton,jet}$',
    },
    'parton_pdgId': {
        "field": "pdgId",
        "bins": 50,
        "start": -25,
        'stop': 25,
        "lim": (0, 1),
        'label': 'Parton pdgId',
    },
    'met_pt': {
        "field": "pt",
        "bins": 28,
        "start": 20,
        'stop': 300,
        "lim": (20, 300),
        'label': "MET [GeV]",
    },
    'met_phi': {
        "field": "phi",
        "bins": 64,
        "start": -math.pi,
        'stop': math.pi,
        "lim": (-math.pi, math.pi),
        'label': "MET $\phi$",
    },
    'mll': {
        "field": "mll",
        "bins": 300,
        "start": 0,
        'stop': 1500,
        "lim": (0, 500),
        'label': "$m_{\ell\ell}$ [GeV]",
    },
}

collection_fields = {
    'jet': ["eta", "pt", "phi", "btagDeepFlavB"],
    'fatjet': [
        "eta",
        "pt",
        "phi",
        "mass",
        "msoftdrop",
        "tau21",
        "btagDDBvLV2",
        "btagDDCvLV2",
        "btagDDCvBV2",
        # "particleNetMD_Xbb", "particleNetMD_Xcc",
        "particleNetMD_Xbb_QCD",
        "particleNetMD_Xcc_QCD",
        "deepTagMD_ZHbbvsQCD",
        "deepTagMD_ZHccvsQCD",
        "btagHbb",
    ],
    'parton': ["eta", "pt", "phi", "dRMatchedJet", "pdgId"],
    'electron': ["eta", "pt", "phi", "etaSC"],
    'muon': ["eta", "pt", "phi"],
    'lepton': ["eta", "pt", "phi", "pdgId"],
    'met': ["pt", "phi"],
    'sv': [
        "summass",
        "logsummass",
        "sv1mass",
        "logsv1mass",
        "sumcorrmass",
        "logsumcorrmass",
    ]
    #'sv': ["summass", "logsummass", "projmass", "logprojmass", "sv1mass", "logsv1mass", "sumcorrmass", "logsumcorrmass"]
}


def _get_default_hist(name, type, coll, pos=None, fields=None, **kwargs):
    out = {}
    for field in collection_fields[type]:
        if fields == None or field in fields:
            hist_name = f"{name}_{field}"
            setting = deepcopy(default_axis_settings[f"{type}_{field}"])
            setting["coll"] = coll
            # If the position argument is given the histogram is
            # created for the specific position
            if pos != None:
                setting["pos"] = pos
                # Avoid 0-indexing for the name of the histogram
                hist_name += f"_{pos+1}"

            out[hist_name] = HistConf(
                axes=[
                    Axis(**setting),
                ],
                **kwargs,
            )
    return out


def jet_hists(coll="JetGood", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "jet", coll, pos, fields)


def fatjet_hists(coll="FatJetGood", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "fatjet", coll, pos, fields)


def parton_hists(coll="PartonMatched", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "parton", coll, pos, fields)


def ele_hists(coll="ElectronGood", pos=None, fields=None, name=None, **kwargs):
    if name == None:
        name = coll
    return _get_default_hist(name, "electron", coll, pos, fields, **kwargs)


def muon_hists(coll="MuonGood", pos=None, fields=None, name=None, **kwargs):
    if name == None:
        name = coll
    return _get_default_hist(name, "muon", coll, pos, fields, **kwargs)


def lepton_hists(coll="LeptonGood", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "lepton", coll, pos, fields)


def met_hists(coll="MET", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "met", coll, pos, fields)


def sv_hists(coll="SV", pos=None, fields=None, name=None):
    if name == None:
        name = coll
    return _get_default_hist(name, "sv", coll, pos, fields)


def count_hist(coll, bins=10, start=0, stop=9, label=None, name=None):
    if name == None:
        name = f"n{coll}"
    return {
        f"{name}": HistConf(
            axes=[
                Axis(
                    coll="events",
                    field=f"n{coll}",
                    label=f"$N_{{{coll}}}$",
                    bins=bins,
                    start=start,
                    stop=stop,
                    lim=(start, stop),
                )
            ]
        )
    }
