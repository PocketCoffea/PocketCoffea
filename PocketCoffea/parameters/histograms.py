from ..lib.HistManager import Axis, HistConf
import math


common_settings = {
        'muon_pt'                  : {"field":"pt", "bins": 200, "start":0, 'stop' : 1000, "lim" : (0,500), 'label' : "$p_{T}^{\mu}$ [GeV]"},
        'muon_eta'                 : {"field":"eta", "bins": 80, "start":-4, 'stop' : 4 , "lim" : (-4,4),  'label' : "$\eta_{\mu}$"},
        'muon_phi'                 : {"field":"phi", "bins": 128, "start":-math.pi, 'stop' : math.pi, "lim" : (-math.pi,math.pi), 'label' : "$\phi_{\mu}$"},
        'electron_pt'              : {"field":"pt", "bins": 200, "start":0, 'stop' : 1000, "lim" : (0,500), 'label' : "$p_{T}^{e}$ [GeV]"},
        'electron_eta'             : {"field":"eta", "bins": 80, "start":-4, 'stop' : 4 , "lim" : (-4,4),  'label' : "$\eta_{e}$"},
        "electron_etaSC"           : {"field":"etaSC", "bins": [-2.5, -2.3, -2.1, -1.9, -1.7, -1.5660, -1.4442, -1.2, -1.0, -0.8,
                                               -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4442,
                                               1.5660, 1.7, 1.9, 2.1, 2.3, 2.5],
                                      'lim' : (-2.5,2.5), 'label' : "Electron Supercluster $\eta$", "type":"variable"},
        'electron_phi'             : {"field":"phi", "bins": 128, "start":-math.pi, 'stop' : math.pi, "lim" : (-math.pi,math.pi),'label' : "$\phi_{e}$"},
        'jet_pt'                   : {"field":"pt", "bins": 150, "start":0, 'stop' : 1500, "lim" : (0,500), 'label' : "$p_{T}^{j}$ [GeV]"},
        'jet_eta'                  : {"field":"eta", "bins": 80, "start":-4, 'stop' : 4, "lim" : (-4,4),  'label' : "$\eta_{j}$"},
        'jet_phi'                  : {"field":"phi", "bins": 128, "start":-math.pi, 'stop' : math.pi, "lim" : (-math.pi,math.pi),'label' : "$\phi_{j}$"},
        'jet_btagDeepFlavB'        : {"field":"btagDeepFlavB", "bins": 50, "start":0, 'stop' : 1, "lim":(0, 1),'label' : "AK4 DeepJet b-tag score"},

        'parton_pt'                : {"field":"pt", "bins": 150, "start":0, 'stop' : 1500, "lim" : (0,500), 'label' : "$p_{T}^{parton}$ [GeV]"},
        'parton_eta'               : {"field":"eta", "bins": 80, "start":-4, 'stop' : 4, "lim" : (-4,4),  'label' : "$\eta_{parton}$"},
        'parton_phi'               : {"field":"phi", "bins": 128, "start":-math.pi, 'stop' : math.pi, "lim" : (-math.pi,math.pi),'label' : "$\phi_{parton}$"},
        'parton_dRMatchedJet'      : {"field":"dRMatchedJet", "bins": 200, "start":0, 'stop' : 5 , "lim" : (0,1),   'label' : 'min ${\Delta}R_{parton,jet}$'},
        'parton_pdgId'             : {"field":"pdgId", "bins": 50, "start":-25, 'stop' : 25, "lim" : (0,1),   'label' : 'Parton pdgId'},

        'met_pt'                   : {"field":"pt","bins": 200, "start":0, 'stop' : 2000, "lim" : (0,500), 'label' : "$p_{T}^{MET}$ [GeV]"},
        'met_phi'                  : {"field":"phi","bins": 128, "start":-math.pi, 'stop' : math.pi, "lim" : (-math.pi,math.pi),'label' : "$\phi_{MET}$"},
        'mll'                      : {"field":"mll", "bins": 300, "start":0, 'stop' : 1500, "lim" : (0,500), 'label' : "$m_{\ell\ell}$ [GeV]"},   
}

collection_fields = {
    'jet': ["eta","pt","phi", "btagDeepFlavB"],
    'parton':  ["eta","pt","phi", "dRMatchedJet","pdgId"],
    'electron': ["eta","pt","phi", "etaSC"],
    'muon':  ["eta","pt","phi"]
} 


def _get_default_hist(name, type, coll, pos=None, fields=None):
    out = {}
    for field in collection_fields[type]:
        if fields == None or field in fields:
            hist_name = f"{name}_{field}"
            setting = common_settings[f"{type}_{field}"]
            setting["coll"] = coll
            # If the position argument is given the histogram is
            # created for the specific position
            if pos:
                setting["pos"] = pos
                hist_name += f"_{pos}"
                
            out[hist_name] = HistConf(
                axes=[Axis(**setting)]
            )
    return out

    
def default_hists_jet(name, coll="JetGood", pos=None, fields=None):
    return _get_default_hist(name, "jet", coll, pos, fields)

def default_hists_parton(name, coll="PartonMatched",  pos=None,fields=None):
    return _get_default_hist(name, "parton", coll, pos, fields)

def default_hists_ele(name, coll="ElectronGood", pos=None, fields=None):
     return _get_default_hist(name, "electron", coll, pos, fields)

def default_hists_muon(name, coll="MuonGood",  pos=None, fields=None):
    return _get_default_hist(name, "muon", coll, pos, fields)

def default_hists_count(name, coll, bins=10, start=0, stop=9, label=None):
    return {
        f"hist_{name}": HistConf(axes=[
        Axis(coll="events", field=f"n{coll}",
             label=f"$N_{{{coll}}}",
             bins=bins, start=start, stop=stop)
        ])
    }





