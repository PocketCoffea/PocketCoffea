import numpy as np
import awkward as ak

def photon_selection(events, photon_collection, params, leptons_collection=""):

    photons = events["Photon"]
    cuts = params.object_preselection[photon_collection]
    # Requirements on pT and eta
    passes_eta = abs(photons.eta) < cuts["eta"]
    passes_transition = np.invert(( abs(photons.eta) >= 1.4442) & (abs(photons.eta) <= 1.5660))
    passes_pt = photons.pt > cuts["pt"]
    passes_pixelseed = ~photons.pixelSeed
    passes_medium_id = photons.cutBased >= cuts["cutBased"]

    if leptons_collection != "":
        dR_photons_lep = photons.metric_table(events[leptons_collection])
        mask_lepton_cleaning = ak.prod(dR_photons_lep > cuts["dr_lepton"], axis=2) == 1
    else:
        mask_lepton_cleaning = True

    good_photons = passes_eta & passes_transition & passes_pt & passes_pixelseed & passes_medium_id & mask_lepton_cleaning 
    

    return photons[good_photons]
    