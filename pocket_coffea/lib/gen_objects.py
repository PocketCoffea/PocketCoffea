import awkward as ak
import numpy as np

def getGenLeptons(events, lepton_flavour, params):

    cuts = params.object_preselection.GEN_Lep
    particles = events.GenPart

    leptons = particles[
        ((np.abs(particles.pdgId) == 11) | (np.abs(particles.pdgId) == 13))
        & ak.fill_none((np.abs(particles.parent.pdgId) != 15), True)
        & (particles.status == 1)
        & (np.abs(particles.eta) < cuts["eta"])
	    & (particles.pt > cuts["pt"])
    ]
    leptons = leptons[ak.argsort(leptons.pt, axis=1, ascending=False)]

    if lepton_flavour == "Muon":
        return leptons[np.abs(particles.pdgId) == 13]
    elif lepton_flavour == "Electron":
        return leptons[np.abs(particles.pdgId) == 11]
    elif lepton_flavour == "Both":
        return leptons
    else:
        raise("This lepton flavour in GEN particles is not supported:", lepton_flavour)



def getGenJets(events, GenLepCollNameForCleaning, params):

    cuts = params.object_preselection.GEN_Jet
    jets = events.GenJet[(np.abs(events.GenJet.eta) < cuts["eta"]) & (events.GenJet.pt > cuts["pt"])]

    dR_jets_lep = jets.metric_table(events[GenLepCollNameForCleaning])
    mask_lepton_cleaning = ak.prod(dR_jets_lep > 0.4, axis=2) == 1

    mask_good_jets = mask_lepton_cleaning

    return jets[mask_good_jets]
