import argparse

import awkward as ak
import numpy as np
import math
import uproot
from vector import MomentumObject4D

from coffea import hist
from coffea.nanoevents.methods import nanoaod

from parameters.preselection import object_preselection

ak.behavior.update(nanoaod.behavior)

def lepton_selection(events, Lepton, finalstate):

    leptons = events[Lepton]
    cuts = object_preselection[finalstate][Lepton]
    # Requirements on pT and eta
    passes_eta = (np.abs(leptons.eta) < cuts["eta"])
    passes_pt = (leptons.pt > cuts["pt"])

    if Lepton == "Electron":
        # Requirements on SuperCluster eta, isolation and id
        etaSC = np.abs(leptons.deltaEtaSC + leptons.eta)
        passes_SC = np.invert((etaSC >= 1.4442) & (etaSC <= 1.5660))
        passes_iso = leptons.pfRelIso03_all < cuts["iso"]
        passes_id = (leptons[cuts['id']] == True)

        good_leptons = passes_eta & passes_pt & passes_SC & passes_iso & passes_id

    elif Lepton == "Muon":
        # Requirements on isolation and id
        passes_iso = leptons.pfRelIso04_all < cuts["iso"]
        passes_id = (leptons[cuts['id']] == True)

        good_leptons = passes_eta & passes_pt & passes_iso & passes_id

    return leptons[good_leptons]

# N.B.: This function works only with awkward v1.5.1 & coffea v0.7.9, it doesn't work with awkward 1.7.0 & coffea v0.7.11
def jet_selection(events, Jet, finalstate, btag=None):

    jets = events[Jet]
    cuts = object_preselection[finalstate][Jet]

    good_jets = ak.ones_like(jets.pt, dtype=bool)

    for Lepton in ['ElectronGood', 'MuonGood']:
        # Only jets that are more distant than dr to ALL leptons are tagged as good jets
        leptons = events[Lepton]
        nlep_max = ak.max(ak.num(leptons, axis=1))

        # A fake lepton object filled with nan values is used for padding in the delta_r calculation
        pfake = {'pt' : [-9999.9], 'eta' : [-9999.9], 'phi' : [-9999.9], 'mass' : [-9999.9]}
        fakeLepton = ak.zip(pfake, with_name="PtEtaPhiMCandidate")
        leptonsPadded = ak.fill_none(ak.pad_none(leptons, nlep_max), fakeLepton)
        good_jets = good_jets & (jets.pt > cuts["pt"]) & (np.abs(jets.eta) < cuts["eta"]) & (jets.jetId >= cuts["jetId"])

        for i in range(nlep_max):
            jets_pass_dr = (jets.delta_r(leptonsPadded[:,i]) > cuts["dr"])
            jets_pass_dr = ak.fill_none(jets_pass_dr, True)
            good_jets = good_jets & jets_pass_dr

        if Jet == "Jet":
            good_jets = good_jets & ( ( (jets.pt < 50) & (jets.puId >= cuts["puId"]) ) | (jets.pt >= 50) )
            if btag != None:
                good_jets = good_jets & ( getattr(jets, btag["btagging_algorithm"]) > btag["btagging_WP"] )
        elif Jet == "FatJet":
            raise NotImplementedError

    return jets[good_jets]

def get_dilepton(electrons, muons, transverse=False):

    fields = {
            "pt": None,
            "eta": None,
            "phi": None,
            "mass": None,
            "charge": None,
    }

    electrons = ak.pad_none(electrons, 2)
    muons = ak.pad_none(muons, 2)

    nelectrons = ak.num(electrons[~ak.is_none(electrons, axis=1)])
    nmuons = ak.num(muons[~ak.is_none(muons, axis=1)])

    ee = electrons[:,0] + electrons[:,1]
    mumu = muons[:,0] + muons[:,1]
    emu = electrons[:,0] + muons[:,0]

    for var in fields.keys():
        fields[var] = ak.where( ((nelectrons + nmuons) == 2) & (nelectrons == 2), getattr(ee, var), getattr(ee, var) )
        fields[var] = ak.where( ((nelectrons + nmuons) == 2) & (nmuons == 2), getattr(mumu, var), fields[var] )
        fields[var] = ak.where( ((nelectrons + nmuons) == 2) & (nelectrons == 1) & (nmuons == 1), getattr(emu, var), fields[var] )

    if transverse:
        fields["eta"] = ak.zeros_like(fields["pt"])
    dileptons = ak.zip(fields, with_name="PtEtaPhiMCandidate")

    return dileptons

def get_diboson(dileptons, MET, transverse=False):

    fields = {
            "pt": MET.pt,
            "eta": ak.zeros_like(MET.pt),
            "phi": MET.phi,
            "mass": ak.zeros_like(MET.pt),
            "charge": ak.zeros_like(MET.pt),
    }

    METs = ak.zip(fields, with_name="PtEtaPhiMCandidate")
    if transverse:
        dileptons_t = dileptons[:]
        dileptons_t["eta"] = ak.zeros_like(dileptons_t.eta)
        dibosons = (dileptons_t + METs)
    else:
        dibosons = (dileptons + METs)

    return dibosons

def get_charged_leptons(electrons, muons, charge, mask):

    fields = {
            "pt": None,
            "eta": None,
            "phi": None,
            "mass": None,
            "energy": None,
            "charge": None,
            "x": None,
            "y": None,
            "z": None,
    }

    nelectrons = ak.num(electrons)
    nmuons = ak.num(muons)
    mask_ee = mask & ((nelectrons + nmuons) == 2) & (nelectrons == 2)
    mask_mumu = mask & ((nelectrons + nmuons) == 2) & (nmuons == 2)
    mask_emu = mask & ((nelectrons + nmuons) == 2) & (nelectrons == 1) & (nmuons == 1)

    for var in fields.keys():
        if var in ["eta", "phi"]:
            default = ak.from_iter(len(electrons)*[[-9.]])
        else:
            default = ak.from_iter(len(electrons)*[[-999.9]])
        if not var in ["energy", "x", "y", "z"]:
            fields[var] = ak.where( mask_ee, electrons[var][electrons.charge == charge], default )
            fields[var] = ak.where( mask_mumu, muons[var][muons.charge == charge], fields[var] )
            fields[var] = ak.where( mask_emu & ak.any(electrons.charge == charge, axis=1), electrons[var][electrons.charge == charge], fields[var] )
            fields[var] = ak.where( mask_emu & ak.any(muons.charge == charge, axis=1), muons[var][muons.charge == charge], fields[var] )
            fields[var] = ak.flatten(fields[var])
        else:
            fields[var] = ak.where( mask_ee, getattr(electrons, var)[electrons.charge == charge], default )
            fields[var] = ak.where( mask_mumu, getattr(muons, var)[muons.charge == charge], fields[var] )
            fields[var] = ak.where( mask_emu & ak.any(electrons.charge == charge, axis=1), getattr(electrons, var)[electrons.charge == charge], fields[var] )
            fields[var] = ak.where( mask_emu & ak.any(muons.charge == charge, axis=1), getattr(muons, var)[muons.charge == charge], fields[var] )
            fields[var] = ak.flatten(fields[var])

    charged_leptons = ak.zip(fields, with_name="PtEtaPhiMCandidate")

    return charged_leptons

def jet_nohiggs_selection(jets, mask_jets, fatjets, dr=1.2):

    #nested_mask = jets.p4.match(fatjets.p4[mask_fatjets,0], matchfunc=pass_dr, dr=dr)
    nested_mask = jets.match(fatjets, matchfunc=pass_dr, dr=dr)
    jets_pass_dr = nested_mask.all()

    return mask_jets & jets_pass_dr
