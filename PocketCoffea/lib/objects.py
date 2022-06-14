import os
import copy
import argparse

import awkward as ak
import numpy as np
import math
import uproot
import correctionlib
from vector import MomentumObject4D

from coffea import hist, lookup_tools
from coffea.nanoevents.methods import nanoaod

from ..parameters.preselection import object_preselection
from ..parameters.jec import JECjsonFiles
from ..lib.deltaR_matching import get_matching_pairs_indices, object_matching

ak.behavior.update(nanoaod.behavior)
    
# example here: https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/master/examples/jercExample.py
def jet_correction(events, Jet, typeJet, year, JECversion, JERversion=None, verbose=False):
    jsonfile = JECjsonFiles[year][[t for t in ['AK4', 'AK8'] if typeJet.startswith(t)][0]]
    JECfile = correctionlib.CorrectionSet.from_file(jsonfile)
    corr = JECfile.compound[f'{JECversion}_L1L2L3Res_{typeJet}']

    # until correctionlib handles jagged data natively we have to flatten and unflatten
    jets = events[Jet]
    jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
    jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
    jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
    j, nj = ak.flatten(jets), ak.num(jets)
    flatCorrFactor = corr.evaluate( np.array(j['area']), np.array(j['eta']), np.array(j['pt_raw']), np.array(j['rho']) )
    corrFactor = ak.unflatten(flatCorrFactor, nj)

    jets_corrected = copy.copy(jets)
    jets_corrected['pt']   = jets['pt_raw'] * corrFactor
    jets_corrected['mass'] = jets['mass_raw'] * corrFactor
    jets_corrected['rho']  = jets['rho']

    seed = events.event[0]

    if verbose:
        print()
        print(seed, 'JEC: starting columns:',ak.fields(jets), end='\n\n')

        print(seed, 'JEC: untransformed pt ratios',jets.pt/jets.pt_raw)
        print(seed, 'JEC: untransformed mass ratios',jets.mass/jets.mass_raw)

        print(seed, 'JEC: corrected pt ratios',jets_corrected.pt/jets_corrected.pt_raw)
        print(seed, 'JEC: corrected mass ratios',jets_corrected.mass/jets_corrected.mass_raw)

        print()
        print(seed, 'JEC: corrected columns:', ak.fields(jets_corrected), end='\n\n')

        #print('JES UP pt ratio',jets_corrected.JES_jes.up.pt/jets_corrected.pt_raw)
        #print('JES DOWN pt ratio',jets_corrected.JES_jes.down.pt/jets_corrected.pt_raw, end='\n\n')

    # Apply JER pt smearing (https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution)
    # The hybrid scaling method is implemented: if a jet is matched to a gen-jet, the scaling method is applied;
    # if a jet is not gen-matched, the stochastic smearing is applied.
    if JERversion:
        sf  = JECfile[f'{JERversion}_ScaleFactor_{typeJet}']
        res = JECfile[f'{JERversion}_PtResolution_{typeJet}']
        genjets = events['GenJet']
        j, nj   = ak.flatten(jets_corrected), ak.num(jets_corrected)
        gj, ngj = ak.flatten(genjets), ak.num(genjets)
        scaleFactor_flat  = sf.evaluate(j['eta'].to_numpy(), 'nom')
        ptResolution_flat = res.evaluate(j['eta'].to_numpy(), j['pt'].to_numpy(), j['rho'].to_numpy())
        scaleFactor  = ak.unflatten(scaleFactor_flat, nj)
        ptResolution = ak.unflatten(ptResolution_flat, nj)
        # Match jets with gen-level jets, with DeltaR and DeltaPt requirements
        dr_min = {'AK4PFchs' : 0.2, 'AK8PFPuppi' : 0.4}[typeJet]  # Match jets within a cone with half the jet radius
        pt_min = 3 * ptResolution * jets_corrected['pt']          # Match jets whose pt does not differ more than 3 sigmas from the gen-level pt
        matched_genjets, matched_jets, deltaR_matched = object_matching(genjets, jets_corrected, dr_min, pt_min)
        # Compute energy correction factor with the scaling method
        detSmear = 1 + (scaleFactor - 1) * (matched_jets['pt'] - matched_genjets['pt']) / matched_jets['pt']
        # Compute energy correction factor with the stochastic method
        np.random.seed(seed)
        seed_dict = {}
        filename   = events.metadata['filename']
        entrystart = events.metadata['entrystart']
        entrystop  = events.metadata['entrystop']
        seed_dict[f'chunk_{filename}_{entrystart}-{entrystop}'] = seed
        rand_gaus = np.random.normal(np.zeros_like(ptResolution_flat), ptResolution_flat)
        jersmear  = ak.unflatten(rand_gaus, nj)
        sqrt_arg_flat = scaleFactor_flat**2 - 1
        sqrt_arg_flat = ak.where(sqrt_arg_flat > 0, sqrt_arg_flat, ak.zeros_like(sqrt_arg_flat))
        sqrt_arg      = ak.unflatten(sqrt_arg_flat, nj)
        stochSmear = 1 + jersmear * np.sqrt(sqrt_arg)
        isMatched = ~ak.is_none(matched_jets.pt, axis=1)
        smearFactor = ak.where(isMatched, detSmear, stochSmear)

        jets_smeared = copy.copy(jets_corrected)
        jets_smeared['pt']   = jets_corrected['pt'] * smearFactor
        jets_smeared['mass'] = jets_corrected['mass'] * smearFactor

        if verbose:
            print()
            print(seed, "JER: isMatched", isMatched)
            print(seed, "JER: matched_jets.pt", matched_jets.pt)
            print(seed, "JER: smearFactor", smearFactor, end='\n\n')

            print(seed, 'JER: corrected pt ratios', jets_corrected.pt/jets_corrected.pt_raw)
            print(seed, 'JER: corrected mass ratios', jets_corrected.mass/jets_corrected.mass_raw)

            print(seed, 'JER: smeared pt ratios', jets_smeared.pt/jets_corrected.pt)
            print(seed, 'JER: smeared mass ratios', jets_smeared.mass/jets_corrected.mass)

            print()
            print(seed, 'JER: corrected columns:', ak.fields(jets_smeared), end='\n\n')

        return jets_smeared, seed_dict
    else:
        return jets_corrected

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

def jet_selection(events, Jet, finalstate):

    jets = events[Jet]
    cuts = object_preselection[finalstate][Jet]
    # Only jets that are more distant than dr to ALL leptons are tagged as good jets
    leptons = events["LeptonGood"]
    # Mask for  jets not passing the preselection
    presel_mask = (jets.pt > cuts["pt"]) & (np.abs(jets.eta) < cuts["eta"]) & (jets.jetId >= cuts["jetId"])
    # Lepton cleaning
    dR_jets_lep = jets.metric_table(leptons)
    lepton_cleaning_mask = ak.prod(dR_jets_lep> cuts["dr"], axis=2) == 1

    if Jet == "Jet":
       jetpuid_mask  =  ( (jets.pt < cuts["puId_ptlim"]) & (jets.puId >= cuts["puId"]) ) | (jets.pt >= 50) 
       good_jets_mask = presel_mask & lepton_cleaning_mask & jetpuid_mask
       
    elif Jet == "FatJet":
        raise NotImplementedError

    return jets[good_jets_mask], good_jets_mask


def btagging(Jet, btag):
    return  Jet[Jet[btag["btagging_algorithm"]] > btag["btagging_WP"]]


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
