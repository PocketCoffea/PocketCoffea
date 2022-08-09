import sys

import numpy as np
import awkward as ak

import correctionlib

from ..parameters.lepton_scale_factors import electronSF, muonSF, electronJSONfiles, muonJSONfiles
from ..parameters.jet_scale_factors import btagSF

def get_ele_sf(year, pt, eta, counts, type='', pt_region=None):
    '''
    This function computes the per-electron reco or id SF.
    If 'reco', the appropriate corrections are chosen by using the argument `pt_region`.
    '''
    if type not in electronSF.keys():
        sys.exit("SF type not included in the parameters.")
    electronJSON = correctionlib.CorrectionSet.from_file(electronJSONfiles[year]['file'])
    map_name = electronJSONfiles[year]['name']
    if type == 'reco':
        sfName = electronSF[type][pt_region]
    elif type == 'id':
        sfName = electronSF[type]

    sf     = electronJSON[map_name].evaluate(year, "sf",     sfName, eta.to_numpy(), pt.to_numpy())
    sfup   = electronJSON[map_name].evaluate(year, "sfup",   sfName, eta.to_numpy(), pt.to_numpy())
    sfdown = electronJSON[map_name].evaluate(year, "sfdown", sfName, eta.to_numpy(), pt.to_numpy())

    # The unflattened arrays are returned in order to have one row per event.
    return ak.unflatten(sf, counts), ak.unflatten(sfup, counts), ak.unflatten(sfdown, counts)

def get_mu_sf(year, pt, eta, counts, type=''):
    '''
    This function computes the per-muon id or iso SF.
    '''
    if type not in muonSF.keys():
        sys.exit("SF type not included in the parameters.")

    muonJSON = correctionlib.CorrectionSet.from_file(muonJSONfiles[year]['file'])
    sfName = muonSF[type]

    year   = f"{year}_UL" # N.B. This works for the UL SF in the current JSON scheme (25.05.2022)
    sf     = muonJSON[sfName].evaluate(year, np.abs(eta.to_numpy()), pt.to_numpy(), "sf")
    sfup   = muonJSON[sfName].evaluate(year, np.abs(eta.to_numpy()), pt.to_numpy(), "systup")
    sfdown = muonJSON[sfName].evaluate(year, np.abs(eta.to_numpy()), pt.to_numpy(), "systdown")

    # The unflattened arrays are returned in order to have one row per event.
    return ak.unflatten(sf, counts), ak.unflatten(sfup, counts), ak.unflatten(sfdown, counts)

def sf_ele_reco(events, year):
    '''
    This function computes the per-electron reco SF and returns the corresponding per-event SF, obtained by multiplying the per-electron SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    Electrons are split into two categories based on a pt cut at 20 GeV, so that the proper SF is applied.
    '''

    ele_pt  = events.ElectronGood.pt
    ele_eta = events.ElectronGood.eta

    Above20 = ele_pt >= 20
    Below20 = ele_pt < 20

    # Since `correctionlib` does not support jagged arrays as an input, the pt and eta arrays are flattened.
    ele_pt_Above20,  ele_counts_Above20 = ak.flatten(ele_pt[Above20]), ak.num(ele_pt[Above20])
    ele_eta_Above20 = ak.flatten(ele_eta[Above20])

    ele_pt_Below20,  ele_counts_Below20 = ak.flatten(ele_pt[Below20]), ak.num(ele_pt[Below20])
    ele_eta_Below20 = ak.flatten(ele_eta[Below20])

    sf_reco_Above20, sfup_reco_Above20, sfdown_reco_Above20 = get_ele_sf(year, ele_pt_Above20, ele_eta_Above20, ele_counts_Above20, 'reco', 'pt>20')
    sf_reco_Below20, sfup_reco_Below20, sfdown_reco_Below20 = get_ele_sf(year, ele_pt_Below20, ele_eta_Below20, ele_counts_Below20, 'reco', 'pt<20')

    # The SF arrays corresponding to the electrons with pt above and below 20 GeV are concatenated and multiplied along the electron axis in order to obtain a per-event scale factor.
    sf_reco     = ak.prod( ak.concatenate( (sf_reco_Above20,     sf_reco_Below20),     axis=1 ), axis=1 )
    sfup_reco   = ak.prod( ak.concatenate( (sfup_reco_Above20,   sfup_reco_Below20),   axis=1 ), axis=1 )
    sfdown_reco = ak.prod( ak.concatenate( (sfdown_reco_Above20, sfdown_reco_Below20), axis=1 ), axis=1 )

    return sf_reco, sfup_reco, sfdown_reco

def sf_ele_id(events, year):
    '''
    This function computes the per-electron id SF and returns the corresponding per-event SF, obtained by multiplying the per-electron SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    ele_pt  = events.ElectronGood.pt
    ele_eta = events.ElectronGood.eta

    ele_pt_flat, ele_eta_flat, ele_counts = ak.flatten(ele_pt), ak.flatten(ele_eta), ak.num(ele_pt)
    sf_id, sfup_id, sfdown_id = get_ele_sf(year, ele_pt_flat, ele_eta_flat, ele_counts, 'id')

    # The SF arrays corresponding to the electrons are multiplied along the electron axis in order to obtain a per-event scale factor.
    return ak.prod(sf_id, axis=1), ak.prod(sfup_id, axis=1), ak.prod(sfdown_id, axis=1)

def sf_mu(events, year, type=''):
    '''
    This function computes the per-muon id SF and returns the corresponding per-event SF, obtained by multiplying the per-muon SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    mu_pt  = events.MuonGood.pt
    mu_eta = events.MuonGood.eta

    # Since `correctionlib` does not support jagged arrays as an input, the pt and eta arrays are flattened.
    mu_pt_flat, mu_eta_flat, mu_counts = ak.flatten(mu_pt), ak.flatten(mu_eta), ak.num(mu_pt)
    sf, sfup, sfdown = get_mu_sf(year, mu_pt_flat, mu_eta_flat, mu_counts, type)

    # The SF arrays corresponding to all the muons are multiplied along the muon axis in order to obtain a per-event scale factor.
    return ak.prod(sf, axis=1), ak.prod(sfup, axis=1), ak.prod(sfdown, axis=1)




def sf_btag(jets, btag_discriminator, year, variation="central"):
    '''
    DeepJet AK4 btagging SF. See https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/BTV_btagging_Run2_UL/BTV_btagging_2018_UL.html
    The scale factors have 8 default uncertainty 
    sources (hf,lf,hfstats1/2,lfstats1/2,cferr1/2) (all of this up_*var*, and down_*var*).
    All except the cferr1/2 uncertainties are to be 
    applied to light and b jets. The cferr1/2 uncertainties are to be applied to c jets. 
    hf/lfstats1/2 uncertainties are to be decorrelated between years, the others correlated.
    '''
    cset = correctionlib.CorrectionSet.from_file(btagSF[year])
    corr = cset["deepJet_shape"]

    #if variation in ["cferr1"]
    
    flavour = ak.to_numpy(ak.flatten(jets.hadronFlavour))
    abseta = np.abs(ak.to_numpy(ak.flatten(jets.eta)))
    pt = ak.to_numpy(ak.flatten(jets.pt))
    discr = ak.to_numpy(ak.flatten(jets[btag_discriminator]))
    counts = ak.num(jets)

    w = ak.unflatten(corr.evaluate(variation, flavour, abseta, pt, discr), counts)
    # product over the jets
    return ak.prod(w, axis=1)

    
    
