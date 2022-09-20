import sys

import numpy as np
import awkward as ak

import correctionlib

from ..parameters.lepton_scale_factors import electronSF, muonSF, electronJSONfiles, muonJSONfiles
from ..parameters.jet_scale_factors import btagSF, btagSF_calibration, jet_puId
from ..parameters.object_preselection import object_preselection

def get_ele_sf(year, pt, eta, counts=None, type='', pt_region=None):
    '''
    This function computes the per-electron reco or id SF.
    If 'reco', the appropriate corrections are chosen by using the argument `pt_region`.
    '''
    if type not in electronSF.keys():
        sys.exit("SF type not included in the parameters.")

    if type == 'reco':
        sfName = electronSF[type][pt_region]
        electronJSON = correctionlib.CorrectionSet.from_file(electronJSONfiles[year]['file_POG'])
        map_name = electronJSONfiles[year]['name']
    elif type == 'id':
        sfName = electronSF[type]
        electronJSON = correctionlib.CorrectionSet.from_file(electronJSONfiles[year]['file_POG'])
        map_name = electronJSONfiles[year]['name']
    elif type == 'trigger':
        if counts != None:
            sys.exit("The `counts` array should not be passed since the trigger SF are computed for the semileptonic final state and the electron arrays are already flat.")
        electronJSON = correctionlib.CorrectionSet.from_file(electronJSONfiles[year]['file_triggerSF'])
        map_name = electronSF[type][year]

    if type in ['reco', 'id']:
        sf     = electronJSON[map_name].evaluate(year, "sf",     sfName, eta.to_numpy(), pt.to_numpy())
        sfup   = electronJSON[map_name].evaluate(year, "sfup",   sfName, eta.to_numpy(), pt.to_numpy())
        sfdown = electronJSON[map_name].evaluate(year, "sfdown", sfName, eta.to_numpy(), pt.to_numpy())
        # The unflattened arrays are returned in order to have one row per event.
        return ak.unflatten(sf, counts), ak.unflatten(sfup, counts), ak.unflatten(sfdown, counts)
    ### N.B.: In the current implementation, only the statistical uncertainty is included in the up/down variations of the trigger SF
    elif type == 'trigger':
        sf     = electronJSON[map_name].evaluate("nominal",  pt.to_numpy(), eta.to_numpy())
        sfup   = electronJSON[map_name].evaluate("statUp",   pt.to_numpy(), eta.to_numpy())
        sfdown = electronJSON[map_name].evaluate("statDown", pt.to_numpy(), eta.to_numpy())
        return sf, sfup, sfdown

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

def sf_ele_trigger(events, year):
    '''
    This function computes the semileptonic electron trigger SF by considering the leading electron in the event.
    This computation is valid only in the case of the semileptonic final state.
    Additionally, also the up and down variations of the SF are returned.
    '''

    ele_pt  = ak.firsts(events.ElectronGood.pt)
    ele_eta = ak.firsts(events.ElectronGood.eta)

    return get_ele_sf(year, ele_pt, ele_eta, type='trigger')

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

def sf_btag(jets, btag_discriminator, year, njets, variations=["central"]):
    '''
    DeepJet AK4 btagging SF. See https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/BTV_btagging_Run2_UL/BTV_btagging_2018_UL.html
    The scale factors have 8 default uncertainty 
    sources (hf,lf,hfstats1/2,lfstats1/2,cferr1/2) (all of this up_*var*, and down_*var*).
    All except the cferr1/2 uncertainties are to be 
    applied to light and b jets. The cferr1/2 uncertainties are to be applied to c jets. 
    hf/lfstats1/2 uncertainties are to be decorrelated between years, the others correlated.
    Additional jes-varied scale factors are supplied to be applied for the jes variations.

    if variation is not one of the jes ones both the up and down sf is returned.
    If variation is a jet variation the argument must be up_jes* or down_jes* since it is applied on the specified
    Jes variation jets. 
    '''
    cset = correctionlib.CorrectionSet.from_file(btagSF[year])
    corr = cset["deepJet_shape"]

    flavour = ak.to_numpy(ak.flatten(jets.hadronFlavour))
    abseta = np.abs(ak.to_numpy(ak.flatten(jets.eta)))
    pt = ak.to_numpy(ak.flatten(jets.pt))
    discr = ak.to_numpy(ak.flatten(jets[btag_discriminator]))

    def _getsfwithmask(variation, mask ):
        index = (np.indices(discr.shape)).flatten()[mask]
        sf = np.ones_like(discr, dtype=float)
        w = corr.evaluate(variation, flavour[mask], abseta[mask], pt[mask], discr[mask])
        sf[index] = w
        sf_out = ak.prod(ak.unflatten(sf, njets), axis=1)
        return sf_out

    output = {}
    for variation in variations:        
        if variation == "central":
            output[variation] = (ak.prod(ak.unflatten(
                corr.evaluate(variation, flavour, abseta, pt, discr),
                njets), axis=1), )
        else:
            # Nominal sf==1 
            nominal = np.ones(ak.num(njets, axis=0))
            # Systematic variations
            if "cferr" in variation:
                # Computing the scale factor only on c-flavour jets
                c_mask = (flavour == 4)
                output[variation] = nominal, _getsfwithmask(f"up_{variation}", c_mask), _getsfwithmask(f"down_{variation}", c_mask)

            elif "jes" in variation:
                # This is a special case where a dedicate btagSF is computed for up and down Jes shape variations.
                # This is not an up/down variation, but a single modified SF.
                notc_mask = (flavour != 4)
                output[variation] =  _getsfwithmask(variation, notcmask)
            else:
                # Computing the scale factor only NON c-flavour jets
                notc_mask = (flavour != 4)
                output[variation] = nominal, _getsfwithmask(f"up_{variation}", notc_mask), _getsfwithmask(f"down_{variation}", notc_mask)

    return output

def sf_btag_calib(sample, year, njets, jetsHt):
    '''Correction to btagSF computing by comparing the inclusive shape without btagSF and with btagSF in 2D:
    njets-JetsHT bins. Each sample/year has a different correction stored in the correctionlib format.'''
    cset = correctionlib.CorrectionSet.from_file(btagSF_calibration[year])
    corr = cset["btagSF_norm_correction"]
    w = corr.evaluate(sample, year, ak.to_numpy(njets), ak.to_numpy(jetsHt))
    return w

def sf_jet_puId(jets, finalstate, year, njets):
    # The SF is applied only on jets passing the preselection (JetGood), pt < maxpt, and matched to a GenJet.
    # In other words the SF is not applied on jets not passing the Jet Pu ID SF.
    # We ASSUME that this function is applied on cleaned, preselected Jets == JetGood.
    # We don't reapply jet puId selection, only the pt limit.
    jet_puId_cfg = object_preselection[finalstate]["Jet"]["puId"]
    
    pt = ak.to_numpy(ak.flatten(jets.pt))
    eta = ak.to_numpy(ak.flatten(jets.eta))
    genJetId_mask = ak.flatten(jets.genJetIdx >= 0)

    # GenGet matching by index, needs some checkes
    cset = correctionlib.CorrectionSet.from_file(jet_puId[year])
    corr = cset["PUJetID_eff"]

    # Requiring jet < maxpt and matched to a GenJet (with a genJet idx != -1)
    mask = (pt < jet_puId_cfg["maxpt"]) & genJetId_mask
    index = (np.indices(pt.shape)).flatten()[mask]
    sf = np.ones_like(pt, dtype=float)
    sfup = np.ones_like(pt, dtype=float)
    sfdown = np.ones_like(pt, dtype=float)
    w = corr.evaluate(eta[mask], pt[mask], "nom", jet_puId_cfg["wp"])
    wup = corr.evaluate(eta[mask], pt[mask], "up", jet_puId_cfg["wp"])
    wdown = corr.evaluate(eta[mask], pt[mask], "down", jet_puId_cfg["wp"])
    sf[index] = w
    sfup[index] = wup
    sfdown[index] = wdown

    sf_out = ak.prod(ak.unflatten(sf, njets), axis=1)
    sf_up_out = ak.prod(ak.unflatten(sfup, njets), axis=1)
    sf_down_out = ak.prod(ak.unflatten(sfdown, njets), axis=1)

    return sf_out, sf_up_out, sf_down_out
