import awkward as ak

import correctionlib

from ..parameters.lepton_scale_factors import electronSF, electronJSONfiles

def get_sf(year, pt, eta, counts, type='', pt_region=None):
    '''
    This function computes the per-electron reco or id SF.
    If 'reco', the appropriate corrections are chosen by using the argument `pt_region`.
    '''
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

    sf_reco_Above20, sfup_reco_Above20, sfdown_reco_Above20 = get_sf(year, ele_pt_Above20, ele_eta_Above20, ele_counts_Above20, 'reco', 'pt>20')
    sf_reco_Below20, sfup_reco_Below20, sfdown_reco_Below20 = get_sf(year, ele_pt_Below20, ele_eta_Below20, ele_counts_Below20, 'reco', 'pt<20')

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
    sf_id, sfup_id, sfdown_id = get_sf(year, ele_pt_flat, ele_eta_flat, ele_counts, 'id')

    # The SF arrays corresponding to the electrons are multiplied along the electron axis in order to obtain a per-event scale factor.
    return ak.prod(sf_id, axis=1), ak.prod(sfup_id, axis=1), ak.prod(sfdown_id, axis=1)
