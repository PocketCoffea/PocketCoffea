import numpy as np
import awkward as ak
import correctionlib


def get_ele_sf(
    params, year, pt, eta, counts=None, key='', pt_region=None, variations=["nominal"]
):
    '''
    This function computes the per-electron reco or id SF.
    If 'reco', the appropriate corrections are chosen by using the argument `pt_region`.
    '''
    electronSF = params["lepton_scale_factors"]["electron_sf"]

    if key in ['reco', 'id']:
        electron_correctionset = correctionlib.CorrectionSet.from_file(
            electronSF.JSONfiles[year]["file"]
        )
        map_name = electronSF.JSONfiles[year]["name"]

        if key == 'reco':
            sfname = electronSF["reco"][pt_region]
        elif key == 'id':
            sfname = electronSF["id"]

        # translate the `year` key into the corresponding key in the correction file provided by the EGM-POG
        year_pog = electronSF["era_mapping"][year]

        sf = electron_correctionset[map_name].evaluate(
            year_pog, "sf", sfname, eta.to_numpy(), pt.to_numpy()
        )
        sfup = electron_correctionset[map_name].evaluate(
            year_pog, "sfup", sfname, eta.to_numpy(), pt.to_numpy()
        )
        sfdown = electron_correctionset[map_name].evaluate(
            year_pog, "sfdown", sfname, eta.to_numpy(), pt.to_numpy()
        )
        # The unflattened arrays are returned in order to have one row per event.
        return (
            ak.unflatten(sf, counts),
            ak.unflatten(sfup, counts),
            ak.unflatten(sfdown, counts),
        )
    elif key == 'trigger':

        electron_correctionset = correctionlib.CorrectionSet.from_file(
            electronSF.trigger_sf[year]["file"]
        )
        map_name = electronSF.trigger_sf[year]["name"]

        output = {}
        for variation in variations:
            if variation == "nominal":
                output[variation] = [
                    electron_correctionset[map_name].evaluate(
                        variation, pt.to_numpy(), eta.to_numpy()
                    )
                ]
            else:
                # Nominal sf==1
                nominal = np.ones_like(pt.to_numpy())
                # Systematic variations
                output[variation] = [
                    nominal,
                    electron_correctionset[map_name].evaluate(
                        f"{variation}Up", pt.to_numpy(), eta.to_numpy()
                    ),
                    electron_correctionset[map_name].evaluate(
                        f"{variation}Down", pt.to_numpy(), eta.to_numpy()
                    ),
                ]
            for i, sf in enumerate(output[variation]):
                output[variation][i] = ak.unflatten(sf, counts)

        return output


def get_mu_sf(params, year, pt, eta, counts, key=''):
    '''
    This function computes the per-muon id or iso SF.
    '''
    muonSF = params["lepton_scale_factors"]["muon_sf"]

    muon_correctionset = correctionlib.CorrectionSet.from_file(
        muonSF.JSONfiles[year]['file']
    )
    sfName = muonSF.sf_name[year][key]

    year_pog = muonSF.era_mapping[year]
    sf = muon_correctionset[sfName].evaluate(
        year_pog, np.abs(eta.to_numpy()), pt.to_numpy(), "sf"
    )
    sfup = muon_correctionset[sfName].evaluate(
        year_pog, np.abs(eta.to_numpy()), pt.to_numpy(), "systup"
    )
    sfdown = muon_correctionset[sfName].evaluate(
        year_pog, np.abs(eta.to_numpy()), pt.to_numpy(), "systdown"
    )

    # The unflattened arrays are returned in order to have one row per event.
    return (
        ak.unflatten(sf, counts),
        ak.unflatten(sfup, counts),
        ak.unflatten(sfdown, counts),
    )


def sf_ele_reco(params, events, year):
    '''
    This function computes the per-electron reco SF and returns the corresponding per-event SF, obtained by multiplying the per-electron SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    Electrons are split into two categories based on a pt cut at 20 GeV, so that the proper SF is applied.
    '''

    ele_pt = events.ElectronGood.pt
    ele_eta = events.ElectronGood.etaSC

    Above20 = ele_pt >= 20
    Below20 = ele_pt < 20

    # Since `correctionlib` does not support jagged arrays as an input, the pt and eta arrays are flattened.
    ele_pt_Above20, ele_counts_Above20 = ak.flatten(ele_pt[Above20]), ak.num(
        ele_pt[Above20]
    )
    ele_eta_Above20 = ak.flatten(ele_eta[Above20])

    ele_pt_Below20, ele_counts_Below20 = ak.flatten(ele_pt[Below20]), ak.num(
        ele_pt[Below20]
    )
    ele_eta_Below20 = ak.flatten(ele_eta[Below20])

    sf_reco_Above20, sfup_reco_Above20, sfdown_reco_Above20 = get_ele_sf(
        params,
        year,
        ele_pt_Above20,
        ele_eta_Above20,
        ele_counts_Above20,
        'reco',
        'pt_gt_20',
    )
    sf_reco_Below20, sfup_reco_Below20, sfdown_reco_Below20 = get_ele_sf(
        params,
        year,
        ele_pt_Below20,
        ele_eta_Below20,
        ele_counts_Below20,
        'reco',
        'pt_lt_20',
    )

    # The SF arrays corresponding to the electrons with pt above and below 20 GeV are concatenated and multiplied along the electron axis in order to obtain a per-event scale factor.
    sf_reco = ak.prod(
        ak.concatenate((sf_reco_Above20, sf_reco_Below20), axis=1), axis=1
    )
    sfup_reco = ak.prod(
        ak.concatenate((sfup_reco_Above20, sfup_reco_Below20), axis=1), axis=1
    )
    sfdown_reco = ak.prod(
        ak.concatenate((sfdown_reco_Above20, sfdown_reco_Below20), axis=1), axis=1
    )

    return sf_reco, sfup_reco, sfdown_reco


def sf_ele_id(params, events, year):
    '''
    This function computes the per-electron id SF and returns the corresponding per-event SF, obtained by multiplying the per-electron SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    ele_pt = events.ElectronGood.pt
    ele_eta = events.ElectronGood.etaSC

    ele_pt_flat, ele_eta_flat, ele_counts = (
        ak.flatten(ele_pt),
        ak.flatten(ele_eta),
        ak.num(ele_pt),
    )
    sf_id, sfup_id, sfdown_id = get_ele_sf(
        params, year, ele_pt_flat, ele_eta_flat, ele_counts, 'id'
    )

    # The SF arrays corresponding to the electrons are multiplied along the electron axis in order to obtain a per-event scale factor.
    return ak.prod(sf_id, axis=1), ak.prod(sfup_id, axis=1), ak.prod(sfdown_id, axis=1)


def sf_ele_trigger(params, events, year, variations=["nominal"]):
    '''
    This function computes the semileptonic electron trigger SF by considering the leading electron in the event.
    This computation is valid only in the case of the semileptonic final state.
    Additionally, also the up and down variations of the SF for a set of systematic uncertainties are returned.
    '''

    ele_pt = events.ElectronGood.pt
    ele_eta = events.ElectronGood.etaSC

    ele_pt_flat, ele_eta_flat, ele_counts = (
        ak.flatten(ele_pt),
        ak.flatten(ele_eta),
        ak.num(ele_pt),
    )
    sf_dict = get_ele_sf(
        params,
        year,
        ele_pt_flat,
        ele_eta_flat,
        ele_counts,
        'trigger',
        variations=variations,
    )

    for variation in sf_dict.keys():
        for i, sf in enumerate(sf_dict[variation]):
            sf_dict[variation][i] = ak.prod(sf, axis=1)

    return sf_dict


def sf_mu(params, events, year, key=''):
    '''
    This function computes the per-muon id SF and returns the corresponding per-event SF, obtained by multiplying the per-muon SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    mu_pt = events.MuonGood.pt
    mu_eta = events.MuonGood.eta

    # Since `correctionlib` does not support jagged arrays as an input, the pt and eta arrays are flattened.
    mu_pt_flat, mu_eta_flat, mu_counts = (
        ak.flatten(mu_pt),
        ak.flatten(mu_eta),
        ak.num(mu_pt),
    )
    sf, sfup, sfdown = get_mu_sf(params, year, mu_pt_flat, mu_eta_flat, mu_counts, key)

    # The SF arrays corresponding to all the muons are multiplied along the
    # muon axis in order to obtain a per-event scale factor.
    return ak.prod(sf, axis=1), ak.prod(sfup, axis=1), ak.prod(sfdown, axis=1)


def sf_btag(params, jets, year, njets, variations=["central"]):
    '''
    DeepJet AK4 btagging SF.
    See https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/summaries/BTV_2018_UL_btagging.html
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
    btagSF = params.jet_scale_factors.btagSF[year]
    btag_discriminator = params.btagging.working_point[year]["btagging_algorithm"]
    cset = correctionlib.CorrectionSet.from_file(btagSF.file)
    corr = cset[btagSF.name]

    flavour = ak.to_numpy(ak.flatten(jets.hadronFlavour))
    abseta = np.abs(ak.to_numpy(ak.flatten(jets.eta)))
    pt = ak.to_numpy(ak.flatten(jets.pt))
    discr = ak.to_numpy(ak.flatten(jets[btag_discriminator]))

    central_SF_byjet = corr.evaluate("central", flavour, abseta, pt, discr)

    def _get_sf_variation_with_mask(variation, mask):
        index = (np.indices(discr.shape)).flatten()[mask]
        # Copying the central SF
        sf = np.copy(central_SF_byjet)
        w = corr.evaluate(variation, flavour[mask], abseta[mask], pt[mask], discr[mask])
        sf[index] = w
        sf_out = ak.prod(ak.unflatten(sf, njets), axis=1)
        return sf_out

    output = {}
    for variation in variations:
        if variation == "central":
            output[variation] = [ak.prod(ak.unflatten(central_SF_byjet, njets), axis=1)]
        else:
            # Nominal sf==1
            nominal = np.ones(ak.num(njets, axis=0))
            # Systematic variations
            if "cferr" in variation:
                # Computing the scale factor only on c-flavour jets
                c_mask = flavour == 4
                output[variation] = [
                    nominal,
                    _get_sf_variation_with_mask(f"up_{variation}", c_mask),
                    _get_sf_variation_with_mask(f"down_{variation}", c_mask),
                ]

            elif "JES" in variation:
                # We need to convert the name of the variation
                # from JES_VariationUp to  up_jesVariation
                if variation == "JES_TotalUp":
                    btag_jes_var = "up_jes"
                elif variation == "JES_TotalDown":
                    btag_jes_var = "down_jes"
                else:
                    if variation[-2:] == "Up":
                        btag_jes_var = f"up_jes{variation[4:-2]}"
                    elif variation[-4:] == "Down":
                        btag_jes_var = f"down_jes{variation[4:-4]}"

                # This is a special case where a dedicate btagSF is computed for up and down Jes shape variations.
                # This is not an up/down variation, but a single modified SF.
                # N.B: It is a central SF
                # notc_mask = flavour != 4
                # output["central"] = [_get_sf_variation_with_mask(btag_jes_var, notc_mask)]
                output["central"] = [
                    ak.prod(
                        ak.unflatten(
                            corr.evaluate("central", flavour, abseta, pt, discr), njets
                        ),
                        axis=1,
                    )
                ]
            else:
                # Computing the scale factor only NON c-flavour jets
                notc_mask = flavour != 4
                output[variation] = [
                    nominal,
                    _get_sf_variation_with_mask(f"up_{variation}", notc_mask),
                    _get_sf_variation_with_mask(f"down_{variation}", notc_mask),
                ]

    return output


def sf_btag_calib(params, sample, year, njets, jetsHt):
    '''Correction to btagSF computing by comparing the inclusive shape without btagSF and with btagSF in 2D:
    njets-JetsHT bins. Each sample/year has a different correction stored in the correctionlib format.'''
    cset = correctionlib.CorrectionSet.from_file(
        params.btagSF_calibration[year]["file"]
    )
    corr = cset[params.btagSF_calibration[year]["name"]]
    w = corr.evaluate(sample, year, ak.to_numpy(njets), ak.to_numpy(jetsHt))
    return w


def sf_jet_puId(params, jets, year, njets):
    # The SF is applied only on jets passing the preselection (JetGood), pt < maxpt, and matched to a GenJet.
    # In other words the SF is not applied on jets not passing the Jet Pu ID SF.
    # We DON'T assume that the function is applied on JetGood, so we REAPPLY jetPUID selections to be sure.
    # We apply also the pt limit.
    jet_puId_cfg = params.object_preselection["Jet"]["puId"]

    pt = ak.to_numpy(ak.flatten(jets.pt))
    eta = ak.to_numpy(ak.flatten(jets.eta))
    puId = ak.to_numpy(ak.flatten(jets.puId))
    genJetId_mask = ak.flatten(jets.genJetIdx >= 0)

    # GenGet matching by index, needs some checkes
    cset = correctionlib.CorrectionSet.from_file(
        params.jet_scale_factors.jet_puId[year]["file"]
    )
    corr = cset[params.jet_scale_factors.jet_puId[year]["name"]]

    # Requiring jet < maxpt, passing the jetpuid WP and matched to a GenJet (with a genJet idx != -1)
    mask = (
        (pt < jet_puId_cfg["maxpt"]) & (puId >= jet_puId_cfg["value"]) & genJetId_mask
    )
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


def sf_L1prefiring(events):
    '''Correction due to the wrong association of L1 trigger primitives (TP) in ECAL to the previous bunch crossing,
    also known as "L1 prefiring".
    The event weights produced by the latest version of the producer are included in nanoAOD starting from version V9.
    The function returns the nominal, up and down L1 prefiring weights.'''
    L1PreFiringWeight = events.L1PreFiringWeight

    return L1PreFiringWeight['Nom'], L1PreFiringWeight['Up'], L1PreFiringWeight['Dn']


def sf_pileup_reweight(params, events, year):
    puFile = params.pileupJSONfiles[year]['file']
    puName = params.pileupJSONfiles[year]['name']

    puWeightsJSON = correctionlib.CorrectionSet.from_file(puFile)

    nPu = events.Pileup.nTrueInt.to_numpy()
    sf = puWeightsJSON[puName].evaluate(nPu, 'nominal')
    sfup = puWeightsJSON[puName].evaluate(nPu, 'up')
    sfdown = puWeightsJSON[puName].evaluate(nPu, 'down')

    return sf, sfup, sfdown
