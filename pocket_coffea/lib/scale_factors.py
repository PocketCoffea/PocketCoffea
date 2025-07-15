import numpy as np
import awkward as ak
import correctionlib


def get_pho_sf(params, year, pt, eta, counts, key=''):
    '''
    This function computes the per-photon id or pxseed SF.
    '''
    photonSF = params["photon_scale_factors"]["photon_sf"]
    # translate the `year` key into the corresponding key in the correction file provided by the EGM-POG
    year_pog = photonSF["era_mapping"][year]

    photon_correctionset = correctionlib.CorrectionSet.from_file(
        photonSF.JSONfiles[year]['file']
    )        

    if key == "id":
    
        sfName = photonSF.sf_name[year][key]
    
        sf = photon_correctionset[sfName].evaluate(
        year_pog, "nominal", "Medium", np.abs(eta.to_numpy()), pt.to_numpy()
        )
        sfup = photon_correctionset[sfName].evaluate(
        year_pog, "systup", "Medium" , np.abs(eta.to_numpy()), pt.to_numpy() 
        )
        sfdown = photon_correctionset[sfName].evaluate(
        year_pog, "systdown", "Medium", np.abs(eta.to_numpy()), pt.to_numpy()
        )

    elif key == "pxseed":

        sfName = photonSF.sf_name[year][key]
    
        sf = photon_correctionset[sfName].evaluate(
        year_pog, "nominal", "Medium", 'EBInc')

        sfup = photon_correctionset[sfName].evaluate(
        year_pog, "systup", "Medium"  , 'EBInc')
            
        sfdown = photon_correctionset[sfName].evaluate(
        year_pog, "systdown", "Medium", 'EBInc')

    else:

        raise Exception(f"Photon SF key {key} not recognized")

  
    # The unflattened arrays are returned in order to have one row per event.
    return (
        ak.unflatten(sf, counts),
        ak.unflatten(sfup, counts),
        ak.unflatten(sfdown, counts),
    )


def sf_photon(params, events, year, key=''):
    '''
    This function computes the per-photon id SF and returns the corresponding per-event SF, obtained by multiplying the per-photon SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    coll = params.photon_scale_factors.photon_sf.collection
    pho_pt = events[coll].pt
    pho_eta = events[coll].eta

    # Since `correctionlib` does not support jagged arrays as an input, the pt and eta arrays are flattened.
    pho_pt_flat, pho_eta_flat, pho_counts = (
        ak.flatten(pho_pt),
        ak.flatten(pho_eta),
        ak.num(pho_pt),
    )
    sf, sfup, sfdown = get_pho_sf(params, year, pho_pt_flat, pho_eta_flat, pho_counts, key)

    # The SF arrays corresponding to all the photons are multiplied along the
    # photon axis in order to obtain a per-event scale factor.
    return ak.prod(sf, axis=1), ak.prod(sfup, axis=1), ak.prod(sfdown, axis=1)



def get_ele_sf(
        params, year, pt, eta, phi, counts=None, key='', pt_region=None, variations=["nominal"]
):
    '''
    This function computes the per-electron reco or id SF.
    If 'reco', the appropriate corrections are chosen by using the argument `pt_region`.
    '''
    electronSF = params["lepton_scale_factors"]["electron_sf"]
    # translate the `year` key into the corresponding key in the correction file provided by the EGM-POG
    year_pog = electronSF["era_mapping"][year]

    if key in ['reco', 'id']:
        electron_correctionset = correctionlib.CorrectionSet.from_file(
            electronSF.JSONfiles[year]["file"]
        )
        map_name = electronSF.JSONfiles[year]["name"]

        if key == 'reco':
            sfname = electronSF.JSONfiles[year]["reco"][pt_region]
        elif key == 'id':
            sfname = electronSF["id"][params.object_preselection["Electron"]["id"]]
        
        if year in ["2023_preBPix", "2023_postBPix"]:
            # Starting from 2023 SFs require the phi:
            if key == 'reco' and pt_region == 'pt_lt_20':
                # It also appears that for RecoBelow20 SFs the eta must be positive (absolute value).
                eta_np = np.abs(eta.to_numpy())
            else:
                eta_np = eta.to_numpy()
                
            sf = electron_correctionset[map_name].evaluate(
                year_pog, "sf", sfname, eta_np, pt.to_numpy(),  phi.to_numpy()
            )
            sfup = electron_correctionset[map_name].evaluate(
                year_pog, "sfup", sfname, eta_np, pt.to_numpy(), phi.to_numpy()
            )
            sfdown = electron_correctionset[map_name].evaluate(
                year_pog, "sfdown", sfname, eta_np, pt.to_numpy(), phi.to_numpy()
            )
        else:
            # All other eras do not need phi:    
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
    else:
        raise Exception(f"Invalid key `{key}` for get_ele_sf. Available keys are 'reco', 'id'.")


def sf_ele_trigger(params, events, year):
    """Compute electron trigger scale factors using the EGM JSON files with correctionlib.
    Returns the per-event scale factor for the trigger.

    Returns:
    --------
    tuple: (sf, sfup, sfdown) per-event scale factor
    """
    electronSF = params.lepton_scale_factors.electron_sf
    year_pog = electronSF.era_mapping[year]
    map_name = electronSF.trigger_sf[year].name
    trigger_path = electronSF.trigger_sf[year].path

    coll = electronSF.collection
    ele_pt = events[coll].pt
    ele_eta = events[coll].etaSC

    ele_pt_flat, ele_eta_flat, ele_counts = (
        ak.flatten(ele_pt).to_numpy(),
        ak.flatten(ele_eta).to_numpy(),
        ak.num(ele_pt),
    )

    electron_correctionset = correctionlib.CorrectionSet.from_file(
        electronSF.trigger_sf[year].file
    )
    corr_eval = electron_correctionset[map_name].evaluate

    # get sf, sfup, sfdown per electron
    scale_factors = [
        ak.unflatten(
            corr_eval(year_pog, variation, trigger_path, ele_eta_flat, ele_pt_flat),
            ele_counts,
        )
        for variation in ("sf", "sfup", "sfdown")
    ]

    # return a per-event scale factor by multiplying all electron scale factors
    return tuple(ak.prod(sf, axis=1) for sf in scale_factors)


def get_mu_sf(params, year, pt, eta, counts, key=''):
    '''
    This function computes the per-muon id or iso SF.
    '''
    muonSF = params["lepton_scale_factors"]["muon_sf"]

    muon_correctionset = correctionlib.CorrectionSet.from_file(
        muonSF.JSONfiles[year]['file']
    )

    if key not in ["id","iso","trigger"]:
        raise Exception(f"Muon SF key {key} not recognized")
    
    sfName = muonSF.sf_name[year][key]
    
    sf = muon_correctionset[sfName].evaluate(
        np.abs(eta.to_numpy()), pt.to_numpy(), "nominal"
    )
    sfup = muon_correctionset[sfName].evaluate(
        np.abs(eta.to_numpy()), pt.to_numpy(), "systup"
    )
    sfdown = muon_correctionset[sfName].evaluate(
        np.abs(eta.to_numpy()), pt.to_numpy(), "systdown"
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
    Electrons are split into two categories based on a pt cut depending on the Run preiod, so that the proper SF is applied.
    '''
    coll = params.lepton_scale_factors.electron_sf.collection
    ele_pt = events[coll].pt
    ele_eta = events[coll].etaSC # This is added on top of NanoAOD
    ele_phi = events[coll].phi

    pt_ranges = []
    if year in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
        pt_ranges += [("pt_lt_20", (ele_pt < 20)), 
                      ("pt_gt_20", (ele_pt >= 20))]
    elif year in ["2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix"]:
        pt_ranges += [("pt_lt_20", (ele_pt < 20)), 
                      ("pt_gt_20_lt_75", (ele_pt >= 20) & (ele_pt < 75)), 
                      ("pt_gt_75", (ele_pt >= 75))]
    else:
        raise Exception("For chosen year "+year+" sf_ele_reco are not implemented yet")
    
    sf_reco, sfup_reco, sfdown_reco = [], [], []

    for pt_range_key, pt_range in pt_ranges:
        ele_pt_inPtRange = ak.flatten(ele_pt[pt_range])
        ele_eta_inPtRange = ak.flatten(ele_eta[pt_range])
        ele_phi_inPtRange = ak.flatten(ele_phi[pt_range])
        ele_counts_inPtRange = ak.num(ele_pt[pt_range])

        sf_reco_inPtRange, sfup_reco_inPtRange, sfdown_reco_inPtRange = get_ele_sf(
            params,
            year,
            ele_pt_inPtRange,
            ele_eta_inPtRange,
            ele_phi_inPtRange,
            ele_counts_inPtRange,
            'reco',
            pt_range_key,
        )
        
        sf_reco.append(sf_reco_inPtRange)
        sfup_reco.append(sfup_reco_inPtRange)
        sfdown_reco.append(sfdown_reco_inPtRange)

    sf_reco = ak.prod(
        ak.concatenate(sf_reco, axis=1), axis=1
    )
    sfup_reco = ak.prod(
        ak.concatenate(sfup_reco, axis=1), axis=1
    )
    sfdown_reco = ak.prod(
        ak.concatenate(sfdown_reco, axis=1), axis=1
    )

    return sf_reco, sfup_reco, sfdown_reco


def sf_ele_id(params, events, year):
    '''
    This function computes the per-electron id SF and returns the corresponding per-event SF, obtained by multiplying the per-electron SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    coll = params.lepton_scale_factors.electron_sf.collection
    ele_pt = events[coll].pt
    ele_eta = events[coll].etaSC
    ele_phi = events[coll].phi

    ele_pt_flat, ele_eta_flat, ele_phi_flat, ele_counts = (
        ak.flatten(ele_pt),
        ak.flatten(ele_eta),
        ak.flatten(ele_phi),
        ak.num(ele_pt),
    )

    sf_id, sfup_id, sfdown_id = get_ele_sf(
        params, year, ele_pt_flat, ele_eta_flat, ele_phi_flat, ele_counts, 'id'
    )
        

    # The SF arrays corresponding to the electrons are multiplied along the electron axis in order to obtain a per-event scale factor.
    return ak.prod(sf_id, axis=1), ak.prod(sfup_id, axis=1), ak.prod(sfdown_id, axis=1)


def sf_mu(params, events, year, key=''):
    '''
    This function computes the per-muon id SF and returns the corresponding per-event SF, obtained by multiplying the per-muon SF in each event.
    Additionally, also the up and down variations of the SF are returned.
    '''
    coll = params.lepton_scale_factors.muon_sf.collection
    mu_pt = events[coll].pt
    mu_eta = events[coll].eta

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
    DeepJet (or other taggers) AK4 btagging SF.
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

            elif variation.startswith("JES") and "AK4" in variation:
                # We need to convert the name of the variation
                # from JES_VariationUp to  up_jesVariation
                if variation.startswith("JES_Total") and variation[-2:] == "Up":
                    btag_jes_var = "up_jes"
                elif variation.startswith("JES_Total") and variation[-4:] == "Down":
                    btag_jes_var = "down_jes"
                else:
                    # we need to remove the possible jet type
                    variation = variation.replace("_AK4PFchs", "")
                    variation = variation.replace("_AK4PFPuppi", "")
                    if variation[-2:] == "Up":
                        btag_jes_var = f"up_jes{variation[4:-2]}"
                    elif variation[-4:] == "Down":
                        btag_jes_var = f"down_jes{variation[4:-4]}"
                # This is a special case where a dedicate btagSF is computed for up and down Jes shape variations.
                # This is not an up/down variation, but a single modified SF.
                # N.B: It is a central SF
                notc_mask = flavour != 4
                output["central"] = [_get_sf_variation_with_mask(btag_jes_var, notc_mask)]
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
    w = corr.evaluate(sample, ak.to_numpy(njets), ak.to_numpy(jetsHt))
    return w


def sf_ctag(params, jets, year, njets, variations=["central"]):
    '''
    Shape correction scale factors (SF) for DepJet charm tagger, taken from:
    https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/summaries/BTV_201X_UL_ctagging.html
    SFs are obtained per jet, then multiplied to obtain an overall weight per event.
    Note: this SF does not preserve the normalization of the MC samples!
     One has to re-normalize the MC samples using the sf_ctag_calib() method.
     The norm. calibration corrections are phase-space/analysis dependant.
     Therefore one has to derive them for each analysis separately.
    '''
    # print("Doing sf_ctag", year)

    ctagSF = params.jet_scale_factors.ctagSF[year]
    ctagger = params.ctagging.working_point[year]["tagger"]
    cset = correctionlib.CorrectionSet.from_file(ctagSF.SF_file)

    #print(list(cset.keys()))
    #print(list(cset.items()))

    corr = cset[ctagSF.name]

    #print(corr)

    flav = ak.to_numpy(ak.flatten(jets.hadronFlavour))
    CvL = ak.to_numpy(ak.flatten(jets.btagDeepCvL))
    CvB = ak.to_numpy(ak.flatten(jets.btagDeepCvB))

    central_SF_byjet = corr.evaluate("central", flav, CvL, CvB)

    #print(central_SF_byjet)
    #print("Unflatt=", ak.unflatten(central_SF_byjet, njets))
    #print("Prod:", ak.prod(ak.unflatten(central_SF_byjet, njets), axis=1))

    output = {}
    for variation in variations:
        if variation == "central":
            output[variation] = [ak.prod(ak.unflatten(central_SF_byjet, njets), axis=1)]
        else:
            # Nominal sf==1
            nominal = np.ones(ak.num(njets, axis=0))
            # Systematic variations
            up_variation_SF_byjet = corr.evaluate(f"up_{variation}", flav, CvL, CvB)
            down_variation_SF_byjet = corr.evaluate(f"down_{variation}", flav, CvL, CvB)

            output[variation] = [
                nominal,
                ak.prod(ak.unflatten(up_variation_SF_byjet,njets), axis=1),
                ak.prod(ak.unflatten(down_variation_SF_byjet,njets), axis=1)
            ]

    return output


def sf_ctag_calib(params, dataset, year, njets, jetsHt):
    '''
    These are correctiosn to normalization of every dataset after application of the ctag_sf shape correction.
    It was computed in V+2J selection by comparing the inclusive shape with and without ctagSF in 2D:
    in nJets-JetsHT bins. Each sample/year has a different correction stored in the correctionlib format.
    Note: the correction  file in parameters/ctagSF_calibrationSF.json is uesd by default here,
    which was  derived for V+2J phase space. It may not be suitable for other analyses.
    '''
    ctagSF = params.jet_scale_factors.ctagSF[year]
    cset = correctionlib.CorrectionSet.from_file(ctagSF.Calib_file)

    corr = cset["ctagSF_norm_correction"]
    w = corr.evaluate(dataset, ak.to_numpy(njets), ak.to_numpy(jetsHt))

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

def sf_partonshower_isr(events):
    '''Up and down variations for the ISR parton shower weights.
    In order to properly store the weights, a dummy weight of 1 is stored
    as central value for the ISR correction.
    Conventions for the PS weights are:
    [0] is ISR=2 FSR=1; [1] is ISR=1 FSR=2[2] is ISR=0.5 FSR=1; [3] is ISR=1 FSR=0.5;
    '''
    isr_up = events.PSWeight[:,2]
    isr_down = events.PSWeight[:,0]
    nom = ak.ones_like(isr_up)

    return nom, isr_up, isr_down

def sf_partonshower_fsr(events):
    '''Up and down variations for the FSR parton shower weights.
    In order to properly store the weights, a dummy weight of 1 is stored
    as central value for the FSR correction.
    Conventions for the PS weights are:
    [0] is ISR=2 FSR=1; [1] is ISR=1 FSR=2[2] is ISR=0.5 FSR=1; [3] is ISR=1 FSR=0.5;
    '''
    fsr_up = events.PSWeight[:,3]
    fsr_down = events.PSWeight[:,1]
    nom = ak.ones_like(fsr_up)

    return nom, fsr_up, fsr_down
