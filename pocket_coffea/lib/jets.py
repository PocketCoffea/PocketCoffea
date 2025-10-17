import copy
import importlib
import gzip
import cloudpickle

import awkward as ak
import numpy as np
import correctionlib
from coffea.jetmet_tools import  CorrectedMETFactory
from ..lib.deltaR_matching import get_matching_pairs_indices, object_matching
from correctionlib.schemav2 import Correction, CorrectionSet


def add_jec_variables(jets, event_rho, isMC=True):
    jets["pt_raw"] = (1 - jets.rawFactor) * jets.pt
    jets["mass_raw"] = (1 - jets.rawFactor) * jets.mass
    jets["event_rho"] = ak.broadcast_arrays(event_rho, jets.pt)[0]
    if isMC:
        jets["pt_gen"] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
    return jets

def load_jet_factory(params):
    #read the factory file from params and load it
    with gzip.open(params.jets_calibration.factory_file) as fin:
        try:
            return cloudpickle.load(fin)
        except Exception as e:
            print(f"Error loading the jet factory file: {params.jets_calibration.factory_file} --> Please remove the file and rerun the code")
            raise Exception(f"Error loading the jet factory file: {params.jets_calibration.factory_file} --> Please remove the file and rerun the code")
        

def jet_correction(params, events, jets, factory, jet_type, chunk_metadata, cache):
    if chunk_metadata["year"] in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
        rho = events.fixedGridRhoFastjetAll
    else:
        rho = events.Rho.fixedGridRhoFastjetAll

    # Note: PNet jet regression should be applied via PNetRegressionCalibrator
    # before calling this function, not within jet_correction itself.
    # The regression code has been moved to pocket_coffea.lib.calibrators.common.pnet_regression.PNetRegressionCalibrator
             
    if chunk_metadata["isMC"]:
        return factory["MC"][jet_type][chunk_metadata["year"]].build(
            add_jec_variables(jets, rho, isMC=True)
        )
    else:
        if chunk_metadata["era"] not in factory["Data"][jet_type][chunk_metadata["year"]]:
            raise Exception(f"Factory for {jet_type} in {chunk_metadata['year']} and era {chunk_metadata['era']} not found. Check your jet calibration files.")

        return factory["Data"][jet_type][chunk_metadata["year"]][chunk_metadata["era"]].build(
            add_jec_variables(jets, rho, isMC=False)
        )

def met_correction_after_jec(events, METcoll, jets_pre_jec, jets_post_jec):
    '''This function rescale the MET vector by minus delta of the jets after JEC correction
    and before the jEC correction.
    This can be used also to rescale the MET when updating on the fly the JEC calibration. '''
    orig_tot_px = ak.sum(jets_pre_jec.px, axis=1)
    orig_tot_py = ak.sum(jets_pre_jec.py, axis=1)
    new_tot_px = ak.sum(jets_post_jec.px, axis=1)
    new_tot_py = ak.sum(jets_post_jec.py, axis=1)
    newpx =  events[METcoll].px - (new_tot_px - orig_tot_px) 
    newpy =  events[METcoll].py - (new_tot_py - orig_tot_py) 
    
    newMetPhi = np.arctan2(newpy, newpx)
    newMetPt = (newpx**2 + newpy**2)**0.5
    return  {"pt": newMetPt, "phi": newMetPhi}


def met_correction(params, MET, jets):
    met_factory = CorrectedMETFactory(params.jet_calibration.jec_name_map) # to be fixed
    return met_factory.build(MET, jets, {})
    
def met_xy_correction(params, events, METcol,  year, era):
    '''Apply MET xy corrections to MET collection'''
    metx = events[METcol].pt * np.cos(events[METcol].phi)
    mety = events[METcol].pt * np.sin(events[METcol].phi)
    nPV = events.PV.npvs

    if era == "MC":
        params_ = params["MET_xy"]["MC"][year]
    else:
        params_ = params["MET_xy"]["Data"][year][era]

    metx = metx - (params_[0][0] * nPV + params_[0][1])
    mety = mety - (params_[1][0] * nPV + params_[1][1])
    pt_corr = np.hypot(metx, mety)
    phi_corr = np.arctan2(mety, metx)
    
    return pt_corr, phi_corr


def jet_correction_correctionlib(
    events, Jet, typeJet, year, JECversion, JERversion=None, verbose=False
):
    '''
    This function implements the Jet Energy corrections and Jet energy smearning
    using factors from correctionlib common-POG json file
    example here: https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/master/examples/jercExample.py

    '''
    jsonfile = JECjsonFiles[year][
        [t for t in ['AK4', 'AK8'] if typeJet.startswith(t)][0]
    ]
    JECfile = correctionlib.CorrectionSet.from_file(jsonfile)
    corr = JECfile.compound[f'{JECversion}_L1L2L3Res_{typeJet}']

    # until correctionlib handles jagged data natively we have to flatten and unflatten
    jets = events[Jet]
    jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
    jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
    jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
    j, nj = ak.flatten(jets), ak.num(jets)
    flatCorrFactor = corr.evaluate(
        np.array(j['area']),
        np.array(j['eta']),
        np.array(j['pt_raw']),
        np.array(j['rho']),
    )
    corrFactor = ak.unflatten(flatCorrFactor, nj)

    jets_corrected = copy.copy(jets)
    jets_corrected['pt'] = jets['pt_raw'] * corrFactor
    jets_corrected['mass'] = jets['mass_raw'] * corrFactor
    jets_corrected['rho'] = jets['rho']

    seed = events.event[0]

    if verbose:
        print()
        print(seed, 'JEC: starting columns:', ak.fields(jets), end='\n\n')

        print(seed, 'JEC: untransformed pt ratios', jets.pt / jets.pt_raw)
        print(seed, 'JEC: untransformed mass ratios', jets.mass / jets.mass_raw)

        print(
            seed, 'JEC: corrected pt ratios', jets_corrected.pt / jets_corrected.pt_raw
        )
        print(
            seed,
            'JEC: corrected mass ratios',
            jets_corrected.mass / jets_corrected.mass_raw,
        )

        print()
        print(seed, 'JEC: corrected columns:', ak.fields(jets_corrected), end='\n\n')

        # print('JES UP pt ratio',jets_corrected.JES_jes.up.pt/jets_corrected.pt_raw)
        # print('JES DOWN pt ratio',jets_corrected.JES_jes.down.pt/jets_corrected.pt_raw, end='\n\n')

    # Apply JER pt smearing (https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution)
    # The hybrid scaling method is implemented: if a jet is matched to a gen-jet, the scaling method is applied;
    # if a jet is not gen-matched, the stochastic smearing is applied.
    if JERversion:
        sf = JECfile[f'{JERversion}_ScaleFactor_{typeJet}']
        res = JECfile[f'{JERversion}_PtResolution_{typeJet}']
        j, nj = ak.flatten(jets_corrected), ak.num(jets_corrected)
        scaleFactor_flat = sf.evaluate(j['eta'].to_numpy(), 'nom')
        ptResolution_flat = res.evaluate(
            j['eta'].to_numpy(), j['pt'].to_numpy(), j['rho'].to_numpy()
        )
        scaleFactor = ak.unflatten(scaleFactor_flat, nj)
        ptResolution = ak.unflatten(ptResolution_flat, nj)
        # Match jets with gen-level jets, with DeltaR and DeltaPt requirements
        dr_min = {'AK4PFchs': 0.2, 'AK8PFPuppi': 0.4}[
            typeJet
        ]  # Match jets within a cone with half the jet radius
        pt_min = (
            3 * ptResolution * jets_corrected['pt']
        )  # Match jets whose pt does not differ more than 3 sigmas from the gen-level pt
        genJet = {'AK4PFchs': 'GenJet', 'AK8PFPuppi': 'GenJetAK8'}[typeJet]
        genJetIdx = {'AK4PFchs': 'genJetIdx', 'AK8PFPuppi': 'genJetAK8Idx'}[typeJet]

        # They can be matched manually
        # matched_genjets, matched_jets, deltaR_matched = object_matching(genjets, jets_corrected, dr_min, pt_min)
        # Or the association in NanoAOD it can be used, removing the indices that are not found. That happens because
        # not all the genJet are saved in the NanoAODs.
        genjets = events[genJet]
        Ngenjet = ak.num(genjets)
        matched_genjets_idx = ak.mask(
            jets_corrected[genJetIdx],
            (jets_corrected[genJetIdx] < Ngenjet) & (jets_corrected[genJetIdx] != -1),
        )
        # this array of indices has already the dimension of the Jet collection
        # in NanoAOD nomatch == -1 --> convert to None with a mask
        matched_objs_mask = ~ak.is_none(matched_genjets_idx, axis=1)
        matched_genjets = genjets[matched_genjets_idx]
        matched_jets = ak.mask(jets_corrected, matched_objs_mask)

        deltaPt = ak.unflatten(
            np.abs(ak.flatten(matched_jets.pt) - ak.flatten(matched_genjets.pt)),
            ak.num(matched_genjets),
        )
        matched_genjets = ak.mask(matched_genjets, deltaPt < pt_min)
        matched_jets = ak.mask(matched_jets, deltaPt < pt_min)

        # Compute energy correction factor with the scaling method
        detSmear = (
            1
            + (scaleFactor - 1)
            * (matched_jets['pt'] - matched_genjets['pt'])
            / matched_jets['pt']
        )
        # Compute energy correction factor with the stochastic method
        np.random.seed(seed)
        seed_dict = {}
        filename = events.metadata['filename']
        entrystart = events.metadata['entrystart']
        entrystop = events.metadata['entrystop']
        seed_dict[f'chunk_{filename}_{entrystart}-{entrystop}'] = seed
        rand_gaus = np.random.normal(
            np.zeros_like(ptResolution_flat), ptResolution_flat
        )
        jersmear = ak.unflatten(rand_gaus, nj)
        sqrt_arg_flat = scaleFactor_flat**2 - 1
        sqrt_arg_flat = ak.where(
            sqrt_arg_flat > 0, sqrt_arg_flat, ak.zeros_like(sqrt_arg_flat)
        )
        sqrt_arg = ak.unflatten(sqrt_arg_flat, nj)
        stochSmear = 1 + jersmear * np.sqrt(sqrt_arg)
        isMatched = ~ak.is_none(matched_jets.pt, axis=1)
        smearFactor = ak.where(isMatched, detSmear, stochSmear)

        jets_smeared = copy.copy(jets_corrected)
        jets_smeared['pt'] = jets_corrected['pt'] * smearFactor
        jets_smeared['mass'] = jets_corrected['mass'] * smearFactor

        if verbose:
            print()
            print(seed, "JER: isMatched", isMatched)
            print(seed, "JER: matched_jets.pt", matched_jets.pt)
            print(seed, "JER: smearFactor", smearFactor, end='\n\n')

            print(
                seed,
                'JER: corrected pt ratios',
                jets_corrected.pt / jets_corrected.pt_raw,
            )
            print(
                seed,
                'JER: corrected mass ratios',
                jets_corrected.mass / jets_corrected.mass_raw,
            )

            print(seed, 'JER: smeared pt ratios', jets_smeared.pt / jets_corrected.pt)
            print(
                seed,
                'JER: smeared mass ratios',
                jets_smeared.mass / jets_corrected.mass,
            )

            print()
            print(seed, 'JER: corrected columns:', ak.fields(jets_smeared), end='\n\n')

        return jets_smeared, seed_dict
    else:
        return jets_corrected


def jet_selection(events, jet_type, params, year, leptons_collection="", jet_tagger=""):

    jets = events[jet_type]
    cuts = params.object_preselection[jet_type]

    # For 2024 no jetId key in Nano anymore. 
    # For nanoV12 (i.e. 22/23), jet Id is also buggy, should therefore be rederived... To be done
    # See https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetID13p6TeV#nanoAOD_Flags
    jetIdCut = True
    if "jetId" in jets.fields:
        jetIdCut = (jets.jetId >= cuts["jetId"])
    else: print("No JetID cut applied, since key jetId missing in jet collections")

    # Mask for  jets not passing the preselection
    mask_presel = (
        (jets.pt > cuts["pt"])
        & (np.abs(jets.eta) < cuts["eta"])
        & jetIdCut
    )
    # Lepton cleaning
    # Only jets that are more distant than dr to ALL leptons are tagged as good jets
    if leptons_collection != "":
        dR_jets_lep = jets.metric_table(events[leptons_collection])
        mask_lepton_cleaning = ak.prod(dR_jets_lep > cuts["dr_lepton"], axis=2) == 1
    else:
        mask_lepton_cleaning = True

    if jet_type == "Jet":
        # Selection on PUid. Only available in Run2 UL, thus we need to determine which sample we run over;
        if year in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
            mask_jetpuid = (jets.puId >= params.jet_scale_factors.jet_puId[year]["working_point"][cuts["puId"]["wp"]]) | (
                jets.pt >= cuts["puId"]["maxpt"]
            )
        else:
            mask_jetpuid = True
  
        mask_good_jets = mask_presel & mask_lepton_cleaning & mask_jetpuid

        if jet_tagger != "":
            if "PNet" in jet_tagger:
                B   = "btagPNetB"
                CvL = "btagPNetCvL"
                CvB = "btagPNetCvB"
            elif "DeepFlav" in jet_tagger:
                B   = "btagDeepFlavB"
                CvL = "btagDeepFlavCvL"
                CvB = "btagDeepFlavCvB"
            elif "RobustParT" in jet_tagger:
                B   = "btagRobustParTAK4B"
                CvL = "btagRobustParTAK4CvL"
                CvB = "btagRobustParTAK4CvB"
            else:
                raise NotImplementedError(f"This tagger is not implemented: {jet_tagger}")
            
            if B not in jets.fields or CvL not in jets.fields or CvB not in jets.fields:
                raise NotImplementedError(f"{B}, {CvL}, and/or {CvB} are not available in the input.")

            jets["btagB"] = jets[B]
            jets["btagCvL"] = jets[CvL]
            jets["btagCvB"] = jets[CvB]

    elif jet_type == "FatJet":
        # Apply the msd and preselection cuts
        mask_msd = events.FatJet.msoftdrop > cuts["msd"]
        mask_good_jets = mask_presel & mask_msd & mask_lepton_cleaning

        if jet_tagger != "":
            if "PNetMD" in jet_tagger:
                BB   = "particleNet_XbbVsQCD"
                CC   = "particleNet_XccVsQCD"
            elif "PNet" in jet_tagger:
                BB   = "particleNetWithMass_HbbvsQCD"
                CC   = "particleNetWithMass_HccvsQCD"
            else:
                raise NotImplementedError(f"This tagger is not implemented: {jet_tagger}")
            
            if BB not in jets.fields or CC not in jets.fields:
                raise NotImplementedError(f"{BB} and/or {CC} are not available in the input.")

            jets["btagBB"] = jets[BB]
            jets["btagCC"] = jets[CC]

    return jets[mask_good_jets], mask_good_jets


def jet_selection_nanoaodv12(events, jet_type, params, year, leptons_collection="", jet_tagger=""):

    jets = events[jet_type]
    cuts = params.object_preselection[jet_type]

    # Bug fix for NanoAOD v12 https://gitlab.cern.ch/cms-jetmet/coordination/coordination/-/issues/117
    
    mask_presel = (
        (jets.pt > cuts["pt"])
        & (np.abs(jets.eta) < cuts["eta"])
    )
        
    if cuts["jetId"] == 6:
        passJetIdTight = ak.zeros_like(jets.pt, dtype=bool)
        passJetIdTightLepVeto = ak.zeros_like(jets.pt, dtype=bool)
        
        passJetIdTight = ak.where((np.abs(jets.eta) <= 2.7), (jets.jetId & (1 << 1))>0, passJetIdTight)
        passJetIdTight = ak.where((np.abs(jets.eta) > 2.7) & (np.abs(jets.eta) <= 3.0), ((jets.jetId & (1 << 1))>0 & (jets.neHEF < 0.99)), passJetIdTight)
        passJetIdTight = ak.where((np.abs(jets.eta) > 3.0), ((jets.jetId & (1 << 1))>0 & (jets.neEmEF < 0.4)), passJetIdTight)
        passJetIdTightLepVeto = ak.where((np.abs(jets.eta) <= 2.7), (passJetIdTight & (jets.muEF < 0.8) & (jets.chEmEF < 0.8)),passJetIdTight)
    
    # Only jets that are more distant than dr to ALL leptons are tagged as good jets
    # Mask for  jets not passing the preselection
    
    # Lepton cleaning
    if leptons_collection != "":
        dR_jets_lep = jets.metric_table(events[leptons_collection])
        mask_lepton_cleaning = ak.prod(dR_jets_lep > cuts["dr_lepton"], axis=2) == 1
    else:
        mask_lepton_cleaning = True

    if jet_type == "Jet":
        # Selection on PUid. Only available in Run2 UL, thus we need to determine which sample we run over;
        if year in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
            mask_jetpuid = (jets.puId >= params.jet_scale_factors.jet_puId[year]["working_point"][cuts["puId"]["wp"]]) | (
                jets.pt >= cuts["puId"]["maxpt"]
            )
        else:
            mask_jetpuid = True        
        
        if cuts["jetId"] == 6:
            mask_good_jets = mask_presel & mask_lepton_cleaning & mask_jetpuid & passJetIdTightLepVeto
        else:
            mask_good_jets = mask_presel & mask_lepton_cleaning & mask_jetpuid 

        if jet_tagger != "":
            if "PNet" in jet_tagger:
                B   = "btagPNetB"
                CvL = "btagPNetCvL"
                CvB = "btagPNetCvB"
            elif "DeepFlav" in jet_tagger:
                B   = "btagDeepFlavB"
                CvL = "btagDeepFlavCvL"
                CvB = "btagDeepFlavCvB"
            elif "RobustParT" in jet_tagger:
                B   = "btagRobustParTAK4B"
                CvL = "btagRobustParTAK4CvL"
                CvB = "btagRobustParTAK4CvB"
            else:
                raise NotImplementedError(f"This tagger is not implemented: {jet_tagger}")
            
            if B not in jets.fields or CvL not in jets.fields or CvB not in jets.fields:
                raise NotImplementedError(f"{B}, {CvL}, and/or {CvB} are not available in the input.")

            jets["btagB"] = jets[B]
            jets["btagCvL"] = jets[CvL]
            jets["btagCvB"] = jets[CvB]

    elif jet_type == "FatJet":
        # Apply the msd and preselection cuts
        mask_msd = events.FatJet.msoftdrop > cuts["msd"]
        mask_good_jets = mask_presel & mask_msd

        if jet_tagger != "":
            if "PNetMD" in jet_tagger:
                BB   = "particleNet_XbbVsQCD"
                CC   = "particleNet_XccVsQCD"
            elif "PNet" in jet_tagger:
                BB   = "particleNetWithMass_HbbvsQCD"
                CC   = "particleNetWithMass_HccvsQCD"
            else:
                raise NotImplementedError(f"This tagger is not implemented: {jet_tagger}")
            
            if BB not in jets.fields or CC not in jets.fields:
                raise NotImplementedError(f"{BB} and/or {CC} are not available in the input.")

            jets["btagBB"] = jets[BB]
            jets["btagCC"] = jets[CC]

    return jets[mask_good_jets], mask_good_jets

def btagging(Jet, btag, wp, veto=False):
    if veto:
        return Jet[Jet[btag["btagging_algorithm"]] < btag["btagging_WP"][wp]]
    else:
        return Jet[Jet[btag["btagging_algorithm"]] > btag["btagging_WP"][wp]]


def CvsLsorted(jets,temp=None):    
    if temp is not None:
        raise NotImplementedError(f"Using the tagger name while calling `CvsLsorted` is deprecated. Please use `jet_tagger={temp}` as an argument to `jet_selection`.")
    return jets[ak.argsort(jets["btagCvL"], axis=1, ascending=False)]

def ProbBsorted(jets,temp=None):    
    if temp is not None:
        raise NotImplementedError(f"Using the tagger name while calling `ProbBsorted` is deprecated. Please use `jet_tagger={temp}` as an argument to `jet_selection`.")
    return jets[ak.argsort(jets["btagB"], axis=1, ascending=False)]


def get_dijet(jets, taggerVars=True, remnant_jet = False):
    if isinstance(taggerVars,str):
        raise NotImplementedError(f"Using the tagger name while calling `get_dijet` is deprecated. Please use `jet_tagger={taggerVars}` as an argument to `jet_selection`.")
    
    fields = {
        "pt": 0.,
        "eta": 0.,
        "phi": 0.,
        "mass": 0.,
    }
    
    if remnant_jet:
        fields_remnant = {
        "pt": 0.,
        "eta": 0.,
        "phi": 0.,
        "mass": 0.,
    }

    jets = ak.pad_none(jets, 2)
    njet = ak.num(jets[~ak.is_none(jets, axis=1)])
    
    dijet = jets[:, 0] + jets[:, 1]
    if remnant_jet:
        remnant = jets[:, 2:]

    for var in fields.keys():
        fields[var] = ak.where(
            (njet >= 2),
            getattr(dijet, var),
            fields[var]
        )
        
    if remnant_jet:
        for var in fields_remnant.keys():
            fields_remnant[var] = ak.where(
                (njet > 2),
                ak.sum(getattr(remnant, var), axis=1),
                fields_remnant[var]
            )

    fields["deltaR"] = ak.where( (njet >= 2), jets[:,0].delta_r(jets[:,1]), -1)
    fields["deltaPhi"] = ak.where( (njet >= 2), abs(jets[:,0].delta_phi(jets[:,1])), -1)
    fields["deltaEta"] = ak.where( (njet >= 2), abs(jets[:,0].eta - jets[:,1].eta), -1)
    fields["j1Phi"] = ak.where( (njet >= 2), jets[:,0].phi, -1)
    fields["j2Phi"] = ak.where( (njet >= 2), jets[:,1].phi, -1)
    fields["j1pt"] = ak.where( (njet >= 2), jets[:,0].pt, -1)
    fields["j2pt"] = ak.where( (njet >= 2), jets[:,1].pt, -1)
    fields["j1eta"] = ak.where( (njet >= 2), jets[:,0].eta, -1)
    fields["j2eta"] = ak.where( (njet >= 2), jets[:,1].eta, -1)
    fields["j1mass"] = ak.where( (njet >= 2), jets[:,0].mass, -1)
    fields["j2mass"] = ak.where( (njet >= 2), jets[:,1].mass, -1)


    if "jetId" in jets.fields and taggerVars:
        '''This dijet fuction should work for GenJets as well. But the btags are not available for them
        Thus, one has to check if a Jet is a GenJet or reco Jet. The jetId variable is only available in reco Jets'''
        fields["j1CvsL"] = ak.where( (njet >= 2), jets[:,0]["btagCvL"], -1)
        fields["j2CvsL"] = ak.where( (njet >= 2), jets[:,1]["btagCvL"], -1)
        fields["j1CvsB"] = ak.where( (njet >= 2), jets[:,0]["btagCvB"], -1)
        fields["j2CvsB"] = ak.where( (njet >= 2), jets[:,1]["btagCvB"], -1)
    
    dijet = ak.zip(fields, with_name="PtEtaPhiMCandidate")
    if remnant_jet:
        remnant = ak.zip(fields_remnant, with_name="PtEtaPhiMCandidate")

    if not remnant_jet:
        return dijet
    else:
        return dijet, remnant


def get_jer_correction_set(jer_json, jer_ptres_tag, jer_sf_tag):
    # learned from: https://github.com/cms-nanoAOD/correctionlib/issues/130
    with gzip.open(jer_json) as fin:
        cset = CorrectionSet.parse_raw(fin.read())

    cset.corrections = [
        c
        for c in cset.corrections
        if c.name
        in (
            jer_ptres_tag,
            jer_sf_tag,
        )
    ]
    cset.compound_corrections = []

    res = Correction.parse_obj(
        {
            "name": "JERSmear",
            "description": "Jet smearing tool",
            "inputs": [
                {"name": "JetPt", "type": "real"},
                {"name": "JetEta", "type": "real"},
                {
                    "name": "GenPt",
                    "type": "real",
                    "description": "matched GenJet pt, or -1 if no match",
                },
                {"name": "Rho", "type": "real", "description": "entropy source"},
                {"name": "EventID", "type": "int", "description": "entropy source"},
                {
                    "name": "JER",
                    "type": "real",
                    "description": "Jet energy resolution",
                },
                {
                    "name": "JERsf",
                    "type": "real",
                    "description": "Jet energy resolution scale factor",
                },
            ],
            "output": {"name": "smear", "type": "real"},
            "version": 1,
            "data": {
                "nodetype": "binning",
                "input": "GenPt",
                "edges": [-1, 0, 1],
                "flow": "clamp",
                "content": [
                    # stochastic
                    {
                        # rewrite gen_pt with a random gaussian
                        "nodetype": "transform",
                        "input": "GenPt",
                        "rule": {
                            "nodetype": "hashprng",
                            "inputs": ["JetPt", "JetEta", "Rho", "EventID"],
                            "distribution": "normal",
                        },
                        "content": {
                            "nodetype": "formula",
                            # TODO min jet pt?
                            "expression": "1+sqrt(max(x*x - 1, 0)) * y * z",
                            "parser": "TFormula",
                            # now gen_pt is actually the output of hashprng
                            "variables": ["JERsf", "JER", "GenPt"],
                        },
                    },
                    # deterministic
                    {
                        "nodetype": "formula",
                        # TODO min jet pt?
                        "expression": "1+(x-1)*(y-z)/y",
                        "parser": "TFormula",
                        "variables": ["JERsf", "JetPt", "GenPt"],
                    },
                ],
            },
        }
    )
    cset.corrections.append(res)
    ceval = cset.to_evaluator()
    return ceval

def get_jersmear(_eval_dict, _ceval, _jer_sf_tag, _syst="nom"):
    _eval_dict.update({"systematic": _syst})
    _inputs_jer_sf = [_eval_dict[input.name] for input in _ceval[_jer_sf_tag].inputs]
    _jer_sf = _ceval[_jer_sf_tag].evaluate(*_inputs_jer_sf)
    _eval_dict.update({"JERsf": _jer_sf})
    _inputs = [_eval_dict[input.name] for input in _ceval["JERSmear"].inputs]
    _jersmear = _ceval["JERSmear"].evaluate(*_inputs)
    return _eval_dict, _jersmear


def jet_correction_corrlib(
    params,
    events,
    jet_type,
    chunk_metadata,
    level="L1L2L3Res",
    jet_coll_name="Jet",
    jec_syst=True,
):
    isMC = chunk_metadata["isMC"]
    year = chunk_metadata["year"]
    era = chunk_metadata["era"]
    jec_clib_dict = params["jets_calibration_clib"]
    variations = params.jets_calibration.variations[year][jet_type]

    json_path = jec_clib_dict[year][jet_type]["json_path"]
    jer_tag = None
    if isMC:
        jec_tag = jec_clib_dict[year][jet_type]['jec_mc'] 
        jer_tag = jec_clib_dict[year][jet_type]['jer']
    else:
        if type(jec_clib_dict[year][jet_type]['jec_data'])==str:
            jec_tag = jec_clib_dict[year][jet_type]['jec_data']
        else:
            jec_tag = jec_clib_dict[year][jet_type]['jec_data'][chunk_metadata["era"]]

    # no jer and variations applied on data
    apply_jes = True
    apply_jer = jes_syst = jer_syst = False
    if isMC:
        apply_jer=True
        if jec_syst:
            jes_syst = True
            if "JER" in variations:
                jer_syst = True

    tag_jec = "_".join([jec_tag, level, jet_type])

    # get the correction sets
    cset = correctionlib.CorrectionSet.from_file(json_path)

    # prepare inputs
    jets_jagged = events[jet_coll_name]
    counts = ak.num(jets_jagged)

    if ("event_id" not in jets_jagged.fields) and (apply_jer or jer_syst):
        jets_jagged["event_id"] = ak.ones_like(jets_jagged.pt) * events.event
    if ("run_nr" not in jets_jagged.fields):
        jets_jagged["run_nr"] = ak.ones_like(jets_jagged.pt) * events.run
    if year in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
        rho = events.fixedGridRhoFastjetAll
    else:
        rho = events.Rho.fixedGridRhoFastjetAll
    jets_jagged = add_jec_variables(jets_jagged, rho, isMC)

    # flatten

    jets = ak.flatten(jets_jagged)
    # evaluate dictionary
    eval_dict = {
        "JetPt": jets.pt_raw,
        "JetEta": jets.eta,
        "JetPhi": jets.phi,
        "Rho": jets.event_rho,
        "JetA": jets.area,
        "run": jets.run_nr
    }

    # jes central
    if apply_jes:
        # get the correction
        if tag_jec in list(cset.compound.keys()):
            sf = cset.compound[tag_jec]
        elif tag_jec in list(cset.keys()):
            sf = cset[tag_jec]
        else:
            print(tag_jec, list(cset.keys()), list(cset.compound.keys()))
            raise Exception(f"[No JEC correction: {tag_jec} - Year: {year} - Era: {era} - Level: {level}")
        inputs = [eval_dict[input.name] for input in sf.inputs]
        sf_value = sf.evaluate(*inputs)
        # update the nominal pt and mass
        jets["pt"] = sf_value * jets["pt_raw"]
        jets["mass"] = sf_value * jets["mass_raw"]

    # jer central and systematics
    if apply_jer or jer_syst:
        # learned from: https://github.com/cms-nanoAOD/correctionlib/issues/130

        jer_ptres_tag = f"{jer_tag}_PtResolution_{jet_type}"
        jer_sf_tag = f"{jer_tag}_ScaleFactor_{jet_type}"

        ceval_jer = get_jer_correction_set(json_path, jer_ptres_tag, jer_sf_tag)
        # update evaluate dictionary
        eval_dict.update(
            {
                "JetPt": jets.pt,
                "GenPt": jets.pt_gen,
                "EventID": jets.event_id,
            }
        )
        # get jer pt resolution
        inputs_jer_ptres = [
            eval_dict[input.name] for input in ceval_jer[jer_ptres_tag].inputs
        ]
        jer_ptres = ceval_jer[jer_ptres_tag].evaluate(*inputs_jer_ptres)
        # update evaluate dictionary
        eval_dict.update({"JER": jer_ptres})
        # adjust pt gen
        eval_dict.update(
            {
                "GenPt": np.where(
                    np.abs(eval_dict["JetPt"] - eval_dict["GenPt"])
                    < 3 * eval_dict["JetPt"] * eval_dict["JER"],
                    eval_dict["GenPt"],
                    -1.0,
                ),
            }
        )
        if apply_jer:
            eval_dict, jersmear = get_jersmear(eval_dict, ceval_jer, jer_sf_tag, "nom")
            jets["pt_jer"] = jets.pt * jersmear
            jets["mass_jer"] = jets.mass * jersmear
        if jer_syst:
            # jer up
            eval_dict, jersmear = get_jersmear(eval_dict, ceval_jer, jer_sf_tag, "up")
            jets["pt_JER_up"] = jets.pt * jersmear
            jets["mass_JER_up"] = jets.mass * jersmear
            # jer down
            eval_dict, jersmear = get_jersmear(eval_dict, ceval_jer, jer_sf_tag, "down")
            jets["pt_JER_down"] = jets.pt * jersmear
            jets["mass_JER_down"] = jets.mass * jersmear
        if apply_jer:
            # to avoid the sf: jer*jer_up or jer*jer_down, update the jer pt/mass after calculation of the jer up/down
            jets["pt"] = jets["pt_jer"]
            jets["mass"] = jets["mass_jer"]

    # jes systematics
    if jes_syst:
        # update evaluate dictionary
        eval_dict.update({"JetPt": jets.pt})
        # loop over all JES variations
        jes_strings = [s[4:] for s in variations if s.startswith("JES")]
        for jes_vari in jes_strings:
            # If Regrouped variations are wanted, the Regrouped_ name must be used in the config
            tag_jec_syst = "_".join([jec_tag, jes_vari, jet_type])
            try:
                sf = cset[tag_jec_syst]
            except:
                raise Exception(
                    f"[ jerc_jet ] No JEC systematic: {tag_jec_syst} - Year: {year} - Era: {era}"
                )
            # systematics
            inputs = [eval_dict[input.name] for input in sf.inputs]
            sf_delta = sf.evaluate(*inputs)

            # divide by correction since it is already applied before
            corr_up_variation = 1 + sf_delta
            corr_down_variation = 1 - sf_delta

            jets[f"pt_JES_{jes_vari}_up"] = jets.pt * corr_up_variation
            jets[f"pt_JES_{jes_vari}_down"] = jets.pt * corr_down_variation
            jets[f"mass_JES_{jes_vari}_up"] = jets.mass * corr_up_variation
            jets[f"mass_JES_{jes_vari}_down"] = jets.mass * corr_down_variation
    jets_jagged = ak.unflatten(jets, counts)
    return jets_jagged
