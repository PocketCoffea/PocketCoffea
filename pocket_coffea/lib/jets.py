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
        try:
            jets["pt_gen"] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
        except AttributeError:
            jets["pt_gen"] = ak.zeros_like(jets.pt, dtype=np.float32)
    return jets


def load_jet_factory(params):
    #read the factory file from params and load it
    with gzip.open(params.jets_calibration.factory_file) as fin:
        try:
            return cloudpickle.load(fin)
        except Exception as e:
            print(f"Error loading the jet factory file: {params.jets_calibration.factory_file} --> Please remove the file and rerun the code")
            raise Exception(f"Error loading the jet factory file: {params.jets_calibration.factory_file} --> Please remove the file and rerun the code")
        
        
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


def jet_selection(events, jet_type, params, year, leptons_collection="", jet_tagger=""):
    jets = events[jet_type]
    cuts = params.object_preselection[jet_type]

    # For nanoV15 no jetId key in Nano anymore. 
    # For nanoV12 (i.e. 22/23), jet Id is also buggy, should therefore be rederived
    # in the following, if nano_version not explicitly specified in params, v9 is assumed for Run2UL, v12 for 22/23 and v15 for 2024
    jets["jetId_corrected"] = compute_jetId(events, jet_type, params, year)

    # Mask for  jets not passing the preselection
    mask_presel = (
        (jets.pt > cuts["pt"])
        & (np.abs(jets.eta) < cuts["eta"])
        & (jets.jetId_corrected >= cuts["jetId"])
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

def compute_jetId(events, jet_type, params, year):
    """
    Add (or recompute) jet ID to the jets object based on the NanoAOD version.
    Inspired by https://gitlab.cern.ch/cms-analysis/general/HiggsDNA/-/blob/master/higgs_dna/tools/jetID.py
    """
    jets = events[jet_type]
    try:
        if nano_version := events.metadata.get("nano_version", None) is None:
            nano_version = params["default_nano_version"][year]
    except:
        raise Exception("Please specify the `nano_version` in the file metadata or the default nano version in the parameters under `default_nano_version` key.")
    
    abs_eta = abs(jets.eta)
    # Return the existing jetId for NanoAOD versions below 12
    if nano_version < 12:
        return jets.jetId

    # For NanoAOD version 12 and above, we recompute the jet ID criteria
    # https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetID13p6TeV
    elif nano_version == 12:
        # Default tight
        passJetIdTight = ak.where(
            abs_eta <= 2.7,
            (jets.jetId & (1 << 1)) > 0,  # Tight criteria for abs_eta <= 2.7
            ak.where(
                (abs_eta > 2.7) & (abs_eta <= 3.0),
                ((jets.jetId & (1 << 1)) > 0) & (jets.neHEF < 0.99),  # Tight criteria for 2.7 < abs_eta <= 3.0
                ((jets.jetId & (1 << 1)) > 0) & (jets.neEmEF < 0.4)  # Tight criteria for 3.0 < abs_eta
            )
        )

        # Default tight lepton veto
        passJetIdTightLepVeto = ak.where(
            abs_eta <= 2.7,
            passJetIdTight & (jets.muEF < 0.8) & (jets.chEmEF < 0.8),  # add lepton veto for abs_eta <= 2.7
            passJetIdTight  # No lepton veto for 2.7 < abs_eta
        )
        return (passJetIdTight * (1 << 1)) | (passJetIdTightLepVeto * (1 << 2))


    elif nano_version >= 15:
        # Example code: https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/master/examples/jetidExample.py?ref_type=heads
        # Load CorrectionSet
        jsonFile = params.jet_scale_factors.jet_id[year]
        cset = correctionlib.CorrectionSet.from_file(jsonFile)

        counts = ak.num(jets)
        jets = ak.flatten(jets, axis=1)

        eval_dict = {
            "eta": jets.eta,
            "chHEF": jets.chHEF,
            "neHEF": jets.neHEF,
            "chEmEF": jets.chEmEF,
            "neEmEF": jets.neEmEF,
            "muEF": jets.muEF,
            "chMultiplicity": jets.chMultiplicity,
            "neMultiplicity": jets.neMultiplicity,
            "multiplicity": jets.chMultiplicity + jets.neMultiplicity
        }

        ## Default tight for NanoAOD version 13 and above
        jet_algo_mapping = params.jets_calibration.collection[year]
        jet_algo = next((k for k, v in jet_algo_mapping.items() if v == jet_type), None)
        if jet_algo==None:
            raise Exception(f"No mapping jet_type ({jet_type}) -> jet_algo (e.g. AK4PFPuppi) defined for year {year}")
        jet_algo = jet_algo.replace("PF", "").upper()
    
        if jet_algo+"_Tight" not in list(cset.keys()):
            raise Exception(f"No correction for jet collection {jet_algo} defined in correctionlib file {jsonFile}")
        idTight = cset[jet_algo+"_Tight"]
        inputsTight = [eval_dict[input.name] for input in idTight.inputs]
        idTight_value = idTight.evaluate(*inputsTight) * 2  # equivalent to bit2

        # Default tight lepton veto
        idTightLepVeto = cset[jet_algo+"_TightLeptonVeto"]
        inputsTightLepVeto = [eval_dict[input.name] for input in idTightLepVeto.inputs]
        idTightLepVeto_value = idTightLepVeto.evaluate(*inputsTightLepVeto) * 4  # equivalent to bit3

        # Default jet ID
        id_value = idTight_value + idTightLepVeto_value

        return ak.unflatten(id_value, counts)


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
    calib_params,
    variations,
    events,
    jet_type,
    jet_coll_name,
    chunk_metadata,
    apply_jer=True,
    jec_syst=True,
):
    isMC = chunk_metadata["isMC"]
    year = chunk_metadata["year"]
    era = chunk_metadata["era"]

    json_path = calib_params["json_path"]
    jer_tag = None
    if isMC:
        jec_tag = calib_params['jec_mc'] 
        jer_tag = calib_params['jer']
    else:
        if type(calib_params['jec_data'])==str:
            jec_tag = calib_params['jec_data']
        else:
            jec_tag = calib_params['jec_data'][chunk_metadata["era"]]
    
    level = calib_params['level']  # e.g. 'L1L2L3' for MC, 'L1L2L3Residual' for data

    # no jer and variations applied on data
    apply_jes = True
    jes_syst = jer_syst = False
    if isMC:
        if jec_syst:
            jes_syst = True
            if "JER" in variations:
                jer_syst = True
    else:
        apply_jer = False

    tag_jec = "_".join([jec_tag, level, jet_type])

    # get the correction sets
    cset = correctionlib.CorrectionSet.from_file(json_path)

    # prepare inputs
    # no need of copies
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
