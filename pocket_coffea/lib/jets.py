import gzip
import cloudpickle
import awkward as ak
import numpy as np
import correctionlib
from coffea.jetmet_tools import  CorrectedMETFactory
from correctionlib.schemav2 import Correction, CorrectionSet
from ..utils.utils import get_nano_version, replace_at_indices


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

def add_jec_variables_subjet(jets, event_rho, isMC=True):
    jets["pt_raw"] = (1 - jets.rawFactor) * jets.pt
    jets["mass_raw"] = (1 - jets.rawFactor) * jets.mass
    jets["event_rho"] = ak.broadcast_arrays(event_rho, jets.pt)[0]

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
            elif "UParT" in jet_tagger:
                B   = "btagUParTAK4B"
                CvL = "btagUParTAK4CvL"
                CvB = "btagUParTAK4CvB"
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
    # Get the nano version from events metadata or from default parameters
    nano_version = get_nano_version(events, params, year)
    abs_eta = abs(jets.eta)
    # Return the existing jetId for NanoAOD versions below 12
    if nano_version < 12:
        return jets.jetId

    # For NanoAOD version 12 and above, we recompute the jet ID criteria
    # https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetID13p6TeV
    elif nano_version == 12:
        if jet_type == "Jet":
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

        elif jet_type == "FatJet":
            # For nanoAOD v12, only using the original branch in the tracker acceptance
            passJetIdTight = ak.where(
                abs_eta <= 2.7,
                (jets.jetId & (1 << 1)) > 0,  # Tight criteria for abs_eta <= 2.7
                ak.zeros_like(jets.jetId) 
                )
            # Not tight lepveto for FatJet
            return (passJetIdTight * (1 << 1)) 
        else:
            raise ValueError(f"Jet type {jet_type} not recognized for JetID")


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


    if taggerVars:
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




def msoftdrop_correction(
    calib_params,
    variations,
    events,
    subjet_type,
    jet_coll_name,
    chunk_metadata,
    jec_syst=True,
):
    """Apply softdrop mass correction to large-radius jets (FatJet) using correctionlib.
    The correction consists in applying AK4 jet corrections to the subjets of the large-radius jets.
    The corrected softdrop mass is computed as the invariant mass of the two corrected subjets.
    Only the JES correction and variations are applied, while the JER correction is not applied to the subjets."""

    assert jet_coll_name in ["FatJet"], f"{jet_coll_name} collection cannot be calibrated. The softdrop mass correction can be applied only on large-radius jets."
    assert subjet_type in ["AK4PFchs", "AK4PFPuppi"], f"{subjet_type} subjet type cannot be calibrated. The subjets can be calibrated only using corrections of type AK4PFchs or AK4PFPuppi."
    isMC = chunk_metadata["isMC"]
    year = chunk_metadata["year"]
    era = chunk_metadata["era"]

    json_path = calib_params["json_path"]
    if isMC:
        jec_tag = calib_params['jec_mc'] 
    else:
        if type(calib_params['jec_data'])==str:
            jec_tag = calib_params['jec_data']
        else:
            jec_tag = calib_params['jec_data'][chunk_metadata["era"]]
    
    level = calib_params['level']  # e.g. 'L1L2L3' for MC, 'L1L2L3Residual' for data

    # no jer and variations applied on data
    apply_jes = True
    jes_syst = False
    if isMC:
        if jec_syst:
            jes_syst = True

    tag_jec = "_".join([jec_tag, level, subjet_type])

    # get the correction sets
    cset = correctionlib.CorrectionSet.from_file(json_path)

    # prepare inputs
    jets_jagged = events[jet_coll_name]
    counts = ak.num(jets_jagged)

    # Create new branch for jets to store the event and run number
    jets_jagged["event_id"] = ak.ones_like(jets_jagged.pt) * events.event
    jets_jagged["run_nr"] = ak.ones_like(jets_jagged.pt) * events.run

    if year in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
        rho = events.fixedGridRhoFastjetAll
    else:
        rho = events.Rho.fixedGridRhoFastjetAll
    
    jets_jagged["rho"] = ak.ones_like(jets_jagged.pt) * rho

    # Early return if no jets in any event
    if ak.sum(counts) == 0:
        return jets_jagged
    
    # Create mask for events that have jets
    events_have_jets = counts > 0
    
    # Only process events that have jets
    if not ak.any(events_have_jets):
        return jets_jagged
    
    # Check which jets have subjets and msoftdrop > 0: this are the only jets that will be corrected
    has_subjets_per_jet = (ak.count(jets_jagged.subjets.pt, axis=-1) > 0) & (jets_jagged.msoftdrop > 0)

    # Check which events have at least one jet with subjets
    events_have_jets_with_subjets = ak.any(has_subjets_per_jet, axis=1) & events_have_jets
    
    if not ak.any(events_have_jets_with_subjets):
        return jets_jagged
    
    # Only process subjets from jets that actually have them
    # Use ak.mask to filter out jets without subjets, avoiding None values
    jets_with_subjets = ak.mask(jets_jagged, has_subjets_per_jet)
    
    # Get only the valid (non-None) jets for processing
    valid_jets = jets_with_subjets[~ak.is_none(jets_with_subjets, axis=1)]
    valid_indices = ak.local_index(jets_with_subjets, axis=1)[~ak.is_none(jets_with_subjets, axis=1)]

    # Only proceed if there are valid jets with subjets
    if ak.sum(ak.num(valid_jets)) == 0:
        return jets_jagged
    
    # flatten jet collection (only jets with subjets)
    jets_flat = ak.flatten(valid_jets)
    counts_valid = ak.num(valid_jets)
    
    # get subjets from valid jets only
    subjets_jagged = jets_flat.subjets
    # mask None subjets (in case some jets have no subjets)
    subjets_jagged = subjets_jagged[~ak.is_none(subjets_jagged, axis=1)]
    # get counts before flattening subjets
    counts_subjet = ak.num(subjets_jagged)

    # Create new branches for subjets to store the event and run number and the event rho
    # and broadcast event variables to subjets
    event_variables = ["event_id", "run_nr", "rho"]
    for var in event_variables:
        if var in jets_flat.fields:
            subjets_jagged[var] = ak.ones_like(subjets_jagged.pt) * jets_flat[var]

    subjets_jagged = add_jec_variables_subjet(subjets_jagged, subjets_jagged["rho"], isMC)

    # flatten subjet collection
    subjets = ak.flatten(subjets_jagged)

    # evaluate dictionary
    # The jet area is needed only for L1 corrections, but for PUPPI jets this correction is not derived
    # It is safe to apply L1L2L3 corrections by passing an average area of 0.5 (= π × 0.4 × 0.4) for all jets as input.
    eval_dict = {
        "JetPt": subjets.pt_raw,
        "JetEta": subjets.eta,
        "JetPhi": subjets.phi,
        "Rho": subjets.event_rho,
        "JetA": 0.5 * ak.ones_like(subjets.pt_raw), # Get "dummy" area for subjets
        "run": subjets.run_nr
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
        subjets["pt"] = sf_value * subjets["pt_raw"]
        subjets["mass"] = sf_value * subjets["mass_raw"]

    # jes systematics
    if jes_syst:
        # update evaluate dictionary
        eval_dict.update({"JetPt": subjets.pt})
        # loop over all JES variations
        jes_strings = [s[4:] for s in variations if s.startswith("JES")]
        for jes_vari in jes_strings:
            # If Regrouped variations are wanted, the Regrouped_ name must be used in the config
            tag_jec_syst = "_".join([jec_tag, jes_vari, subjet_type])
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

            subjets[f"pt_JES_{jes_vari}_up"] = subjets.pt * corr_up_variation
            subjets[f"pt_JES_{jes_vari}_down"] = subjets.pt * corr_down_variation
            subjets[f"mass_JES_{jes_vari}_up"] = subjets.mass * corr_up_variation
            subjets[f"mass_JES_{jes_vari}_down"] = subjets.mass * corr_down_variation
    
    # Reconstruct subjets back to jagged array
    subjets_jagged_corrected = ak.unflatten(subjets, counts_subjet)
    
    # Compute corrected softdrop mass from corrected subjets
    # This has the shape of valid_jets (jets with subjets)
    corrected_msoftdrop_for_valid_jets = subjets_jagged_corrected.sum(axis=-1).mass
    
    # Unflatten corrected msoftdrop to match the valid_jets structure
    corrected_msoftdrop_for_valid_jets = ak.unflatten(corrected_msoftdrop_for_valid_jets, counts_valid)
    
    # Now we need to map this back to the original jets_jagged structure
    # Create a masked array with the same structure as jets_jagged but with corrected values
    # where jets have subjets, and None elsewhere
    corrected_msoftdrop_masked = ak.copy(jets_with_subjets.msoftdrop)  # This has None for jets without subjets

    corrected_msoftdrop_masked = replace_at_indices(
        ak.Array(corrected_msoftdrop_masked, behavior={}),
        ak.Array(valid_indices, behavior={}),
        ak.Array(corrected_msoftdrop_for_valid_jets, behavior={}),
        array_builder=ak.ArrayBuilder()
    ).snapshot()
    
    # Finally, use ak.where to replace None values in the corrected softdrop mass with the original msoftdrop values
    # None values happen when the msoftdrop mass = -1 in the NanoAOD
    # nan values also need to be removed
    new_msoftdrop = ak.where(
        ak.is_none(corrected_msoftdrop_masked, axis=-1),
        jets_jagged.msoftdrop,
        corrected_msoftdrop_masked
    )
    new_msoftdrop = ak.where(
        np.isnan(new_msoftdrop),
        jets_jagged.msoftdrop,
        new_msoftdrop
    )
    jets_jagged["msoftdrop"] = new_msoftdrop

    # N.B.: to be tested yet: JES variations on msoftdrop
    #if jes_syst:
    #    for jes_vari in jes_strings:
    #        for shift in ["up", "down"]:
    #            subjets_jagged_corrected_varied = ak.zip({
    #                "pt": subjets_jagged_corrected[f"pt_JES_{jes_vari}_{shift}"],
    #                "mass": subjets_jagged_corrected[f"mass_JES_{jes_vari}_{shift}"],
    #                "eta": subjets_jagged_corrected.eta,
    #                "phi": subjets_jagged_corrected.phi,
    #                "charge": ak.zeros_like(subjets_jagged_corrected.pt)
    #            }, with_name="PtEtaPhiMCandidate")
    #            corrected_msoftdrop_for_valid_jets = subjets_jagged_corrected_varied.sum(axis=-1).mass
    #            corrected_msoftdrop_for_valid_jets = ak.unflatten(corrected_msoftdrop_for_valid_jets, counts_valid)                
    #            corrected_msoftdrop_masked = ak.copy(jets_with_subjets.msoftdrop)  # This has None for jets without subjets
    #            corrected_msoftdrop_masked = replace_at_indices(
    #                ak.Array(corrected_msoftdrop_masked, behavior={}),
    #                ak.Array(valid_indices, behavior={}),
    #                ak.Array(corrected_msoftdrop_for_valid_jets, behavior={})
    #            ).snapshot()
    #            
    #            # Finally, use ak.where to replace None values in the corrected softdrop mass with the original msoftdrop values
    #            # None values happen when the msoftdrop mass = -1 in the NanoAOD
    #            # nan values also need to be removed
    #            new_msoftdrop = ak.where(
    #                ak.is_none(corrected_msoftdrop_masked, axis=-1),
    #                jets_jagged.msoftdrop,
    #                corrected_msoftdrop_masked
    #            )
    #            new_msoftdrop = ak.where(
    #                np.isnan(new_msoftdrop),
    #                jets_jagged.msoftdrop,
    #                new_msoftdrop
    #            )
    #            jets_jagged[f"msoftdrop_JES_{jes_vari}_{shift}"] = new_msoftdrop

    return jets_jagged
