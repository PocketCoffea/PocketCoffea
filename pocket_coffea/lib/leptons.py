import awkward as ak
import numpy as np
import correctionlib


def get_ele_scaled(ele, jsonFileName, correction_name, isMC, runNr):
    evaluator = correctionlib.CorrectionSet.from_file(jsonFileName)
    evaluator_scale = evaluator[correction_name]
    ele_gain_flat = ak.flatten(ele["seedGain"])
    ele_eta_flat = ak.flatten(ele["etaSC"])
    ele_r9_flat = ak.flatten(ele["r9"])
    ele_pt_flat = ak.flatten(ele["pt"])
    ele_counts = ak.num(ele["pt"])
    ele_runNr_flat = np.repeat(runNr, ele_counts)
    if not isMC: # for data return nominal
        scale = evaluator_scale.evaluate(
            "total_correction", 
            ele_gain_flat, 
            ele_runNr_flat, 
            ele_eta_flat, 
            ele_r9_flat, 
            ele_pt_flat
        )
        ele_pt_corrected = ak.unflatten(scale * ele_pt_flat, counts=ele_counts)
        return {"nominal": ele_pt_corrected}
    else: # for MC return shifts
        scale_MC_unc = evaluator_scale.evaluate(
            "total_uncertainty", 
            ele_gain_flat, 
            ele_runNr_flat, 
            ele_eta_flat, 
            ele_r9_flat, 
            ele_pt_flat
        )
        ele_pt_scale_up = ak.unflatten((1+scale_MC_unc) * ele_pt_flat, counts=ele_counts)
        ele_pt_scale_down = ak.unflatten((1-scale_MC_unc) * ele_pt_flat, counts=ele_counts)
        return {"up": ele_pt_scale_up, "down": ele_pt_scale_down}

def get_ele_smeared(mc_ele, jsonFileName,correction_name, isMC, only_nominal=True, seed=125):
    if not isMC:
        return
    evaluator = correctionlib.CorrectionSet.from_file(jsonFileName)
    evaluator_smearing = evaluator[correction_name]
    ele_eta_flat = ak.flatten(mc_ele["etaSC"])
    ele_r9_flat = ak.flatten(mc_ele["r9"])
    ele_pt_flat = ak.flatten(mc_ele["pt"])
    ele_counts = ak.num(mc_ele["pt"])
    rng = np.random.default_rng(seed=seed)
    rho = evaluator_smearing.evaluate(
        "rho",
        ele_eta_flat,
        ele_r9_flat
    )
    smearing = rng.normal(loc=1., scale=rho)
    mc_ele_pt_corrected_nom = ak.unflatten(smearing * ele_pt_flat, counts=ele_counts)

    if only_nominal:
        return {"nominal": mc_ele_pt_corrected_nom}
    else:
        unc_rho = evaluator_smearing.evaluate(
            "err_rho", 
            ele_eta_flat, 
            ele_r9_flat
        )
        smearing_up = rng.normal(loc=1., scale=rho + unc_rho)
        smearing_down = rng.normal(loc=1., scale=rho - unc_rho)
        mc_ele_pt_up = ak.unflatten(smearing_up * ele_pt_flat, counts=ele_counts)
        mc_ele_pt_down = ak.unflatten(smearing_down * ele_pt_flat, counts=ele_counts)
        return {"nominal": mc_ele_pt_corrected_nom,
                "up": mc_ele_pt_up, 
                "down": mc_ele_pt_down}



def lepton_selection(events, lepton_flavour, params):

    leptons = events[lepton_flavour]
    cuts = params.object_preselection[lepton_flavour]
    # Requirements on pT and eta
    passes_eta = abs(leptons.eta) < cuts["eta"]
    passes_pt = leptons.pt > cuts["pt"]

    if lepton_flavour == "Electron":
        # Requirements on SuperCluster eta, isolation and id
        etaSC = abs(leptons.deltaEtaSC + leptons.eta)
        passes_SC = np.invert((etaSC >= 1.4442) & (etaSC <= 1.5660))
        passes_iso = True
        if "iso" in cuts.keys():
            passes_iso = leptons.pfRelIso03_all < cuts["iso"]
        passes_id = leptons[cuts['id']] == True

        good_leptons = passes_eta & passes_pt & passes_SC & passes_iso & passes_id

    elif lepton_flavour == "Muon":
        # Requirements on isolation and id
        passes_iso = leptons.pfRelIso04_all < cuts["iso"]
        passes_id = leptons[cuts['id']] == True

        good_leptons = passes_eta & passes_pt & passes_iso & passes_id

    return leptons[good_leptons]

def soft_lepton_selection(events, lepton_flavour, params):

    leptons = events[lepton_flavour]
    cuts = params.object_preselection[lepton_flavour]
    # Requirements on pT and eta
    passes_eta = abs(leptons.eta) < cuts["eta"]
    passes_pt = leptons.pt > cuts["pt"]

    if lepton_flavour == "Electron":
        # Requirements on SuperCluster eta, isolation and id
        etaSC = abs(leptons.deltaEtaSC + leptons.eta)
        passes_SC = np.invert((etaSC >= 1.4442) & (etaSC <= 1.5660))
        passes_iso = True
        if "iso" in cuts.keys():
            passes_iso = leptons.pfRelIso03_all < cuts["iso"]
        passes_id = leptons[cuts['id']] == True

        good_leptons = passes_eta & passes_pt & passes_SC & passes_iso & passes_id

    elif lepton_flavour == "Muon":
        # Requirements on isolation and id
        passes_iso = leptons.pfRelIso04_all > cuts["iso_soft"]
        passes_dxy_check = (abs(leptons.dxy / leptons.dxyErr) > 1.0)
        passes_id = leptons[cuts['id']] == True

        good_leptons = passes_eta & passes_pt & passes_iso & passes_id & passes_dxy_check

    return leptons[good_leptons]


def get_dilepton(electrons, muons, transverse=False):

    fields = {
        "pt": 0.,
        "eta": 0.,
        "phi": 0.,
        "mass": 0.,
        "charge": 0.,
    }

    if muons is None and electrons is None:
        raise("Must specify either muon or electron collection in get_dilepton() function")
    elif muons is None and electrons is not None:
        leptons = ak.pad_none(ak.with_name(electrons, "PtEtaPhiMCandidate"), 2)
    elif electrons is None and muons is not None:
        leptons = ak.pad_none(ak.with_name(muons, "PtEtaPhiMCandidate"), 2)
    else:
        leptons = ak.pad_none(ak.with_name(ak.concatenate([ muons[:, 0:2], electrons[:, 0:2]], axis=1), "PtEtaPhiMCandidate"), 2)
        
    nlep = ak.num(leptons[~ak.is_none(leptons, axis=1)])
    
    ll = leptons[:,0] + leptons[:,1]

    for var in fields.keys():
        fields[var] = ak.where(
            (nlep == 2),
            getattr(ll, var),
            fields[var]
        )
        
    fields["deltaR"] = ak.where(
        (nlep == 2), leptons[:,0].delta_r(leptons[:,1]), -1)
    fields["deltaPhi"] = ak.where(
        (nlep == 2), abs(leptons[:,0].delta_phi(leptons[:,1])), -1)
    fields["deltaEta"] = ak.where(
        (nlep == 2), abs(leptons[:,0].eta - leptons[:,1].eta), -1)
    fields["l1phi"] = ak.where(
        (nlep == 2), leptons[:,0].phi, -1)
    fields["l2phi"] = ak.where(
        (nlep == 2), leptons[:,1].phi, -1)
    fields["l1pt"] = ak.where(
        (nlep == 2), leptons[:,0].pt, -1)
    fields["l2pt"] = ak.where(
        (nlep == 2), leptons[:,1].pt, -1)
    fields["l1eta"] = ak.where(
        (nlep == 2), leptons[:,0].eta, -1)
    fields["l2eta"] = ak.where(
        (nlep == 2), leptons[:,1].eta, -1)
    fields["l1mass"] = ak.where(
        (nlep == 2), leptons[:,0].mass, -1)
    fields["l2mass"] = ak.where(
        (nlep == 2), leptons[:,1].mass, -1)
    

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
        dibosons = dileptons_t + METs
    else:
        dibosons = dileptons + METs

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
            default = ak.from_iter(len(electrons) * [[-9.0]])
        else:
            default = ak.from_iter(len(electrons) * [[-999.9]])
        if not var in ["energy", "x", "y", "z"]:
            fields[var] = ak.where(
                mask_ee, electrons[var][electrons.charge == charge], default
            )
            fields[var] = ak.where(
                mask_mumu, muons[var][muons.charge == charge], fields[var]
            )
            fields[var] = ak.where(
                mask_emu & ak.any(electrons.charge == charge, axis=1),
                electrons[var][electrons.charge == charge],
                fields[var],
            )
            fields[var] = ak.where(
                mask_emu & ak.any(muons.charge == charge, axis=1),
                muons[var][muons.charge == charge],
                fields[var],
            )
            fields[var] = ak.flatten(fields[var])
        else:
            fields[var] = ak.where(
                mask_ee, getattr(electrons, var)[electrons.charge == charge], default
            )
            fields[var] = ak.where(
                mask_mumu, getattr(muons, var)[muons.charge == charge], fields[var]
            )
            fields[var] = ak.where(
                mask_emu & ak.any(electrons.charge == charge, axis=1),
                getattr(electrons, var)[electrons.charge == charge],
                fields[var],
            )
            fields[var] = ak.where(
                mask_emu & ak.any(muons.charge == charge, axis=1),
                getattr(muons, var)[muons.charge == charge],
                fields[var],
            )
            fields[var] = ak.flatten(fields[var])

    charged_leptons = ak.zip(fields, with_name="PtEtaPhiMCandidate")

    return charged_leptons
