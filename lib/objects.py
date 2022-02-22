import argparse

import awkward as ak
import numpy as np
import math
import uproot
from vector import MomentumObject4D

from coffea import hist
from coffea.nanoevents.methods import nanoaod

ak.behavior.update(nanoaod.behavior)

def lepton_selection(leps, cuts, year):

	passes_eta = (np.abs(leps.eta) < cuts["eta"])
	passes_subleading_pt = (leps.pt > cuts["subleading_pt"])
	passes_leading_pt = (leps.pt > cuts["leading_pt"][year])

	if cuts["type"] == "el":
		sca = np.abs(leps.deltaEtaSC + leps.eta)
		passes_id = (leps.cutBased >= 4)
		passes_SC = np.invert((sca >= 1.4442) & (sca <= 1.5660))
		# cuts taken from: https://twiki.cern.ch/twiki/bin/view/CMS/CutBasedElectronIdentificationRun2#Working_points_for_92X_and_later
		passes_impact = ((leps.dz < 0.10) & (sca <= 1.479)) | ((leps.dz < 0.20) & (sca > 1.479)) | ((leps.dxy < 0.05) & (sca <= 1.479)) | ((leps.dxy < 0.1) & (sca > 1.479))

		#select electrons
		good_leps = passes_eta & passes_leading_pt & passes_id & passes_SC & passes_impact
		veto_leps = passes_eta & passes_subleading_pt & np.invert(good_leps) & passes_id & passes_SC & passes_impact

	elif cuts["type"] == "mu":
		passes_leading_iso = (leps.pfRelIso04_all < cuts["leading_iso"])
		passes_subleading_iso = (leps.pfRelIso04_all < cuts["subleading_iso"])
		passes_id = (leps.tightId == 1)

		#select muons
		good_leps = passes_eta & passes_leading_pt & passes_leading_iso & passes_id
		veto_leps = passes_eta & passes_subleading_pt & passes_subleading_iso & passes_id & np.invert(good_leps)

	return good_leps, veto_leps

def get_dilepton_v7(electrons, muons, transverse=False):

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

def get_diboson_v7(dileptons, MET, transverse=False):

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

def get_charged_leptons_v7(electrons, muons, charge, mask):

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

# N.B.: This function works only with awkward v1.5.1 & coffea v0.7.9, it doesn't work with awkward 1.7.0 & coffea v0.7.11
def jet_selection_v7(jets, leps, mask_leps, cuts):

	# Only jets that are more distant than dr to ALL leptons are tagged as good jets
	sleps = leps[mask_leps]
	nlep_max = ak.max(ak.num(sleps, axis=1))

	# A fake lepton object filled with nan values is used for padding in the delta_r calculation
	pfake = {'pt' : [-9999.9], 'eta' : [-9999.9], 'phi' : [-9999.9], 'mass' : [-9999.9]}
	fakeLepton = ak.zip(pfake, with_name="PtEtaPhiMCandidate")
	slepsPadded = ak.fill_none(ak.pad_none(sleps, nlep_max), fakeLepton)
	good_jets = (jets.pt > cuts["pt"]) & (np.abs(jets.eta) < cuts["eta"]) & (jets.jetId >= cuts["jetId"])

	for i in range(nlep_max):
		jets_pass_dr = (jets.delta_r(slepsPadded[:,i]) > cuts["dr"])
		jets_pass_dr = ak.fill_none(jets_pass_dr, True)
		good_jets = good_jets & jets_pass_dr


	if cuts["type"] == "jet":
		good_jets = good_jets & ( ( (jets.pt < 50) & (jets.puId >= cuts["puId"]) ) | (jets.pt >= 50) )

	return good_jets

def jet_nohiggs_selection(jets, mask_jets, fatjets, dr=1.2):

	#nested_mask = jets.p4.match(fatjets.p4[mask_fatjets,0], matchfunc=pass_dr, dr=dr)
	nested_mask = jets.match(fatjets, matchfunc=pass_dr, dr=dr)
	jets_pass_dr = nested_mask.all()

	return mask_jets & jets_pass_dr
