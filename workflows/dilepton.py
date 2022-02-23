import json
import coffea
from coffea import hist, processor, lookup_tools
from coffea.lumi_tools import LumiMask #, LumiData
import os
import h5py
import numpy as np
import awkward as ak
#import uproot

from lib.objects import lepton_selection, jet_nohiggs_selection, jet_selection_v7, get_dilepton_v7, get_diboson_v7, get_charged_leptons_v7
from lib.weights import load_puhist_target, compute_lepton_weights
#from lib.reconstruction import pnuCalculator_v7
from utils.parameters import histogram_settings, samples_info

class ttHbbDilepton	(processor.ProcessorABC):
	def __init__(self, year='2017', parameters=None, cfg='test.json', samples_info=samples_info, hist_dir='histograms/', hist2d =False, DNN=False):
		#self.sample = sample
		with open(cfg) as f:
			self.cfg = json.load(f)
		self.year = year
		self.parameters = parameters
		self.samples_info = samples_info
		self.hist2d = hist2d
		self.DNN = DNN
		self._nsolved = 0
		self._n2l2b = 0
		self._variables = histogram_settings['variables']
		self._variables2d = histogram_settings['variables2d']
		self._varnames = self.cfg['variables']
		self._hist2dnames = self.cfg['variables2d']
		self._hist_dict = {}
		self._hist2d_dict = {}
		self._selections = {
		  'trigger' : {
			#'nbtags' : 2,
		  },
		  'basic' : {
			'nbtags' : 2,
		  },
		}

		# Define axes
		dataset_axis = hist.Cat("dataset", "Dataset")
		cut_axis     = hist.Cat("cut", "Cut")

		self._sumw_dict = {
			"sumw": processor.defaultdict_accumulator(float),
			"nevts": processor.defaultdict_accumulator(int),
		}

		#for var in self._vars_to_plot.keys():
		#	self._accumulator.add(processor.dict_accumulator({var : processor.column_accumulator(np.array([]))}))

		#for mask_name in self._selections.keys():
		for var_name in self._varnames:
			obj, field = var_name.split('_')
			variable_axis = hist.Bin( field, self._variables[var_name]['xlabel'], **self._variables[var_name]['binning'] )
			self._hist_dict[f'hist_{var_name}'] = hist.Hist("$N_{events}$", dataset_axis, cut_axis, variable_axis)
		for hist2d_name in self._hist2dnames:
			varname_x = list(self._variables2d[hist2d_name].keys())[0]
			varname_y = list(self._variables2d[hist2d_name].keys())[1]
			variable_x_axis = hist.Bin("x", self._variables2d[hist2d_name][varname_x]['xlabel'], **self._variables2d[hist2d_name][varname_x]['binning'] )
			variable_y_axis = hist.Bin("y", self._variables2d[hist2d_name][varname_y]['ylabel'], **self._variables2d[hist2d_name][varname_y]['binning'] )
			self._hist2d_dict[f'hist2d_{hist2d_name}'] = hist.Hist("$N_{events}$", dataset_axis, cut_axis, variable_x_axis, variable_y_axis)

		if self.hist2d:
			self._hist_dict.update(**self._hist2d_dict)
		self._hist_dict.update(**self._sumw_dict)
		self._accumulator = processor.dict_accumulator(self._hist_dict)
		self.muon_hists = [histname for histname in self._hist_dict.keys() if 'muon' in histname]
		self.electron_hists = [histname for histname in self._hist_dict.keys() if 'electron' in histname]
		self.jet_hists = [histname for histname in self._hist_dict.keys() if 'jet' in histname and not 'fatjet' in histname]

	@property
	def accumulator(self):
		return self._accumulator

	def process(self, events):
		output = self.accumulator.identity()
		#if len(events)==0: return output
		dataset = events.metadata["dataset"]
		nEvents = ak.count(events.event)
		output['nevts'][dataset] += nEvents
		isMC = 'genWeight' in events.fields
		if isMC:
			output['sumw'][dataset] += sum(events.genWeight)

		cuts = processor.PackedSelection()

		muons = events.Muon
		electrons = events.Electron
		#scalars = events.eventvars
		jets = events.Jet
		fatjets = events.FatJet
		PuppiMET = events.PuppiMET
		PV = events.PV
		Flag = events.Flag
		run = events.run
		luminosityBlock = events.luminosityBlock
		HLT = events.HLT
		genWeight = events.genWeight
		#puWeight = events.puWeight
		if isMC:
			genparts = events.GenPart

		if self.year =='2017':
			metstruct = 'METFixEE2017'
			MET = events.METFixEE2017
		else:
			metstruct = 'MET'
			MET = events.MET

		mask_clean = np.ones(nEvents, dtype=np.bool)

		# Event cleaning and  PV selection
		flags = [
			"goodVertices", "globalSuperTightHalo2016Filter", "HBHENoiseFilter", "HBHENoiseIsoFilter", "EcalDeadCellTriggerPrimitiveFilter", "BadPFMuonFilter"]#, "BadChargedCandidateFilter", "ecalBadCalibFilter"]
		if not isMC:
			flags.append("eeBadScFilter")
		for flag in flags:
			mask_clean = mask_clean & getattr(Flag, flag)
		mask_clean = mask_clean & (PV.npvsGood > 0)

		# Weights
		weights = processor.Weights(nEvents)
		if isMC:
			weights.add('genWeight', genWeight)

		# In case of data: check if event is in golden lumi file
		if not isMC and not (lumimask is None):
			mask_lumi = lumimask(run, luminosityBlock)
			mask_clean = mask_clean & mask_lumi

		cuts.add('clean', ak.to_numpy(mask_clean))

		# Build masks for selection of muons, electrons, jets, fatjets
		good_muons, veto_muons = lepton_selection(muons, self.parameters["muons"], self.year)
		good_electrons, veto_electrons = lepton_selection(electrons, self.parameters["electrons"], self.year)
		good_jets = jet_selection_v7(jets, muons, (good_muons|veto_muons), self.parameters["jets"]) & jet_selection_v7(jets, electrons, (good_electrons|veto_electrons), self.parameters["jets"])
		good_bjets = good_jets & (getattr(jets, self.parameters["btagging_algorithm"]) > self.parameters["btagging_WP"])
		good_fatjets = jet_selection_v7(fatjets, muons, good_muons, self.parameters["fatjets"]) & jet_selection_v7(fatjets, electrons, good_electrons, self.parameters["fatjets"])

		# As a reference, additional masks used in the boosted analysis
		#good_jets_nohiggs = ( good_jets & (jets.delta_r(leading_fatjets) > 1.2) )
		#good_bjets_boosted = good_jets_nohiggs & (getattr(jets, self.parameters["btagging_algorithm"]) > self.parameters["btagging_WP"])
		#good_nonbjets_boosted = good_jets_nohiggs & (getattr(jets, self.parameters["btagging_algorithm"]) < self.parameters["btagging_WP"])

		leadFatJet = ak.firsts(events.FatJet)

		# apply basic event selection -> individual categories cut later
		ngoodmuons     = ak.num(muons[good_muons])
		ngoodelectrons = ak.num(electrons[good_electrons])
		nmuons         = ak.num(muons[good_muons | veto_muons])
		nelectrons     = ak.num(electrons[good_electrons | veto_electrons])
		ngoodleps      = ngoodmuons + ngoodelectrons
		nleps          = nmuons + nelectrons
		njets          = ak.num(jets[good_jets])
		nbjets         = ak.num(jets[good_bjets])
		nfatjets       = ak.num(fatjets[good_fatjets])

		# As a reference, here is how jets are counted in the boosted analysis
		#njets_boosted  = ak.num(jets[nonbjets])
		#nbjets_resolved = ak.num(jets[good_bjets_boosted])


		# trigger logic
		#trigger_mu = (nleps == 1) & (nmuons == 1)
		#trigger_e = (nleps == 1) & (nelectrons == 1)
		trigger_mumu = np.zeros(len(events), dtype='bool')
		trigger_emu = np.zeros(len(events), dtype='bool')
		trigger_ee = np.zeros(len(events), dtype='bool')
		triggers = []

		for trigger in self.parameters["triggers"]["mumu"]: trigger_mumu = trigger_mumu | HLT[trigger.lstrip("HLT_")]
		for trigger in self.parameters["triggers"]["emu"]:  trigger_emu  = trigger_emu  | HLT[trigger.lstrip("HLT_")]
		for trigger in self.parameters["triggers"]["ee"]:   trigger_ee   = trigger_ee   | HLT[trigger.lstrip("HLT_")]

		cuts.add('trigger', ak.to_numpy(trigger_mumu | trigger_emu | trigger_ee))
		cuts.add('dilepton', ak.to_numpy(nleps == 2))

		selection = {}
		selection['trigger'] = {'clean', 'trigger'}
		selection['basic'] = {'clean', 'trigger', 'dilepton'}

		for histname, h in output.items():
			for cut in self._selections.keys():
				if histname in self.muon_hists:
					muon = muons[good_muons | veto_muons]
					weight = ak.flatten(weights.weight() * ak.Array(ak.ones_like(muon.pt) * cuts.all(*selection[cut])))
					fields = {k: ak.flatten(ak.fill_none(muon[k], -9999)) for k in h.fields if k in dir(muon)}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)
				if histname in self.electron_hists:
					electron = electrons[good_electrons | veto_electrons]
					weight = ak.flatten(weights.weight() * ak.Array(ak.ones_like(electron.pt) * cuts.all(*selection[cut])))
					fields = {k: ak.flatten(ak.fill_none(electron[k], -9999)) for k in h.fields if k in dir(electron)}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)
				if histname in self.jet_hists:
					jet = jets[good_jets]
					weight = ak.flatten(weights.weight() * ak.Array(ak.ones_like(jet.pt) * cuts.all(*selection[cut])))
					fields = {k: ak.flatten(ak.fill_none(jet[k], -9999)) for k in h.fields if k in dir(jet)}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)

		return output

	def postprocess(self, accumulator):

		#print(accumulator['leading_lepton_pt'])
		if self.DNN:
			filepath = self.DNN
			hdf_dir = '/'.join(filepath.split('/')[:-1])
			if not os.path.exists(hdf_dir):
				os.makedirs(hdf_dir)
			# Minimal input set, only leading jet, lepton and fatjet observables
			# No top-related variables used
			input_vars = ['ngoodjets', 'njets', 'btags', 'nfatjets', 'met_pt', 'met_phi',
						  'leading_jet_pt', 'leading_jet_eta', 'leading_jet_phi', 'leading_jet_mass',
						  'leadAK8JetPt', 'leadAK8JetEta', 'leadAK8JetPhi', 'leadAK8JetMass', 'leadAK8JetHbb', 'leadAK8JetTau21',
						  'lepton_plus_pt', 'lepton_plus_eta', 'lepton_plus_phi', 'lepton_plus_mass',
						  'lepton_minus_pt', 'lepton_minus_eta', 'lepton_minus_phi', 'lepton_minus_mass',
						  'deltaRHiggsTop', 'deltaRHiggsTopbar', 'deltaRHiggsTT', 'deltaRLeptonPlusHiggs', 'deltaRLeptonMinusHiggs',
						  'ttHbb_label',
						  'weights_nominal']
			for key, value in accumulator.items():
				if key in input_vars:
					print(key, value.value)
			vars_to_DNN = {key : ak.Array(value.value) for key, value in accumulator.items() if key in input_vars}
			print("vars_to_DNN = ", vars_to_DNN)
			inputs = ak.zip(vars_to_DNN)
			print("inputs = ", inputs)
			print(f"Saving DNN inputs to {filepath}")
			df = ak.to_pandas(inputs)
			df.to_hdf(filepath, key='df', mode='w')
			"""
			h5f = h5py.File(filepath, 'w')
			for k in inputs.fields:
				print("Create", k)
				h5f.create_dataset(k, data=inputs[k])
			h5f.close()
			"""

		return accumulator
