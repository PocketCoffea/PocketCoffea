import json
import coffea
from coffea import hist, processor, lookup_tools
from coffea.lumi_tools import LumiMask #, LumiData
import os
import h5py
import numpy as np
import awkward as ak
#import uproot

from lib.objects import lepton_selection, jet_nohiggs_selection, jet_selection, get_dilepton, get_diboson, get_charged_leptons
from lib.weights import load_puhist_target, compute_lepton_weights
#from lib.reconstruction import pnuCalculator
from parameters.cuts import cuts
from parameters.triggers import triggers
from parameters.btag import btag
from parameters.lumi import lumi
from parameters.samples import samples_info
from parameters.allhistograms import histogram_settings

class ttHbbDilepton	(processor.ProcessorABC):
	def __init__(self, year='2017', cfg='test.json', hist_dir='histograms/', hist2d =False, DNN=False):
		#self.sample = sample
		with open(cfg) as f:
			self.cfg = json.load(f)
		self._year = year
		self._cuts = cuts['dilepton']
		self._triggers = triggers['dilepton'][self._year]
		self._btag = btag[self._year]
		self._samples_info = samples_info
		self.hist2d = hist2d
		self.DNN = DNN
		self._nsolved = 0
		self._n2l2b = 0
		self._varnames = self.cfg['variables']
		self._hist2dnames = self.cfg['variables2d']
		self._variables = {var_name : histogram_settings['variables'][var_name] for var_name in self._varnames}
		self._variables2d = {hist2d_name : histogram_settings['variables2d'][hist2d_name] for hist2d_name in self._hist2dnames}
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

		#self._nobj_dict = {f'hist_{key}' : None for key in ['nmuon', 'nelectron', 'nlep', 'nmuongood', 'nelectrongood', 'nlepgood', 'njet', 'nbjet', 'nfatjet']}

		#for var in self._vars_to_plot.keys():
		#	self._accumulator.add(processor.dict_accumulator({var : processor.column_accumulator(np.array([]))}))

		#for mask_name in self._selections.keys():
		for var_name in self._varnames:
			if var_name.startswith('n'):
				field = var_name
			else:
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
		self.nobj_hists = [histname for histname in self._hist_dict.keys() if histname.lstrip('hist_').startswith('n') and not 'nevts' in histname]
		self.muon_hists = [histname for histname in self._hist_dict.keys() if 'muon' in histname and not histname in self.nobj_hists]
		self.electron_hists = [histname for histname in self._hist_dict.keys() if 'electron' in histname and not histname in self.nobj_hists]
		self.jet_hists = [histname for histname in self._hist_dict.keys() if 'jet' in histname and not 'fatjet' in histname and not histname in self.nobj_hists]

	@property
	def accumulator(self):
		return self._accumulator

	def object_preselection(self, events):
		# Build masks for selection of muons, electrons, jets, fatjets
		self.good_muons, self.veto_muons = lepton_selection(events.Muon, self._cuts["muons"], self._year)
		self.good_electrons, self.veto_electrons = lepton_selection(events.Electron, self._cuts["electrons"], self._year)
		self.good_jets = jet_selection(events.Jet, events.Muon, (self.good_muons|self.veto_muons), self._cuts["jets"]) & jet_selection(events.Jet, events.Electron, (self.good_electrons|self.veto_electrons), self._cuts["jets"])
		self.good_bjets = self.good_jets & (getattr(events.Jet, self._btag["btagging_algorithm"]) > self._btag["btagging_WP"])
		self.good_fatjets = jet_selection(events.FatJet, events.Muon, self.good_muons, self._cuts["fatjets"]) & jet_selection(events.FatJet, events.Electron, self.good_electrons, self._cuts["fatjets"])

		# As a reference, additional masks used in the boosted analysis
		#self.good_jets_nohiggs = ( self.good_jets & (jets.delta_r(leading_fatjets) > 1.2) )
		#self.good_bjets_boosted = self.good_jets_nohiggs & (getattr(events.jets, self.parameters["btagging_algorithm"]) > self.parameters["btagging_WP"])
		#self.good_nonbjets_boosted = self.good_jets_nohiggs & (getattr(events.jets, self.parameters["btagging_algorithm"]) < self.parameters["btagging_WP"])

		events["MuonAll"]      = events.Muon[self.good_muons | self.veto_muons]
		events["ElectronAll"]  = events.Electron[self.good_electrons | self.veto_electrons]
		events["MuonGood"]     = events.Muon[self.good_muons]
		events["ElectronGood"] = events.Electron[self.good_electrons]
		events["JetGood"]      = events.Jet[self.good_jets]
		events["JetGoodB"]     = events.Jet[self.good_bjets]
		events["FatJetGood"]   = events.FatJet[self.good_fatjets]

		return

	def count_objects(self, events):
		self.nmuongood     = ak.num(events.MuonGood)
		self.nelectrongood = ak.num(events.ElectronGood)
		self.nmuon         = ak.num(events.MuonAll)
		self.nelectron     = ak.num(events.ElectronAll)
		self.nlepgood      = self.nmuongood + self.nelectrongood
		self.nlep          = self.nmuon + self.nelectron
		self.njet          = ak.num(events.JetGood)
		self.nbjet         = ak.num(events.JetGoodB)
		self.nfatjet       = ak.num(events.FatJetGood)

		# As a reference, here is how jets are counted in the boosted analysis
		#njet_boosted  = ak.num(jets[nonbjets])
		#nbjet_resolved = ak.num(jets[good_bjets_boosted])

		return

	def apply_triggers(self, events):
		# Trigger logic
		self.trigger_mumu = np.zeros(len(events), dtype='bool')
		self.trigger_emu = np.zeros(len(events), dtype='bool')
		self.trigger_ee = np.zeros(len(events), dtype='bool')

		for trigger in self._triggers["mumu"]: self.trigger_mumu = self.trigger_mumu | events.HLT[trigger.lstrip("HLT_")]
		for trigger in self._triggers["emu"]:  self.trigger_emu  = self.trigger_emu  | events.HLT[trigger.lstrip("HLT_")]
		for trigger in self._triggers["ee"]:   self.trigger_ee   = self.trigger_ee   | events.HLT[trigger.lstrip("HLT_")]

		return

	def process(self, events):
		output = self.accumulator.identity()
		#if len(events)==0: return output
		dataset = events.metadata["dataset"]
		nEvents = ak.count(events.event)
		output['nevts'][dataset] += nEvents
		self.isMC = 'genWeight' in events.fields
		if self.isMC:
			output['sumw'][dataset] += sum(events.genWeight)

		cuts = processor.PackedSelection()

		if self._year =='2017':
			metstruct = 'METFixEE2017'
			MET = events.METFixEE2017
		else:
			metstruct = 'MET'
			MET = events.MET

		mask_clean = np.ones(nEvents, dtype=np.bool)

		# Event cleaning and  PV selection
		flags = [
			"goodVertices", "globalSuperTightHalo2016Filter", "HBHENoiseFilter", "HBHENoiseIsoFilter", "EcalDeadCellTriggerPrimitiveFilter", "BadPFMuonFilter"]#, "BadChargedCandidateFilter", "ecalBadCalibFilter"]
		if not self.isMC:
			flags.append("eeBadScFilter")
		for flag in flags:
			mask_clean = mask_clean & getattr(events.Flag, flag)
		mask_clean = mask_clean & (events.PV.npvsGood > 0)

		# Weights
		weights = processor.Weights(nEvents)
		if self.isMC:
			weights.add('genWeight', events.genWeight)

		# In case of data: check if event is in golden lumi file
		if not self.isMC and not (lumimask is None):
			mask_lumi = lumimask(events.run, events.luminosityBlock)
			mask_clean = mask_clean & mask_lumi

		cuts.add('clean', ak.to_numpy(mask_clean))

		leadFatJet = ak.firsts(events.FatJet)

		self.object_preselection(events)
		self.count_objects(events)
		self.apply_triggers(events)

		cuts.add('trigger', ak.to_numpy(self.trigger_mumu | self.trigger_emu | self.trigger_ee))
		cuts.add('dilepton', ak.to_numpy(self.nlep == 2))

		selection = {}
		selection['trigger'] = {'clean', 'trigger'}
		selection['basic'] = {'clean', 'trigger', 'dilepton'}

		for histname, h in output.items():
			for cut in self._selections.keys():
				if histname in self.nobj_hists:
					weight = weights.weight() * cuts.all(*selection[cut])
					fields = {k: ak.fill_none(getattr(self, k), -9999) for k in h.fields if k in histname}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)					
				if histname in self.muon_hists:
					muon = events.MuonAll
					weight = ak.flatten(weights.weight() * ak.Array(ak.ones_like(muon.pt) * cuts.all(*selection[cut])))
					fields = {k: ak.flatten(ak.fill_none(muon[k], -9999)) for k in h.fields if k in dir(muon)}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)
				if histname in self.electron_hists:
					electron = events.ElectronAll
					weight = ak.flatten(weights.weight() * ak.Array(ak.ones_like(electron.pt) * cuts.all(*selection[cut])))
					fields = {k: ak.flatten(ak.fill_none(electron[k], -9999)) for k in h.fields if k in dir(electron)}
					h.fill(dataset=dataset, cut=cut, **fields, weight=weight)
				if histname in self.jet_hists:
					jet = events.JetGood
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
