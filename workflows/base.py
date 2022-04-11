import json
import coffea
from coffea import hist, processor, lookup_tools
from coffea.lumi_tools import LumiMask #, LumiData
from coffea.analysis_tools import PackedSelection, Weights
import os
import numpy as np
import awkward as ak

from lib.objects import lepton_selection, jet_selection, get_dilepton
from lib.fill import fill_histograms_object
from parameters.triggers import triggers
from parameters.btag import btag
from parameters.lumi import lumi
from parameters.samples import samples_info
from parameters.allhistograms import histogram_settings

class ttHbbBaseProcessor(processor.ProcessorABC):
    def __init__(self, cfg) -> None:
        #self.sample = sample
        # Read required cuts and histograms from config file
        self.cfg = cfg
    
        # Save histogram settings of the required histograms
        self._variables = self.cfg.variables
        self._variables2d = self.cfg.variables2d
        self._hist_dict = {}
        self._hist2d_dict = {}

        # Get the preselection list from the cfg.
        # The set of unique cuts functions and cut groups (categories)
        # are also explicited in the cfg.
        self._preselections  = self.cfg.preselections
        self._cuts = self.cfg.cuts
        self._categories = self.cfg.categories
        ### Define PackedSelector to save per-event cuts and dictionary of selections
        # The preselection mask is extracted on the baseline nanoaod and applied to skim the events
        self._preselection_masks = PackedSelection()
        # After the preselections more cuts are defined and combined in categories.
        # These cuts are applied only before the output 
        self._cuts_masks = PackedSelection()
        
        # Define axes
        sample_axis = hist.Cat("sample", "Sample")
        cat_axis    = hist.Cat("cat", "Cat")
        year_axis   = hist.Cat("year", "Year")

        self._sumw_dict = {
            "sumw": processor.defaultdict_accumulator(float),
            "nevts_initial": processor.defaultdict_accumulator(int),
            "nevts_presel" : processor.defaultdict_accumulator(int),
        }

        #for var in self._vars_to_plot.keys():
        #       self._accumulator.add(processor.dict_accumulator({var : processor.column_accumulator(np.array([]))}))

        for var_name in self._variables.keys():
            if var_name.startswith('n'):
                field = var_name
            else:
                obj, field = var_name.split('_')
            variable_axis = hist.Bin( field, self._variables[var_name]['xlabel'], **self._variables[var_name]['binning'] )
            self._hist_dict[f'hist_{var_name}'] = hist.Hist("$N_{events}$", sample_axis, cat_axis, year_axis, variable_axis)
        for hist2d_name in self._variables2d.keys():
            varname_x = list(self._variables2d[hist2d_name].keys())[0]
            varname_y = list(self._variables2d[hist2d_name].keys())[1]
            variable_x_axis = hist.Bin("x", self._variables2d[hist2d_name][varname_x]['xlabel'], **self._variables2d[hist2d_name][varname_x]['binning'] )
            variable_y_axis = hist.Bin("y", self._variables2d[hist2d_name][varname_y]['ylabel'], **self._variables2d[hist2d_name][varname_y]['binning'] )
            self._hist2d_dict[f'hist2d_{hist2d_name}'] = hist.Hist("$N_{events}$", sample_axis, cat_axis, year_axis, variable_x_axis, variable_y_axis)

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

    @property
    def nevents(self):
        return ak.count(self.events.event)

    # Function to load year-dependent parameters
    def load_metadata(self):
        self._dataset = self.events.metadata["dataset"]
        self._sample = self.events.metadata["sample"]
        self._year = self.events.metadata["year"]
        self._triggers = triggers[self.cfg.finalstate][self._year]
        self._btag = btag[self._year]

    # Function to apply flags and lumi mask
    def clean_events(self):
        mask_clean = np.ones(self.nEvents_initial, dtype=np.bool)
        flags = [
            "goodVertices", "globalSuperTightHalo2016Filter", "HBHENoiseFilter", "HBHENoiseIsoFilter", "EcalDeadCellTriggerPrimitiveFilter", "BadPFMuonFilter"]#, "BadChargedCandidateFilter", "ecalBadCalibFilter"]
        if not self.isMC:
            flags.append("eeBadScFilter")
        for flag in flags:
            mask_clean = mask_clean & getattr(self.events.Flag, flag)
        mask_clean = mask_clean & (self.events.PV.npvsGood > 0)

        # In case of data: check if event is in golden lumi file
        if not self.isMC and not (lumimask is None):
            mask_lumi = lumimask(self.events.run, self.events.luminosityBlock)
            mask_clean = mask_clean & mask_lumi
        # add the basic clearning to the preselection mask
        self._preselection_masks.add('clean', ak.to_numpy(mask_clean))

    # Function to compute masks to preselect objects and save them as attributes of `events`
    def apply_object_preselection(self):
        # Build masks for selection of muons, electrons, jets, fatjets
        self.events["MuonGood"]     = lepton_selection(self.events, "Muon", self.cfg.finalstate)
        self.events["ElectronGood"] = lepton_selection(self.events, "Electron", self.cfg.finalstate)
        leptons = ak.with_name( ak.concatenate( (self.events.MuonGood, self.events.ElectronGood), axis=1 ), name='PtEtaPhiMCandidate' )
        self.events["LeptonGood"]   = leptons[ak.argsort(leptons.pt, ascending=False)]
        self.events["JetGood"]  = jet_selection(self.events, "Jet", self.cfg.finalstate)
        self.events["BJetGood"] = jet_selection(self.events, "Jet", self.cfg.finalstate, btag=self._btag)

        # As a reference, additional masks used in the boosted analysis
        # In case the boosted analysis is implemented, the correct lepton cleaning should be checked (remove only "leading" leptons)
        #self.good_fatjets = jet_selection(self.events, "FatJet", self.cfg.finalstate)
        #self.good_jets_nohiggs = ( self.good_jets & (jets.delta_r(leading_fatjets) > 1.2) )
        #self.good_bjets_boosted = self.good_jets_nohiggs & (getattr(self.events.jets, self.parameters["btagging_algorithm"]) > self.parameters["btagging_WP"])
        #self.good_nonbjets_boosted = self.good_jets_nohiggs & (getattr(self.events.jets, self.parameters["btagging_algorithm"]) < self.parameters["btagging_WP"])
        #self.events["FatJetGood"]   = self.events.FatJet[self.good_fatjets]

        if self.cfg.finalstate == 'dilepton':
            self.events["ll"]           = get_dilepton(self.events.ElectronGood, self.events.MuonGood)

    # Function that counts the preselected objects and save the counts as attributes of `events`
    def count_objects(self):
        self.events["nmuon"]     = ak.num(self.events.MuonGood)
        self.events["nelectron"] = ak.num(self.events.ElectronGood)
        self.events["nlep"]      = self.events["nmuon"] + self.events["nelectron"]
        self.events["njet"]      = ak.num(self.events.JetGood)
        self.events["nbjet"]     = ak.num(self.events.BJetGood)
        #self.events["nfatjet"]   = ak.num(self.events.FatJetGood)

    # Function that computes the trigger masks and save the logical OR of the mumu, emu and ee triggers in the PackedSelector
    def apply_triggers(self):
        # Trigger logic
        self.trigger_mumu = np.zeros(len(self.events), dtype='bool')
        self.trigger_emu = np.zeros(len(self.events), dtype='bool')
        self.trigger_ee = np.zeros(len(self.events), dtype='bool')

        for trigger in self._triggers["mumu"]: self.trigger_mumu = self.trigger_mumu | self.events.HLT[trigger.lstrip("HLT_")]
        for trigger in self._triggers["emu"]:  self.trigger_emu  = self.trigger_emu  | self.events.HLT[trigger.lstrip("HLT_")]
        for trigger in self._triggers["ee"]:   self.trigger_ee   = self.trigger_ee   | self.events.HLT[trigger.lstrip("HLT_")]
        # Trigger preselection
        self._preselection_masks.add('trigger', ak.to_numpy(self.trigger_mumu | self.trigger_emu | self.trigger_ee))

    def apply_preselection_cuts(self):
        for cut in self._preselections:
            # Apply the cut function and add it to the mask
            mask = cut.get_mask(self.events, year=self._year, sample=self._sample)
            self._preselection_masks.add(cut.name, mask)
        # Now that the preselection mask is complete we can apply it to events
        self.events = self.events[self._preselection_masks.all(*self._preselection_masks.names)]
            
    def define_categories(self):
        for cut in self._cuts:
            mask = cut.get_mask(self.events, year=self._year, sample=self._sample)
            self._cuts_masks.add(cut.name, mask)
        # We make sure that for each category the list of cuts is unique in the Configurator validation

    def compute_weights(self):
        self.weights = Weights(self.nevents)
        if self.isMC:
            self.weights.add('genWeight', self.events.genWeight)
            self.weights.add('lumi', ak.full_like(self.events.genWeight, lumi[self._year]))
            self.weights.add('XS', ak.full_like(self.events.genWeight, samples_info[self._sample]["XS"]))
            self.weights.add('sumw', ak.full_like(self.events.genWeight, 1./self.output["sumw"][self._sample]))

    def fill_histograms(self):
        for (obj, obj_hists) in zip([None], [self.nobj_hists]):
            fill_histograms_object(self, obj, obj_hists, event_var=True)
        for (obj, obj_hists) in zip([self.events.MuonGood, self.events.ElectronGood, self.events.JetGood], [self.muon_hists, self.electron_hists, self.jet_hists]):
            fill_histograms_object(self, obj, obj_hists)

    def process_extra_before_presel(self) -> ak.Array:
        pass

    def process_extra_after_presel(self) -> ak.Array:
        pass

    def fill_histograms_extra(self):
        pass
    
    def process(self, events):
        self.events = events
        self.load_metadata()
        self.output = self.accumulator.identity()
        #if len(events)==0: return output
        self.nEvents_initial = self.nevents
        self.output['nevts_initial'][self._sample] += self.nEvents_initial
        self.isMC = 'genWeight' in self.events.fields
        if self.isMC:
            # This is computed before any preselection
            self.output['sumw'][self._sample] += sum(self.events.genWeight)

        # Event cleaning and  PV selection
        self.clean_events()

        # Apply preselections, triggers and cuts
        self.apply_object_preselection()
        self.count_objects()
        self.apply_triggers()
        # Possible extra work before applying preselections
        self.process_extra_before_presel()
        # This will remove all the events not passing preselection from further 
        self.apply_preselection_cuts()
        self.nEvents_after_presel = self.nevents
        self.output['nevts_presel'][self._sample] += self.nEvents_after_presel

        # This function looks at the categories in the cfg file,
        # apply the single cut functions and then prepares the categories
        self.define_categories()

        # Weights
        self.compute_weights()
        
        # This function is empty in the base processor, but can be overriden in processors derived from the class ttHbbBaseProcessor
        self.process_extra_after_presel()

        # Fill histograms
        self.fill_histograms()
        self.fill_histograms_extra()
        
        return self.output

    def postprocess(self, accumulator):

        return accumulator
