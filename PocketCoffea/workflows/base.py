import sys
import os
import tarfile
import json
import copy 

import numpy as np
import awkward as ak
from collections import defaultdict

import coffea
from coffea import processor, lookup_tools
from coffea.processor import column_accumulator
from coffea.lumi_tools import LumiMask #, LumiData
from coffea.analysis_tools import PackedSelection, Weights
from coffea.util import load

import correctionlib

from ..lib.WeightsManager import WeightsManager
from ..lib.HistManager import HistManager, Axis, HistConf
from ..lib.triggers import get_trigger_mask
from ..lib.objects import jet_correction, lepton_selection, jet_selection, btagging, get_dilepton
from ..lib.fill import fill_histograms_object
from ..parameters.triggers import triggers
from ..parameters.btag import btag, btag_variations
from ..parameters.jec import JECversions, JERversions
from ..parameters.event_flags import event_flags, event_flags_data
from ..parameters.lumi import lumi, goldenJSON
from ..parameters.samples import samples_info

class ttHbbBaseProcessor(processor.ProcessorABC):

    def __init__(self, cfg) -> None:
        # Read required cuts and histograms from config file
        self.cfg = cfg

        ### Cuts
        # The definition of the cuts in the analysis is hierarchical.
        # 1) a list of *skim* functions is applied on bare NanoAOD, with no object preselection or correction.
        #   Triggers are also applied there. 
        # 2) Objects are corrected and selected and a list of *preselection* cuts are applied to skim the dataset.
        # 3) A list of cut function is applied and the masks are kept in memory to defined later "categories"
        self._skim = self.cfg.skim
        self._preselections  = self.cfg.preselections
        self._cuts = self.cfg.cut_functions
        self._categories = self.cfg.categories
        ### Define PackedSelector to save per-event cuts and dictionary of selections
        # The skim mask is applied on baseline nanoaod before any object is corrected
        self._skim_masks = PackedSelection()
        # The preselection mask is applied after the objects have been corrected
        self._preselection_masks = PackedSelection()
        # After the preselections more cuts are defined and combined in categories.
        # These cuts are applied only for outputs, so they cohexists in the form of masks
        self._cuts_masks = PackedSelection()

        # Weights configuration 
        self.weights_config_allsamples = self.cfg.weights_config

        # Output format
        # Accumulators for the output
        self.output_format = {
            "sum_genweights": {},
            "sumw":  {cat: { s: 0. for s in self.cfg.samples } for cat in self._categories},
            "cutflow": {
                "initial" : { s: 0 for s in self.cfg.samples },
                "skim":     { s: 0 for s in self.cfg.samples },
                "presel":   { s: 0 for s in self.cfg.samples },
                **{cat: { s: 0. for s in self.cfg.samples } for cat in self._categories}
            },
            "seed_chunk": defaultdict(str),
            "variables": { v: {} for v, vcfg in self.cfg.variables.items() if not vcfg.metadata_hist},
            "columns" :  {cat: {} for cat in self._categories},
            "processing_metadata": { v: {} for v, vcfg in self.cfg.variables.items() if vcfg.metadata_hist}
        }

        # Custom axes to add to histograms in this processor
        self.custom_axes = [Axis(field="year", label="year", bins=self.cfg.years,
                                 coll="metadata", type="strcat", growth=False)]
        

    def add_column_accumulator(self, name, cat=None, store_size=True):
        '''
        Add a column_accumulator with `name` to the category `cat` (to all the category if `cat`==None).
        If store_size == True, create a parallel column called name_size, containing the number of entries
        for each event. 
        '''
        if cat == None:
            # add the column accumulator to all the categories
            for cat in self._categories:
                # we need a defaultdict accumulator to be able to include different samples for different chunks
                self.output_format['columns'][cat][name] = {}
                if store_size:
                    # in thise case we save also name+size which will contain the number of entries per event
                    self.output_format['columns'][cat][name+"_size"] = {}
        else:
            if cat not in self._categories:
                raise Exception(f"Category not found: {cat}")
            self.output_format['columns'][cat][name] ={}
            if store_size:
                # in thise case we save also name+size which will contain the number of entries per event
                self.output_format['columns'][cat][name+"_size"] = {}
        
    @property
    def nevents(self):
        '''The current number of events in the chunk is computed with a property'''
        return ak.count(self.events.event, axis=0)

    
    # Function to load year-dependent parameters
    def load_metadata(self):
        self._dataset = self.events.metadata["dataset"]
        self._sample = self.events.metadata["sample"]
        self._year = self.events.metadata["year"]
        self._triggers = triggers[self.cfg.finalstate][self._year]
        self._btag = btag[self._year]
        self._isMC = self.events.metadata["isMC"]
        if self._isMC:
            self._era = "MC"
        else:
            self._era = self.events.metadata["era"]
        # JEC
        self._JECversion = JECversions[self._year]['MC' if self._isMC else 'Data']
        self._JERversion = JERversions[self._year]['MC' if self._isMC else 'Data']
        self._goldenJSON = goldenJSON[self._year]

        
    # Function that computes the trigger masks and save it in the PackedSelector
    def apply_triggers(self):
        # Trigger logic is included in the preselection mask
        self._skim_masks.add('trigger', get_trigger_mask(self.events, self._triggers, self.cfg.finalstate))

    # Function to apply flags and lumi mask
    def skim_events(self):
        # In the initial skimming we apply METfilters, PV requirement, lumimask(for DATA), the trigger,
        # and user-defined skimming function.
        # BE CAREFUL: the skimming is done before any object preselection and cleaning
        mask_flags = np.ones(self.nEvents_initial, dtype=np.bool)
        flags = event_flags[self._year]
        if not self._isMC:
            flags += event_flags_data[self._year]
        for flag in flags:
            mask_flags = getattr(self.events.Flag, flag)
        self._skim_masks.add("event_flags", mask_flags)
        
        # Primary vertex requirement
        self._skim_masks.add("PVgood", self.events.PV.npvsGood > 0)

        # In case of data: check if event is in golden lumi file
        if not self._isMC:
            #mask_lumi = lumimask(self.events.run, self.events.luminosityBlock)
            mask_lumi = LumiMask(self._goldenJSON)(self.events.run, self.events.luminosityBlock)
            self._skim_masks.add("lumi_golden", mask_lumi)

        for skim_func in self._skim:
            # Apply the skim function and add it to the mask
            mask = skim_func.get_mask(self.events, year=self._year, sample=self._sample, btag=self._btag, isMC=self._isMC)
            self._skim_masks.add(skim_func.id, mask)
            
        # Finally we skim the events and count them
        self.events = self.events[self._skim_masks.all(*self._skim_masks.names)]
        self.nEvents_after_skim = self.nevents
        self.output['cutflow']['skim'][self._sample] += self.nEvents_after_skim
        self.has_events = self.nEvents_after_skim > 0
        
    def apply_JERC(self, JER=True, verbose=False):
        if not self._isMC: return
        if int(self._year) > 2018:
            sys.exit("Warning: Run 3 JEC are not implemented yet.")
        if JER:
            self.events.Jet, seed_dict = jet_correction(self.events, "Jet", "AK4PFchs", self._year, self._JECversion, self._JERversion, verbose=verbose)
            self.output['seed_chunk'].update(seed_dict)
        else:
            self.events.Jet = jet_correction(self.events, "Jet", "AK4PFchs", self._year, self._JECversion, verbose=verbose)

    # Function to compute masks to preselect objects and save them as attributes of `events`
    def apply_object_preselection(self):
        # Include the supercluster pseudorapidity variable
        electron_etaSC = self.events.Electron.eta + self.events.Electron.deltaEtaSC
        self.events["Electron"] = ak.with_field(self.events.Electron, electron_etaSC, "etaSC")
        # Build masks for selection of muons, electrons, jets, fatjets
        self.events["MuonGood"]     = lepton_selection(self.events, "Muon", self.cfg.finalstate)
        self.events["ElectronGood"] = lepton_selection(self.events, "Electron", self.cfg.finalstate)
        leptons = ak.with_name( ak.concatenate( (self.events.MuonGood, self.events.ElectronGood), axis=1 ), name='PtEtaPhiMCandidate' )
        self.events["LeptonGood"]   = leptons[ak.argsort(leptons.pt, ascending=False)]
        self.events["JetGood"], self.jetGoodMask = jet_selection(self.events, "Jet", self.cfg.finalstate)
        self.events["BJetGood"] = btagging(self.events["JetGood"], self._btag)

        if self.cfg.finalstate == 'dilepton':
            self.events["ll"] = get_dilepton(self.events.ElectronGood, self.events.MuonGood)

    # Function that counts the preselected objects and save the counts as attributes of `events`
    def count_objects(self):
        self.events["nMuonGood"]     = ak.num(self.events.MuonGood)
        self.events["nElectronGood"] = ak.num(self.events.ElectronGood)
        self.events["nLepGood"]      = self.events["nMuonGood"] + self.events["nElectronGood"]
        self.events["nJetGood"]      = ak.num(self.events.JetGood)
        self.events["nBJetGood"]     = ak.num(self.events.BJetGood)
        #self.events["nfatjet"]   = ak.num(self.events.FatJetGood)

    # Function that defines common variables employed in analyses and save them as attributes of `events`
    def define_common_variables(self):
        self.events["ht"] = ak.sum(abs(self.events.JetGood.pt), axis=1)

    def apply_preselections(self):
        ''' The function computes all the masks from the preselection cuts
        and filter out the events to speed up the later computations.
        N.B.: Preselection happens after the objects correction and cleaning'''
        for cut in self._preselections:
            # Apply the cut function and add it to the mask
            mask = cut.get_mask(self.events, year=self._year, sample=self._sample, isMC=self._isMC)
            self._preselection_masks.add(cut.id, mask)
        # Now that the preselection mask is complete we can apply it to events
        self.events = self.events[self._preselection_masks.all(*self._preselection_masks.names)]
        self.nEvents_after_presel = self.nevents
        self.output['cutflow']['presel'][self._sample] += self.nEvents_after_presel
        self.has_events = self.nEvents_after_presel > 0
            
    def define_categories(self):
        for cut in self._cuts:
            mask = cut.get_mask(self.events, year=self._year, sample=self._sample, isMC=self._isMC )
            self._cuts_masks.add(cut.id, mask)
        # We make sure that for each category the list of cuts is unique in the Configurator validation

    @classmethod
    def available_weights(cls):
        return WeightsManager.available_weights()

    @classmethod
    def available_variations(cls):
        return WeightsManager.available_variations()
    
    def compute_weights(self):
        if not self._isMC: return
        # Creating the WeightsManager with all the configured weights
        self.weights_manager = WeightsManager(self.weights_config_allsamples[self._sample],
                                              self.nEvents_after_presel,
                                              self.events, # to compute weights
                                              storeIndividual=False,
                                              metadata={
                                                  "year": self._year,
                                                  "sample": self._sample,
                                                  "finalstate": self.cfg.finalstate
                                              })
    def compute_weights_extra(self):
        pass
        
    def count_events(self):
        # Fill the output with the number of events and the sum
        # of their weights in each category for each sample
        for category, cuts in self._categories.items():
            mask = self._cuts_masks.all(*cuts)
            self.output["cutflow"][category][self._sample] = ak.sum(mask)
            if self._isMC:
                w = self.weights_manager.get_weight(category)
                self.output["sumw"][category][self._sample] = ak.sum(w*mask)


    def fill_histograms(self):
        # Filling the autofill=True histogram automatically
        self.hists_manager.fill_histograms(self.events,
                                           self.weights_manager,
                                           self._cuts_masks,
                                           custom_fields=None)
        # Saving in the output the filled histograms for the current sample
        for var, H in self.hists_manager.get_histograms().items():
            self.output["variables"][var][self._sample] = H
        # Filling the special histograms for events if they are present
        if "events_per_chunk" in self.hists_manager.histograms:
            h_cfg, hepc = self.hists_manager.get_histogram("events_per_chunk")
            hepc.fill(cat=hepc.axes["cat"][0],
                      variation= "nominal",
                      year= self._year,
                      nEvents_initial = self.nEvents_initial,
                      nEvents_after_skim=self.nEvents_after_skim,
                      nEvents_after_presel=self.nEvents_after_presel)
            self.output["processing_metadata"]["events_per_chunk"][self._sample] = hepc
            
    def process_extra_before_skim(self):
        pass
    
    def process_extra_before_presel(self):
        pass
    
    def process_extra_after_presel(self):
        pass
    
    def define_common_variables_extra(self):
        pass

    def fill_histograms_extra(self):
        pass
    
    def process(self, events):
        self.events = events

        ###################
        # At the beginning of the processing the initial number of events
        # and the sum of the genweights is stored for later use
        #################
        self.load_metadata()
        # Define the accumulator instance for this chunk
        self.output = copy.deepcopy(self.output_format)

        # Create the HistManager
        self.hists_manager = HistManager(self.cfg.variables,
                                         self._sample,
                                         self._categories,
                                         self.cfg.variations_config[self._sample],
                                         self.custom_axes)

        self.nEvents_initial = self.nevents
        self.output['cutflow']['initial'][self._sample] += self.nEvents_initial
        if self._isMC:
            # This is computed before any preselection
            self.output['sum_genweights'][self._sample] = ak.sum(self.events.genWeight)

        self.weights_config = self.weights_config_allsamples[self._sample]
        ########################
        # Then the first skimming happens. 
        # Events that are for sure useless are removed, and trigger are applied.
        # The user can specify in the configuration a function to apply for the skiming.
        # BE CAREFUL: objects are not corrected and cleaned at this stage, the skimming
        # selections MUST be loose and inclusive w.r.t the final selections. 
        #########################
        # Trigger are applied at the beginning since they do not change 
        self.apply_triggers()
        # Customization point for derived workflows before skimming
        self.process_extra_before_skim()
        # MET filter, lumimask, + custom skimming function
        self.skim_events()
        if not self.has_events: return self.output

        #########################
        # After the skimming we apply the object corrections and preselection
        # Doing so we avoid to compute them on the full NanoAOD dataset
        #########################
        # Apply JEC + JER
        self.apply_JERC()

        # Apply preselections
        self.apply_object_preselection()
        self.count_objects()
        self.define_common_variables()
        # Customization point for derived workflows after preselection cuts
        self.process_extra_before_presel()
        
        # This will remove all the events not passing preselection from further processing
        self.apply_preselections()
       
        # If not events remains after the preselection we skip the chunk
        if not self.has_events: return self.output

        ##########################
        # After the preselection cuts has been applied more processing is performwend 
        ##########################
        # Customization point for derived workflows after preselection cuts
        self.process_extra_after_presel()
        
        # This function applies all the cut functions in the cfg file
        # Each category is an AND of some cuts. 
        self.define_categories()

        # Weights
        self.compute_weights()
        self.compute_weights_extra()

        # Fill histograms
        self.fill_histograms()
        self.fill_histograms_extra()
        self.count_events()
        
        return self.output

    def postprocess(self, accumulator):
        # Rescale MC histograms by the total sum of the genweights
        scale_genweight = {}
        for sample in accumulator["cutflow"]["initial"].keys():
            scale_genweight[sample] = 1 if sample.startswith('DATA') else 1./accumulator['sum_genweights'][sample]
            # correct also the sumw (sum of weighted events) accumulator
            for cat in self._categories:
                accumulator["sumw"][cat][sample] *= scale_genweight[sample]

        for var, hists in accumulator["variables"].items():
            # Rescale only histogram without no_weights option
            if self.cfg.variables[var].no_weights: continue
            for sample, h in hists.items():
                 h *= scale_genweight[sample]
        accumulator["scale_genweight"] = scale_genweight
        return accumulator
 
