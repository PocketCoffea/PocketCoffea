import os
import tarfile
import json

import numpy as np
import awkward as ak

import coffea
from coffea import processor, lookup_tools, hist
from coffea.processor import dict_accumulator, defaultdict_accumulator,column_accumulator
from coffea.lumi_tools import LumiMask #, LumiData
from coffea.analysis_tools import PackedSelection, Weights
from coffea.util import load

import correctionlib

from ..lib.triggers import get_trigger_mask
from ..lib.objects import jet_correction, lepton_selection, jet_selection, btagging, get_dilepton
from ..lib.pileup import sf_pileup_reweight
from ..lib.scale_factors import sf_ele_reco, sf_ele_id, sf_mu, sf_btag, sf_btag_calib, sf_jet_puId
from ..lib.fill import fill_histograms_object
from ..parameters.triggers import triggers
from ..parameters.btag import btag, btag_variations
from ..parameters.jec import JECversions, JERversions
from ..parameters.event_flags import event_flags, event_flags_data
from ..parameters.lumi import lumi, goldenJSON
from ..parameters.samples import samples_info
from ..parameters.allhistograms import histogram_settings

class ttHbbBaseProcessor(processor.ProcessorABC):

    def __init__(self, cfg) -> None:
        # Read required cuts and histograms from config file
        self.cfg = cfg

        # Save histogram settings of the required histograms
        self._variables = self.cfg.variables
        self._variables2d = self.cfg.variables2d

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

        # Trigger SF
        self._apply_triggerSF = True

        # Weights configuration
        self.weights_config_allsamples = self.cfg.weights_config
        
        # Accumulators for the output
        self._accum_dict = {}
        self._hist_dict = {}
        self._hist2d_dict = {}
        self._sumw_dict = {
            "sum_genweights": {},
        }
        self._accum_dict.update(self._sumw_dict)
        # Accumulator with number of events passing each category for each sample
        self._cutflow_dict =   {cat: defaultdict_accumulator(int) for cat in self._categories}
        self._cutflow_dict["initial"] = defaultdict_accumulator(int)
        self._cutflow_dict["skim"] = defaultdict_accumulator(int)
        self._cutflow_dict["presel"] = defaultdict_accumulator(int)
        self._accum_dict["cutflow"] = dict_accumulator(self._cutflow_dict)
        
        # Accumulator with sum of weights of the events passing each category for each sample
        self._sumw_weighted_dict =   {cat: defaultdict_accumulator(float) for cat in self._categories}
        self._accum_dict["sumw"] = dict_accumulator(self._sumw_weighted_dict)

        # Accumulator with seed number used for the stochastic smearing, for each processed chunk
        self._seed_dict = {"seed_chunk" : processor.defaultdict_accumulator(int)}
        self._accum_dict.update(self._seed_dict)

        #for var in self._vars_to_plot.keys():
        #       self._accumulator.add(processor.dict_accumulator({var : processor.column_accumulator(np.array([]))}))

        # Define axes
        self._sample_axis = hist.Cat("sample", "Sample")
        self._cat_axis    = hist.Cat("cat", "Cat")
        self._year_axis   = hist.Cat("year", "Year")
        self._sparse_axes = [self._sample_axis, self._cat_axis, self._year_axis]

        if self.cfg.split_eras:
            self._era_axis = hist.Cat("era", "Era")
            self._sparse_axes = [self._sample_axis, self._cat_axis, self._year_axis, self._era_axis]

        # Create histogram accumulators
        for var_name in self._variables.keys():
            if var_name.startswith('n'):
                field = var_name
            elif "_" in var_name:
                splits = obj, field = var_name.split('_')
                if splits[0] in ["electron", "muon", "jet"]:
                    field = splits[1]
                else:
                    field = var_name
            else:
                field = var_name
            variable_axis = hist.Bin( field, self._variables[var_name]['xlabel'],
                                      **self._variables[var_name]['binning'] )
            self._hist_dict[f'hist_{var_name}'] = hist.Hist("$N_{events}$", *self._sparse_axes, variable_axis)
        for hist2d_name in self._variables2d.keys():
            varname_x = list(self._variables2d[hist2d_name].keys())[0]
            varname_y = list(self._variables2d[hist2d_name].keys())[1]
            variable_x_axis = hist.Bin(varname_x, self._variables2d[hist2d_name][varname_x]['xlabel'],
                                       **self._variables2d[hist2d_name][varname_x]['binning'] )
            variable_y_axis = hist.Bin(varname_y, self._variables2d[hist2d_name][varname_y]['ylabel'],
                                       **self._variables2d[hist2d_name][varname_y]['binning'] )
            self._hist2d_dict[f'hist2d_{hist2d_name}'] = hist.Hist("$N_{events}$", *self._sparse_axes, variable_x_axis, variable_y_axis)
            
        self._accum_dict.update(self._hist_dict)
        self._accum_dict.update(self._hist2d_dict)

        self.nobj_hists = [histname for histname in self._hist_dict.keys() if histname.lstrip('hist_').startswith('n') and not 'nevts' in histname]
        self.muon_hists = [histname for histname in self._hist_dict.keys() if 'muon' in histname and not histname in self.nobj_hists]
        self.electron_hists = [histname for histname in self._hist_dict.keys() if 'electron' in histname and not histname in self.nobj_hists]
        self.jet_hists = [histname for histname in self._hist_dict.keys() if 'jet' in histname and not 'fatjet' in histname and not histname in self.nobj_hists]

        # prepare a special entry for column accumulator
        # one for each category
        self._accum_dict["columns"] = {cat: {} for cat in self._categories}

        # The final accumulator dictionary is built when the accumulator() property is called for the first time.
        # Doing so, subclasses can add additional context to the self._accum_dict. 
        # self._accumulator = dict_accumulator(self._accum_dict)
        self._accumulator = None
        
    @property
    def accumulator(self):
        ''' The accumulator instance of the processor is not built in the constructor directly because sub-classes
        can add more accumulators to the self._accum_dict definition.
        When the accumulator() property if then accessed the dict_accumulator is built.'''
        if not self._accumulator:
            self._accumulator = dict_accumulator(self._accum_dict)
        return self._accumulator

    def add_additional_histograms(self, histograms_dict):
        '''Helper function to add additional histograms to the dict_accumulator.
        Usually useful for derived classes to include the definition of custom histograms '''
        for k,h in histograms_dict.items():
            if k in self._accum_dict:
                raise Exception(f"You are trying to overwrite an already defined histogram {k}!")
            else:
                self._accum_dict[k] = h

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
                self._accum_dict['columns'][cat][name] = {}
                if store_size:
                    # in thise case we save also name+size which will contain the number of entries per event
                    self._accum_dict['columns'][cat][name+"_size"] = {}
        else:
            if cat not in self._categories:
                raise Exception(f"Category not found: {cat}")
            self._accum_dict['columns'][cat][name] ={}
            if store_size:
                # in thise case we save also name+size which will contain the number of entries per event
                self._accum_dict['columns'][cat][name+"_size"] = {}
        
    @property
    def nevents(self):
        '''The current number of events in the chunk is computed with a property'''
        return ak.count(self.events.event, axis=0)

    def get_histogram(self, name):
        if name in self.output:
            return self.output[name]
        else:
            raise Exception("Missing histogram: ", name)

    # Define trigger SF map to apply
    def define_triggerSF_maps(self):
        pass
    
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
        
        #adding the metadata in the dictionary
        self.events.metadata["triggers"] = self._triggers
        self.events.metadata["finalstate"] = self.cfg.finalstate
        self.events.metadata["btag"] = self._btag

        
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
            selfd.events.Jet = jet_correction(self.events, "Jet", "AK4PFchs", self._year, self._JECversion, verbose=verbose)

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
        self.events["nmuon"]     = ak.num(self.events.MuonGood)
        self.events["nelectron"] = ak.num(self.events.ElectronGood)
        self.events["nlep"]      = self.events["nmuon"] + self.events["nelectron"]
        self.events["njet"]      = ak.num(self.events.JetGood)
        self.events["nbjet"]     = ak.num(self.events.BJetGood)
        #self.events["nfatjet"]   = ak.num(self.events.FatJetGood)
 

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
            mask = cut.get_mask(self.events, year=self._year, sample=self._sample)
            self._cuts_masks.add(cut.id, mask)
        # We make sure that for each category the list of cuts is unique in the Configurator validation

    @classmethod
    def available_weights(cls):
        return ['genWeight', 'lumi', 'XS', 'pileup',
                'sf_ele_reco_id', 'sf_mu_id_iso', 'sf_btag',
                'sf_btag_calib', 'sf_jet_puId']
        
    def compute_weights(self):
        '''
        This function computes the weights to be applied on MC.
        Weights can be inclusive or by category.
        Moreover, different samples can have different weights, as defined in the weights_configuration.
        The name of the weights available in the current workflow are defined in the class methods "available_weights"
        '''
        # Helper function defining the weights
        def __add_weight(weight, weight_obj):
            if weight == "genWeight":
                weight_obj.add('genWeight', self.events.genWeight)
            elif weight == 'lumi':
                weight_obj.add('lumi', ak.full_like(self.events.genWeight, lumi[self._year]["tot"]))
            elif weight == 'XS':
                weight_obj.add('XS', ak.full_like(self.events.genWeight, samples_info[self._sample]["XS"]))
            elif weight == 'pileup':
                # Pileup reweighting with nominal, up and down variations
                weight_obj.add('pileup', *sf_pileup_reweight(self.events, self._year))
            elif weight == 'sf_ele_reco_id':
                # Electron reco and id SF with nominal, up and down variations
                weight_obj.add('sf_ele_reco', *sf_ele_reco(self.events, self._year))
                weight_obj.add('sf_ele_id',   *sf_ele_id(self.events, self._year))
            elif weight == 'sf_mu_id_iso':
                # Muon id and iso SF with nominal, up and down variations
                weight_obj.add('sf_mu_id',  *sf_mu(self.events, self._year, 'id'))
                weight_obj.add('sf_mu_iso', *sf_mu(self.events, self._year, 'iso'))
            elif weight == 'sf_btag':
                # Get all the nominal and variation SF
                btagsf = sf_btag(self.events.JetGood, self._btag['btagging_algorithm'], self._year,
                                 variations=["central"]+btag_variations[self._year],
                                 njets = self.events.njet)
                for variation, weights in btagsf.items():
                    weight_obj.add(f"sf_btag_{variation}", *weights)
                
            elif weight == 'sf_btag_calib':
                # This variable needs to be defined in another method
                jetsHt = ak.sum(abs(self.events.JetGood.pt), axis=1)
                weight_obj.add("sf_btag_calib", sf_btag_calib(self._sample, self._year, self.events.njet, jetsHt ) )

            elif weight == 'sf_jet_puId':
                weight_obj.add('sf_jet_puId', *sf_jet_puId(self.events.JetGood, self.cfg.finalstate,
                                                           self._year, njets=self.events.njet))

        #Inclusive weights
        self._weights_incl = Weights(self.nEvents_after_presel, storeIndividual=True)

        if not self._isMC: return
        # Compute first the inclusive weights
        for weight in self.weights_config["inclusive"]:
            __add_weight(weight, self._weights_incl)

        # Now weights for dedicated categories
        if self.weights_config["is_split_bycat"]:
            #Create the weights object only if for the current sample there is a weights_by_category
            #configuration
            self._weights_bycat = { cat: Weights(self.nEvents_after_presel, storeIndividual=True)
                                    for cat in self._categories if len(self.weights_config["bycategory"][cat])!=0}
            for cat, ws in self.weights_config["bycategory"].items():
                for weight in ws:
                    __add_weight(weight, self._weights_bycat[cat])

    def get_weight(self, category=None, modifier=None):
        '''
        The function returns the total weights stored in the processor for the current sample.
        If category==None the inclusive weight is returned.
        If category!=None but weights_split_bycat=False, the inclusive weight is returned.
        Otherwise the inclusive*category specific weight is returned.
        '''
        if category==None or self.weights_config["is_split_bycat"] == False:
            # return the inclusive weight
            return self._weights_incl.weight(modifier=modifier)
        
        elif category and self.weights_config["is_split_bycat"] == True:
            if category not in self._categories:
                raise Exception(f"Requested weights for non-existing category: {category}")
            # moltiply the inclusive weight and the by category one
            return self._weights_incl.weight(modifier=modifier) * self._weights_bycat[category].weight(modifier=modifier)
        
    def apply_triggerSF(self):
        if self.cfg.triggerSF != None:
            dict_corr = load(self.cfg.triggerSF)
            self.triggerSF_weights = {cat : Weights(self.nEvents_after_presel) for cat in dict_corr.keys()}
            for cat, corr in dict_corr.items():
                if self._isMC:
                    # By filling the None in the pt array with -999, the SF will be fixed to 1 for events with 0 electrons
                    self.triggerSF_weights[cat].add( 'triggerSFcorr', corr(ak.fill_none(ak.firsts(self.events.ElectronGood.pt), -999), ak.fill_none(ak.firsts(self.events.ElectronGood.eta), -999)) )
                else:
                    print(self.events.ElectronGood.pt)
                    print(ak.ones_like(ak.fill_none(self.events.ElectronGood.pt, 0)))
                    self.triggerSF_weights[cat].add( 'triggerSFcorr', ak.ones_like(ak.fill_none(ak.firsts(self.events.ElectronGood.pt), 0)) )

    def fill_histograms(self):
        for (obj, obj_hists) in zip([None], [self.nobj_hists]):
            fill_histograms_object(self, obj, obj_hists, event_var=True, split_eras=self.cfg.split_eras)
        for (obj, obj_hists) in zip([self.events.MuonGood, self.events.ElectronGood, self.events.JetGood], [self.muon_hists, self.electron_hists, self.jet_hists]):
            fill_histograms_object(self, obj, obj_hists, split_eras=self.cfg.split_eras)

    def count_events(self):
        # Fill the output with the number of events and the sum of their weights in each category for each sample
        for category, cuts in self._categories.items():
            mask = self._cuts_masks.all(*cuts)
            self.output["cutflow"][category][self._sample] = ak.sum(mask)
            if self._isMC:
                w = self.get_weight(category)
                self.output["sumw"][category][self._sample] = ak.sum(w*mask)

    def process_extra_before_skim(self):
        pass
    
    def process_extra_before_presel(self):
        pass
    
    def process_extra_after_presel(self):
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
        self.output = self.accumulator.identity()
        #if len(events)==0: return output
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

        # Per-event trigger SF reweighting
        self.apply_triggerSF()
        
        # Fill histograms
        self.fill_histograms()
        self.fill_histograms_extra()
        self.count_events()
        
        return self.output

    def postprocess(self, accumulator):
        # Rescale MC histograms by the total sum of the genweights
        scale_genweight = {}
        h = accumulator[list(self._hist_dict.keys())[0]]
        for sample in h.identifiers('sample'):
            sample = str(sample)
            scale_genweight[sample] = 1 if sample.startswith('DATA') else 1./accumulator['sum_genweights'][sample]
            # correct also the sumw (sum of weighted events) accumulator
            for cat in self._categories:
                accumulator["sumw"][cat][sample] *= scale_genweight[sample]

        for histname in accumulator:
            if (histname in self._hist_dict) | (histname in self._hist2d_dict):
                accumulator[histname].scale(scale_genweight, axis='sample')

        accumulator["scale_genweight"] = scale_genweight
        

        return accumulator
