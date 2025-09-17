import numpy as np
import awkward as ak
import uproot
from collections import defaultdict
from abc import ABC, abstractmethod
import cachetools
from copy import deepcopy

import copy
import os
import logging
import time

from coffea import processor
from coffea.analysis_tools import PackedSelection

from ..lib.weights.weights_manager import WeightsManager
from ..lib.columns_manager import ColumnsManager
from ..lib.hist_manager import HistManager
from ..lib.jets import jet_correction, met_correction_after_jec, load_jet_factory
from ..lib.leptons import get_ele_smeared, get_ele_scaled
from ..lib.categorization import CartesianSelection
from ..lib.calibrators.calibrators_manager import CalibratorsManager
from ..utils.skim import uproot_writeable, copy_file
from ..utils.utils import dump_ak_array

from ..utils.configurator import Configurator


class BaseProcessorABC(processor.ProcessorABC, ABC):
    '''
    Abstract Class which defined the common operations of a PocketCoffea
    processor.
    '''

    def __init__(self, cfg: Configurator) -> None:
        '''
        Constructor of the BaseProcessor.
        It instanties the objects needed for skimming and categories and prepared the
        `output_format` dictionary.

        :param cfg: Configurator object containing all the configurations.
        '''
        # Saving the configurator object in the processor
        self.cfg = cfg
        self.workflow_options = self.cfg.workflow_options
        # Saving the parameters from the configurator objects
        self.params = self.cfg.parameters
        # Cuts
        # 1) a list of *skim* functions is applied on bare NanoAOD, with no object preselection or correction.
        #   Triggers are also applied there.
        # 2) Objects are corrected and selected and a list of *preselection* cuts are applied to skim the dataset.
        # 3) A list of cut function is applied and the masks are kept in memory to defined later "categories"
        self._skim = self.cfg.skim
        self._preselections = self.cfg.preselections
        # The categories objects handles a generator of categories and masks to be applied
        self._categories = self.cfg.categories

        # Subsamples configurations: special cuts to split a sample in subsamples
        self._subsamples = self.cfg.subsamples
        self._columns = self.cfg.columns

        # Weights configuration
        self.weights_config_allsamples = self.cfg.weights_config
        self.weights_classes = self.cfg.weights_classes

        # Load the jet calibration factory once for all chunks
        self.jmefactory = load_jet_factory(self.params)

        # Custom axis for the histograms
        self.custom_axes = []
        self.custom_histogram_fields = {}

        # Output format
        # Accumulators for the output
        self.output_format = {
            "sum_genweights": {},
            "sum_signOf_genweights": {},
            "sumw": {
                cat: {} for cat in self._categories
            },
            "sumw2": {
                cat: {} for cat in self._categories
            },
            "cutflow": {
                "initial": {},
                "skim": {},
                "presel": {},
                **{cat: {} for cat in self._categories},
            },
            "variables": {
                v: defaultdict(dict)
                for v, vcfg in self.cfg.variables.items()
                if not vcfg.metadata_hist
            },
            "columns": {},
            "processing_metadata": {
                v: defaultdict(dict)
                for v, vcfg in self.cfg.variables.items()
                if vcfg.metadata_hist
            },
            "datasets_metadata":{ } #year:sample:subsample
        }


    @property
    def nevents(self):
        '''Compute the current number of events in the current chunk.
        If the function is called after skimming or preselection the
        number of events is reduced accordingly'''
        return len(self.events)

    def load_metadata(self):
        '''
        The function is called at the beginning of the processing for each chunk
        to load some metadata depending on the chunk sample, year and dataset.

        - `_dataset`: name assigned by the user to the fileset configuration: it identifies
                      in a unique way the output and the source of the events.
        - `_sample`: category of the events, used in the processing to parametrize things
        - `_samplePart`: subcategory of the events, used in the processing to parametrize things
        '''
        self._dataset = self.events.metadata["dataset"] # this is the name given to the fileset
        self._sample = self.events.metadata["sample"] # this is the name of the sample
        self._samplePart = self.events.metadata.get("part", None) # this is the (optional) name of the part of the sample

        self._year = self.events.metadata["year"]
        self._isMC = ((self.events.metadata["isMC"] in ["True", "true"])
                      or (self.events.metadata["isMC"] == True))
        # if the dataset is a skim the sumgenweights are scaled by the skim efficiency
        self._isSkim = ("isSkim" in self.events.metadata and self.events.metadata["isSkim"] in ["True","true"]) or(
            "isSkim" in self.events.metadata and self.events.metadata["isSkim"] == True)
        # for some reason this get to a string WIP
        if self._isMC:
            self._era = "MC"
            self._xsec = self.events.metadata["xsec"]
        else:
            self._era = self.events.metadata["era"]
        # Loading metadata for subsamples
        self._hasSubsamples = self.cfg.has_subsamples[self._sample]
        # Store all metadata in a single dict for easier access
        self._metadata = {
            "year": self._year,
            "sample": self._sample,
            "samplePart": self._samplePart,
            "dataset": self._dataset,
            "isMC": self._isMC,
            "isSkim": self._isSkim,
            "era": self._era,
        }

    def load_metadata_extra(self):
        '''
        Function that can be called by a derived processor to define additional metadata.
        For example load additional information for a specific sample.
        '''
        pass

    def skim_events(self):
        '''
        Function which applied the initial event skimming.
        By default the skimming does not comprehend cuts. 

        BE CAREFUL: the skimming is done before any object preselection and cleaning.
        Only collections and branches already present in the NanoAOD before any corrections
        can be used.
        Alternatively, if you need to apply the cut on preselected objects -
        define the cut at the preselection level, not at skim level.
        '''
        self._skim_masks = PackedSelection()

        for skim_func in self._skim:
            # Apply the skim function and add it to the mask
            mask = skim_func.get_mask(
                self.events,
                processor_params=self.params,
                year=self._year,
                sample=self._sample,
                isMC=self._isMC,
            )
            self._skim_masks.add(skim_func.id, mask)

        # Finally we skim the events and count them
        self.events = self.events[self._skim_masks.all(*self._skim_masks.names)]
        self.nEvents_after_skim = self.nevents
        self.output['cutflow']['skim'][self._dataset] = self.nEvents_after_skim
        self.has_events = self.nEvents_after_skim > 0

    def export_skimmed_chunk(self):
        ''' Function that export the skimmed chunk to a new ROOT file.
        It rescales the genweight so that the processing on the skimmed file respects
        the original cross section. '''
        # Save the rescaling of the genweight to get the original cross section
        if self._isMC:
            #sumgenweight after the skimming
            skimmed_sumw = ak.sum(self.events.genWeight)
            # the scaling factor is the original sumgenweight / the skimmed sumgenweight
            if skimmed_sumw == 0:
                self.events["skimRescaleGenWeight"] = np.zeros(self.nEvents_after_skim)
            else:
                self.events["skimRescaleGenWeight"] =  np.ones(self.nEvents_after_skim) * self.output['sum_genweights'][self._dataset] / skimmed_sumw
            self.output['sum_genweights_skimmed'] = { self._dataset : skimmed_sumw }
        
        filename = (
            "__".join(
                [
                    self._dataset,
                    self.events.metadata['fileuuid'],
                    str(self.events.metadata['entrystart']),
                    str(self.events.metadata['entrystop']),
                ]
            )
            + ".root"
        )
        with uproot.recreate(f"{filename}", compression=uproot.ZSTD(5)) as fout:
            fout["Events"] = uproot_writeable(self.events)
        # copy the file
        copy_file(
            filename, "./", self.cfg.save_skimmed_files_folder, subdirs=[self._dataset]
        )
        # save the new file location for the new dataset definition
        self.output["skimmed_files"] = {
            self._dataset: [
                os.path.join(self.cfg.save_skimmed_files_folder, self._dataset, filename)
            ]
        }
        self.output["nskimmed_events"] = {self._dataset: [self.nEvents_after_skim]}

    @abstractmethod
    def apply_object_preselection(self, variation):
        '''
        Function which **must** be defined by the actual user processor to preselect
        and clean objects and define the collections as attributes of `events`.
        E.g.::

             self.events["ElectronGood"] = lepton_selection(self.events, "Electron", self.params)
        '''
        pass

    @abstractmethod
    def count_objects(self, variation):
        '''
        Function that counts the preselected objects and save the counts as attributes of `events`.
        The function **must** be defined by the user processor.
        '''
        pass

    def apply_preselections(self, variation):
        '''The function computes all the masks from the preselection cuts
        and filter out the events to speed up the later computations.
        N.B.: Preselection happens after the objects correction and cleaning.'''

        # The preselection mask is applied after the objects have been corrected
        self._preselection_masks = PackedSelection()

        for cut in self._preselections:
            # Apply the cut function and add it to the mask
            mask = cut.get_mask(
                self.events,
                processor_params=self.params,
                year=self._year,
                sample=self._sample,
                isMC=self._isMC,
            )
            self._preselection_masks.add(cut.id, mask)
        # Now that the preselection mask is complete we can apply it to events
        self.events = self.events[
            self._preselection_masks.all(*self._preselection_masks.names)
        ]
        self.nEvents_after_presel = self.nevents
        if variation == "nominal":
            self.output['cutflow']['presel'][self._dataset] = self.nEvents_after_presel
        self.has_events = self.nEvents_after_presel > 0

    def define_categories(self, variation):
        '''
        The function saves all the cut masks internally, in order to use them later
        to define categories (groups of cuts.).

        The categorization objects takes care of the details of the caching of the mask
        and expose a common interface.

        Moreover it computes the cut masks defining the subsamples for the current
        chunks and store them in the `self.subsamples` attribute for later use.
        '''

        # We make sure that for each category the list of cuts is unique in the Configurator validation
        self._categories.prepare(
            events=self.events,
            processor_params=self.params,
            year=self._year,
            sample=self._sample,
            isMC=self._isMC,
        )

        self._subsamples[self._sample].prepare(
            events=self.events,
            processor_params=self.params,
            year=self._year,
            sample=self._sample,
            isMC=self._isMC,
        )

    def define_common_variables_before_presel(self, variation):
        '''
        Function that defines common variables employed in analyses
        and save them as attributes of `events`, **before preselection**.
        If the user processor does not redefine it, no common variables are built.
        '''
        pass

    def define_common_variables_after_presel(self, variation):
        '''
        Function that defines common variables employed in analyses
        and save them as attributes of `events`,  **after preselection**.
        If the user processor does not redefine it, no common variables are built.
        '''
        pass

    def define_weights(self):
        '''
        The WeightsManager is build only for MC, not for data.
        The user processor can redefine this function to customize the
        weights computation object.
        The WeightManager loads the weights configuration and precomputed
        the available weights variations for the current chunk.
        The Histmanager will ask the WeightsManager to have the available weights
        variations to create histograms axis.
        '''
        # Creating the WeightsManager with all the configured weights
        self.weights_manager = WeightsManager(
            self.params,
            self.weights_config_allsamples[self._sample],
            self.weights_classes,
            storeIndividual=False,
            metadata={
                "year": self._year,
                "sample": self._sample,
                "dataset": self._dataset,
                "part": self._samplePart,
                "xsec": self._xsec if self._isMC else None,
                "isMC": self._isMC,
            }
        )
    
    def compute_weights(self, variation):
        '''
        Function which computed the weights (called after preselection).
        The current shape variation is passed to be used for the weights
        calculation. 
        '''
        # Compute the weights
        self.weights_manager.compute(self.events,
                                     size=self.nEvents_after_presel,
                                     shape_variation=variation)

    def compute_weights_extra(self, variation):
        '''Function that can be defined by user processors
        to define **additional weights** to be added to the WeightsManager.
        To completely redefine the WeightsManager, use the function `compute_weights`
        '''
        pass

    def count_events(self, variation):
        '''
        Count the number of events in each category and
        also sum their nominal weights (for each sample, by chunk).
        Store the results in the `cutflow` and `sumw` outputs
        '''
        for category, mask in self._categories.get_masks():
            if self._categories.is_multidim and mask.ndim > 1:
                # The Selection object can be multidim but returning some mask 1-d
                # For example the CartesianSelection may have a non multidim StandardSelection
                mask_on_events = ak.any(mask, axis=1)
            else:
                mask_on_events = mask

            self.output["cutflow"][category][self._dataset] = {self._sample: ak.sum(mask_on_events)}
            if self._isMC:
                w = self.weights_manager.get_weight(category)
                self.output["sumw"][category][self._dataset] = {self._sample: ak.sum(w * mask_on_events)}
                self.output["sumw2"][category][self._dataset] = {self._sample: ak.sum((w**2) * mask_on_events)}

            # If subsamples are defined we also save their metadata
            if self._hasSubsamples:
                for subs, subsam_mask in self._subsamples[self._sample].get_masks():
                    # get the subsample specific weight
                    mask_withsub = mask_on_events & subsam_mask
                    self.output["cutflow"][category][self._dataset][f"{self._sample}__{subs}"] = ak.sum(mask_withsub)
                    if self._isMC:
                        w_tot = w * self.weights_manager.get_weight_only_subsample(subsample=f"{self._sample}__{subs}",
                                                                                   category=category)
                        self.output["sumw"][category][self._dataset][f"{self._sample}__{subs}"] = ak.sum(w_tot * mask_withsub)
                        self.output["sumw2"][category][self._dataset][f"{self._sample}__{subs}"] = ak.sum(((w_tot)**2) * mask_withsub)


    def define_custom_axes_extra(self):
        '''
        Function which get called before the definition of the Histogram
        manager.
        It is used to defined extra custom axes for the histograms
        depending on the current chunk metadata.
        E.g.: it can be used to add a `era` axes only for data.

        Custom axes needed for all the samples can be added in the
        user processor constructor, by appending to `self.custom_axes`.
        '''
        pass

    def define_histograms(self):
        '''
        Initialize the HistManager.
        Redefine to customize completely the creation of the histManager.
        Only one HistManager is created for all the subsamples.
        The subsamples masks are passed to `fill_histogram` and used internally.
        '''
        self.hists_manager = HistManager(
            self.cfg.variables,
            self._year,
            self._sample,
            self._hasSubsamples,
            self._subsamples[self._sample].keys(),
            self._categories,
            variations_config=self.cfg.variations_config[self._sample] if self._isMC else None,
            processor_params=self.params,
            weights_manager=self.weights_manager,
            calibrators_manager=self.calibrators_manager,
            custom_axes=self.custom_axes,
            isMC=self._isMC,
        )

    def define_histograms_extra(self):
        '''
        Function that get called after the creation of the HistManager.
        The user can redefine this function to manipulate the HistManager
        histogram configuration to add customizations directly to the histogram
        objects before the filling.

        This function should also be redefined to fill the `self.custom_histogram_fields`
        that are passed to the histogram filling.
        '''
        pass

    def fill_histograms(self, variation):
        '''Function which fill the histograms for each category and variation,
        throught the HistManager.
        '''
        # Filling the autofill=True histogram automatically
        # Calling hist manager with the subsample masks
        self.hists_manager.fill_histograms(
            self.events,
            self._categories,
            subsamples=self._subsamples[self._sample],
            shape_variation=variation,
            custom_fields=self.custom_histogram_fields,
        )
        # Saving the output for each sample/subsample
        for subs in self._subsamples[self._sample].keys():
            # When we loop on all the subsample we need to format correctly the output if
            # there are no subsamples
            if self._hasSubsamples:
                name = f"{self._sample}__{subs}"
            else:
                name = self._sample
            for var, H in self.hists_manager.get_histograms(subs).items():
                self.output["variables"][var][name][self._dataset] = H


    def fill_histograms_extra(self, variation):
        '''
        The function get called after the filling of the default histograms.
        Redefine it to fill custom histograms
        '''
        pass

    def define_column_accumulators(self):
        '''
        Define the ColumnsManagers to handle the requested columns from the configuration.
        If Subsamples are defined a columnsmanager is created for each of them.
        '''
        self.column_managers = {}
        for subs in self._subsamples[self._sample].keys():
            if self._hasSubsamples:
                name = f"{self._sample}__{subs}"
            else:
                name = self._sample

            if name in self._columns:
                self.column_managers[subs] = ColumnsManager(
                    self._columns[name], self._categories
                )

    def define_column_accumulators_extra(self):
        '''
        This function should be redefined to add column accumulators in the custom
        processor, if they cannot be defined from the configuration
        '''

    def fill_column_accumulators(self, variation):
        if variation != "nominal":
            return

        if len(self.column_managers) == 0:
            return

        outcols = self.output["columns"]
        # TODO Fill column accumulator for different variations
        if self._hasSubsamples:
            # call the filling for each
            for subs in self._subsamples[self._sample].keys():
                if self.workflow_options!=None and self.workflow_options.get("dump_columns_as_arrays_per_chunk", None)!=None:
                    # filling awkward arrays to be dumped per chunk
                    if self.column_managers[subs].ncols == 0: break
                    out_arrays = self.column_managers[subs].fill_ak_arrays(
                                               self.events,
                                               self._categories,
                                               subsample_mask=self._subsamples[self._sample].get_mask(subs),
                                               weights_manager=self.weights_manager
                                               )
                    fname = (self.events.behavior["__events_factory__"]._partition_key.replace( "/", "_" )
                        + ".parquet")
                    for category, akarr in out_arrays.items():
                        # building the file name
                        subdirs = [self._dataset, subs, category]
                        dump_ak_array(akarr, fname, self.workflow_options["dump_columns_as_arrays_per_chunk"]+"/", subdirs)


                else:
                    # Filling columns to be accumulated for all the chunks
                    # Calling hist manager with a subsample mask
                    if self.column_managers[subs].ncols == 0: break
                    self.output["columns"][f"{self._sample}__{subs}"]= {
                        self._dataset : self.column_managers[subs].fill_columns_accumulators(
                                                   self.events,
                                                   self._categories,
                                                   subsample_mask=self._subsamples[self._sample].get_mask(subs),
                                                   weights_manager=self.weights_manager
                                                   )
                    }
        else:
            # NO subsamples
            if self.column_managers[self._sample].ncols == 0: return
            if self.workflow_options!=None and self.workflow_options.get("dump_columns_as_arrays_per_chunk", None)!=None:
                out_arrays = self.column_managers[self._sample].fill_ak_arrays(
                                               self.events,
                                               self._categories,
                                               subsample_mask=None,
                                               weights_manager=self.weights_manager
                                               )
                # building the file name
                fname = (self.events.behavior["__events_factory__"]._partition_key.replace( "/", "_" )
                         + ".parquet")
                for category, akarr in out_arrays.items():
                    subdirs = [self._dataset, category]
                    dump_ak_array(akarr, fname, self.workflow_options["dump_columns_as_arrays_per_chunk"]+"/", subdirs)
            else:
                self.output["columns"][self._sample] = { self._dataset: self.column_managers[
                    self._sample
                ].fill_columns_accumulators(
                    self.events,
                    self._categories,
                    subsample_mask = None,
                    weights_manager=self.weights_manager
                ) }

    def fill_column_accumulators_extra(self, variation):
        pass

    def process_extra_before_skim(self):
        pass

    def process_extra_after_skim(self):
        pass

    def process_extra_before_presel(self, variation):
        pass

    def process_extra_after_presel(self, variation):
        pass

    def save_processing_metadata(self):
        # Filling the special histograms for processing metadata if they are present
        total_processing_time = self.stop_time - self.start_time # in seconds
        add_axes = {"variation":"nominal"} if self._isMC else {}  
        if self._hasSubsamples:
            for subs in self._subsamples[self._sample].keys():
                for k, n in zip(["initial", "skim","presel"],
                                [self.nEvents_initial, self.nEvents_after_skim, self.nEvents_after_presel ]):
                    if hepc := self.hists_manager.get_histogram(subs, f"events_per_chunk_{k}"):
                        hepc.hist_obj.fill(
                            cat=hepc.only_categories[0],
                            nevents=n,
                            **add_axes
                        )
                        self.output["processing_metadata"][f"events_per_chunk_{k}"][
                            f"{self._sample}__{subs}"][self._dataset] = hepc.hist_obj
                        
                    if hepc := self.hists_manager.get_histogram(subs, f"throughput_per_chunk_{k}"):
                        hepc.hist_obj.fill(
                            cat=hepc.only_categories[0],
                            throughput=n/total_processing_time,
                            **add_axes
                        )
                        self.output["processing_metadata"][f"throughput_per_chunk_{k}"][
                            f"{self._sample}__{subs}"][self._dataset] = hepc.hist_obj
        else:
            for k, n in zip(["initial", "skim","presel"],
                            [self.nEvents_initial, self.nEvents_after_skim, self.nEvents_after_presel ]):
                if hepc := self.hists_manager.get_histogram(self._sample, f"events_per_chunk_{k}"):
                    hepc.hist_obj.fill(
                        cat=hepc.only_categories[0],
                        nevents=n,
                        **add_axes
                    )
                    self.output["processing_metadata"][f"events_per_chunk_{k}"][
                        self._sample][self._dataset] = hepc.hist_obj

                if hepc := self.hists_manager.get_histogram(self._sample, f"throughput_per_chunk_{k}"):
                    hepc.hist_obj.fill(
                        cat=hepc.only_categories[0],
                        throughput=n/total_processing_time,
                        **add_axes
                    )
                    self.output["processing_metadata"][f"throughput_per_chunk_{k}"][
                        self._sample][self._dataset] = hepc.hist_obj


    def initialize_calibrators(self):
        '''Creates the calibator manager and initialize all the calibrators.
        This prepares also the list of avaialable shape variations for this chunk.
        That will be utilized by the HistManager to create the histograms variations axes.'''
        self.calibrators_manager = CalibratorsManager(
            self.cfg.calibrators,
            self.events,
            self.params,
            self._metadata,
            requested_calibrator_variations=self.cfg.available_shape_variations[self._sample],
            # Additional arg to pass the jmefactory to the jet calibrator --> hacky
            jme_factory=self.jmefactory,
        )

    def loop_over_variations(self):
        # Get the requested shape variations by calibrator
        for variation, events_calibrated in self.calibrators_manager.calibration_loop(
            self.events,
            # Running only the shape variations activated in the configuration
            # for the current sample. 
            # The shape variations are defined by the calibrator name 
            variations_for_calibrators=self.cfg.available_shape_variations[self._sample],
            debug=self.workflow_options.get("debug_calibrators", False),
        ):
            # We need to set the events to the calibrated ones
            # and call the function to apply the preselection
            self.events = events_calibrated
            yield variation
        

    def process(self, events: ak.Array):
        '''
        This function get called by Coffea on each chunk of NanoAOD file.
        The processing steps of PocketCoffea are defined in this function.

        Customization points for user-defined processor are provided as
        `_extra` functions. By redefining those functions the user can change the behaviour
        of the processor in some predefined points.

        The processing goes through the following steps:

          - load metadata
          - Skim events (first masking of events):
              HLT triggers should be applied here, but their use is left to the configuration,
              and not hardcoded in the processor.
          - apply object preselections
          - count objects
          - apply event preselection (secondo masking of events)
          - define categories
          - define weights
          - define histograms
          - count events in each category
        '''
        self.start_time = time.time()
        self.events = events
        # Define the accumulator instance for this chunk
        self.output = copy.deepcopy(self.output_format)

        ###################
        # At the beginning of the processing the initial number of events
        # and the sum of the genweights is stored for later use
        #################
        self.load_metadata()
        self.load_metadata_extra()

        self.nEvents_initial = self.nevents
        self.output['cutflow']['initial'][self._dataset] = self.nEvents_initial
        if self._isMC:
            # This is computed before any preselection
            if not self._isSkim:
                self.output['sum_genweights'][self._dataset] = ak.sum(self.events.genWeight)
            else:
                # If the dataset is a skim, the sumgenweights are rescaled
                self.output['sum_genweights'][self._dataset] = ak.sum(self.events.skimRescaleGenWeight * self.events.genWeight)
            #FIXME: handle correctly the skim for the sum_signOf_genweights
            self.output['sum_signOf_genweights'][self._dataset] = ak.sum(np.sign(self.events.genWeight))
                
        ########################
        # Then the first skimming happens.
        # Events that are for sure useless are removed.
        # The user can specify in the configuration a function to apply for the skiming.
        # BE CAREFUL: objects are not corrected and cleaned at this stage, the skimming
        # selections MUST be loose and inclusive w.r.t the final selections.
        #########################
        # Customization point for derived workflows before skimming
        self.process_extra_before_skim()
        # MET filter, lumimask, + custom skimming function
        self.skim_events()
        if not self.has_events:
            return self.output

        if self.cfg.save_skimmed_files:
            self.export_skimmed_chunk()
            return self.output

        #########################
        # After the skimming we apply the object corrections and preselection
        # Doing so we avoid to compute them on the full NanoAOD dataset
        #########################

        self.process_extra_after_skim()
        # Define and load the calibators
        self.initialize_calibrators()
        # Define and load the weights manager
        self.define_weights()
        # Create the HistManager and ColumnManager before systematic variations
        self.define_custom_axes_extra()
        self.define_histograms()
        self.define_histograms_extra()
        self.define_column_accumulators()
        self.define_column_accumulators_extra()

        for variation in self.loop_over_variations():
            # Apply preselections
            self.apply_object_preselection(variation)
            self.count_objects(variation)
            # Compute variables after object preselection
            self.define_common_variables_before_presel(variation)
            # Customization point for derived workflows after preselection cuts
            self.process_extra_before_presel(variation)

            # This will remove all the events not passing preselection
            # from further processing
            self.apply_preselections(variation)

            # If not events remains after the preselection we skip the chunk
            if not self.has_events:
                continue

            ##########################
            # After the preselection cuts has been applied more processing is performend
            ##########################
            # Customization point for derived workflows after preselection cuts
            self.define_common_variables_after_presel(variation)
            self.process_extra_after_presel(variation)

            # This function applies all the cut functions in the cfg file
            # Each category is an AND of some cuts.
            self.define_categories(variation)

            # Weights
            self.compute_weights(variation)
            self.compute_weights_extra(variation)

            # Fill histograms
            self.fill_histograms(variation)
            self.fill_histograms_extra(variation)
            self.fill_column_accumulators(variation)
            self.fill_column_accumulators_extra(variation)

            # Count events
            if variation == "nominal":
                self.count_events(variation)

        self.stop_time = time.time()
        self.save_processing_metadata()
        return self.output


    def rescale_sumgenweights(self, output):
        # rescale each variable
        for var, vardata in output["variables"].items():
            for samplename, dataset_in_sample in vardata.items():
                for dataset, histo in dataset_in_sample.items():
                    # First, determine whether we must use the sum_signOf_genweights or sum_genweights for rescaling.
                    # This information is taken from a weights config file for each _sample_
                    # Getting the original sample name to check weights config
                    sample = self.cfg.subsamples_reversed_map[samplename] #needed for subsamples
                    if not self.cfg.samples_metadata[sample]['isMC']:
                        continue
                    wei = self.cfg.weights_config[sample]['inclusive']
                    rescale = True
                    if 'signOf_genWeight' in wei and 'genWeight' not in wei:
                        sumgenw_dict = output["sum_signOf_genweights"]
                    elif "genWeight" in wei and "signOf_genWeight" not in wei:
                        sumgenw_dict = output["sum_genweights"]
                    elif "genWeight" in wei and "signOf_genWeight" in wei:
                        print("!!! Both genWeight and signOf_genWeight are present in the weights config. This is not allowed.")
                        print("Using sum_genweights for rescaling by default")
                        sumgenw_dict = output["sum_genweights"]
                    else:
                        rescale = False
                    
                    if rescale and dataset in sumgenw_dict:
                        scaling = 1/sumgenw_dict[dataset]
                        # it  means that's a MC sample
                        histo *= scaling

        # rescale sumw
        for cat, catdata in output["sumw"].items():
            for dataset, dataset_data in catdata.items():
                # Getting the first sample for the dataset in the "sumw" output
                # it is working also for subsamples before the first sample key is the primary sample
                sample_from_dataset = list(dataset_data.keys())[0]
                if not self.cfg.samples_metadata[sample_from_dataset]['isMC']:
                    continue
                wei = self.cfg.weights_config[sample_from_dataset]['inclusive']
                rescale = True
                if 'signOf_genWeight' in wei and 'genWeight' not in wei:
                    sumgenw_dict = output["sum_signOf_genweights"]
                elif "genWeight" in wei and "signOf_genWeight" not in wei:
                    sumgenw_dict = output["sum_genweights"]
                elif "genWeight" in wei and "signOf_genWeight" in wei:
                    print("!!! Both genWeight and signOf_genWeight are present in the weights config. This is not allowed.")
                    print("Using sum_genweights for rescaling by default")
                    sumgenw_dict = output["sum_genweights"]
                else:
                    rescale = False
                    
                if rescale and dataset in sumgenw_dict:
                    scaling = 1/sumgenw_dict[dataset]
                    for sample in dataset_data.keys():
                        dataset_data[sample] *= scaling

        # rescale sumw2
        for cat, catdata in output["sumw2"].items():
            for dataset, dataset_data in catdata.items():
                sample_from_dataset = list(dataset_data.keys())[0]
                if not self.cfg.samples_metadata[sample_from_dataset]['isMC']:
                    continue
                wei = self.cfg.weights_config[sample_from_dataset]['inclusive']
                rescale = True
                if 'signOf_genWeight' in wei and 'genWeight' not in wei:
                    sumgenw_dict = output["sum_signOf_genweights"]
                elif "genWeight" in wei and "signOf_genWeight" not in wei:
                    sumgenw_dict = output["sum_genweights"]
                elif "genWeight" in wei and "signOf_genWeight" in wei:
                    print("!!! Both genWeight and signOf_genWeight are present in the weights config. This is not allowed.")
                    print("Using sum_genweights for rescaling by default")
                    sumgenw_dict = output["sum_genweights"]
                else:
                    rescale = False
                     
                if rescale and dataset in sumgenw_dict:
                    scaling = 1/sumgenw_dict[dataset]**2
                    for sample in dataset_data.keys():
                        dataset_data[sample] *= scaling


    def postprocess(self, accumulator):
        '''
        The function is called by coffea at the end of the processing.
        The default function calls the `rescale_sumgenweights` function to rescale the histograms
        and `sumw` metadata using the sum of the genweights computed without preselections
        for each dataset.

        Moreover the function saves in the output a dictionary of metadata
        with the full description of the datasets taken from the configuration.

        To add additional customatizaion redefine the `postprocessing` function,
        but remember to include a super().postprocess() call.
        '''
        
        if not self.cfg.do_postprocessing:
            return accumulator

        
        # Saving dataset metadata directly in the output file reading from the config
        dmeta = accumulator["datasets_metadata"] = {
            "by_datataking_period": {},
            "by_dataset": defaultdict(dict)
        }

        for dataset in accumulator["cutflow"]["initial"].keys():
            df = self.cfg.filesets[dataset]
            #copying the full metadata of the used samples in the output per direct reference
            dmeta["by_dataset"][dataset] = df["metadata"]
            # now adding by datataking period
            sample = df["metadata"]["sample"]
            year = df["metadata"]["year"]
            if year not in dmeta["by_datataking_period"]:
                dmeta["by_datataking_period"][year] = defaultdict(set)

            if self.cfg.has_subsamples[sample]:
                for subsam in self._subsamples[sample].keys():
                    dmeta["by_datataking_period"][year][f"{sample}__{subsam}"].add(dataset)
            else:
                dmeta["by_datataking_period"][year][sample].add(dataset)

        # Rescale the histograms and sumw using the sum of the genweights
        if not self.workflow_options.get("donotscale_sumgenweights", False):
            self.rescale_sumgenweights(accumulator)
        # Check if histograms have any nan value
        for var, vardata in accumulator["variables"].items():
            for samplename, dataset_in_sample in vardata.items():
                for dataset, histo in dataset_in_sample.items():
                    if any(np.isnan(histo.values().flatten())):
                        raise Exception(
                            f"NaN values in the histogram {var} for dataset {dataset} after rescaling"
                        )

        return accumulator
