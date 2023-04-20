import numpy as np
import awkward as ak
import uproot
from collections import defaultdict
from abc import ABC, abstractmethod
import cachetools

import copy
import os
import logging

from coffea import processor
from coffea.lumi_tools import LumiMask
from coffea.analysis_tools import PackedSelection

from ..lib.weights_manager import WeightsManager
from ..lib.columns_manager import ColumnsManager
from ..lib.hist_manager import HistManager
from ..lib.jets import jet_correction, met_correction
from ..lib.categorization import CartesianSelection
from ..utils.skim import uproot_writeable, copy_file

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

        # Weights configuration
        self.weights_config_allsamples = self.cfg.weights_config

        # Custom axis for the histograms
        self.custom_axes = []
        self.custom_histogram_fields = {}

        # Output format
        # Accumulators for the output
        self.output_format = {
            "sum_genweights": {},
            "sumw": {
                cat: {s: 0.0 for s in self.cfg.samples} for cat in self._categories
            },
            "cutflow": {
                "initial": {s: 0 for s in self.cfg.samples},
                "skim": {s: 0 for s in self.cfg.samples},
                "presel": {s: 0 for s in self.cfg.samples},
                **{cat: {s: 0.0 for s in self.cfg.samples} for cat in self._categories},
            },
            "variables": {
                v: {}
                for v, vcfg in self.cfg.variables.items()
                if not vcfg.metadata_hist
            },
            "columns": {},
            "processing_metadata": {
                v: {} for v, vcfg in self.cfg.variables.items() if vcfg.metadata_hist
            },
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
        '''
        self._dataset = self.events.metadata["dataset"]
        self._sample = self.events.metadata["sample"]
        self._year = self.events.metadata["year"]
        self._isMC = self.events.metadata["isMC"] == "True"
        if self._isMC:
            self._era = "MC"
            self._xsec = self.events.metadata["xsec"]
            if "sum_genweights" in self.events.metadata:
                self._sum_genweights = self.events.metadata["sum_genweights"]
            else:
                raise Exception(f"The metadata dict of {self._dataset} has no key named `sum_genweights`.")
        else:
            self._era = self.events.metadata["era"]
            self._goldenJSON = goldenJSON[self._year]
        # Loading metadata for subsamples
        self._hasSubsamples = self.cfg.has_subsamples[self._sample]

    def load_metadata_extra(self):
        '''
        Function that can be called by a derived processor to define additional metadata.
        For example load additional information for a specific sample.
        '''
        pass

    def skim_events(self):
        '''
        Function which applied the initial event skimming.
        By default the skimming comprehend:

          - METfilters,
          - PV requirement *at least 1 good primary vertex
          - lumi-mask (for DATA): applied the goldenJson selection
          - requested HLT triggers (from configuration, not hardcoded in the processor)
          - **user-defined** skimming cuts

        BE CAREFUL: the skimming is done before any object preselection and cleaning.
        Only collections and branches already present in the NanoAOD before any correct
        can be used.
        Alternatively, if you need to apply the cut on preselected objects,
        defined the cut at the preselection level, not at skim level.
        '''
        self._skim_masks = PackedSelection()
        mask_flags = np.ones(self.nEvents_initial, dtype=np.bool)
        flags = self.params.event_flags[self._year]
        if not self._isMC:
            flags += self.params.event_flags_data[self._year]
        for flag in flags:
            mask_flags = getattr(self.events.Flag, flag)
        self._skim_masks.add("event_flags", mask_flags)

        # Primary vertex requirement
        self._skim_masks.add("PVgood", self.events.PV.npvsGood > 0)

        # In case of data: check if event is in golden lumi file
        if not self._isMC:
            # mask_lumi = lumimask(self.events.run, self.events.luminosityBlock)
            mask_lumi = LumiMask(self.params.lumi.goldenJSON)(
                self.events.run, self.events.luminosityBlock
            )
            self._skim_masks.add("lumi_golden", mask_lumi)

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
        self.output['cutflow']['skim'][self._sample] += self.nEvents_after_skim
        self.has_events = self.nEvents_after_skim > 0

    def export_skimmed_chunk(self):
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
        # TODO Generalize skimming output temporary location
        with uproot.recreate(f"/scratch/{filename}") as fout:
            fout["Events"] = uproot_writeable(self.events)
        # copy the file
        copy_file(
            filename, "/scratch", self.cfg.save_skimmed_files, subdirs=[self._dataset]
        )
        # save the new file location for the new dataset definition
        self.output["skimmed_files"] = {
            self._dataset: [
                os.path.join(self.cfg.save_skimmed_files, self._dataset, filename)
            ]
        }
        self.output["nskimmed_files"] = {self._dataset: [self.nEvents_after_skim]}

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
                self.events, processor_params=self.params,
                year=self._year, sample=self._sample, isMC=self._isMC,
            )
            self._preselection_masks.add(cut.id, mask)
        # Now that the preselection mask is complete we can apply it to events
        self.events = self.events[
            self._preselection_masks.all(*self._preselection_masks.names)
        ]
        self.nEvents_after_presel = self.nevents
        if variation == "nominal":
            self.output['cutflow']['presel'][self._sample] += self.nEvents_after_presel
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

    @classmethod
    def available_weights(cls):
        '''
        Identifiers of the weights available thorugh this processor.
        By default they are all the weights defined in the WeightsManager
        '''
        return WeightsManager.available_weights()

    def compute_weights(self, variation):
        '''
        Function which define weights (called after preselection).
        The WeightsManager is build only for MC, not for data.
        The user processor can redefine this function to customize the
        weights computation object.
        '''
        if not self._isMC:
            self.weights_manager = None
        else:
            # Creating the WeightsManager with all the configured weights
            self.weights_manager = WeightsManager(
                self.weights_config_allsamples[self._sample],
                self.nEvents_after_presel,
                self.events,  # to compute weights
                storeIndividual=False,
                shape_variation=variation,
                metadata={
                    "year": self._year,
                    "sample": self._sample,
                    "xsec": self._xsec,
                    "sum_genweights": self._sum_genweights,
                    "finalstate": self.cfg.finalstate,
                },
            )

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

            self.output["cutflow"][category][self._sample] = ak.sum(mask_on_events)
            if self._isMC:
                w = self.weights_manager.get_weight(category)
                self.output["sumw"][category][self._sample] = ak.sum(w * mask_on_events)

            # If subsamples are defined we also save their metadata
            if self._hasSubsamples:
                for subs, subsam_mask in self._subsamples[self._sample].get_masks():
                    mask_withsub = mask_on_events & subsam_mask
                    self.output["cutflow"][category][subs] = ak.sum(mask_withsub)
                    if self._isMC:
                        self.output["sumw"][category][subs] = ak.sum(w * mask_withsub)

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
        self.hists_managers = HistManager(
            self.cfg.variables,
            self._year,
            self._sample,
            self._subsamples[self._sample].keys(),
            self._categories,
            variations_config=self.cfg.variations_config[self._sample],
            processor_params=self.params,
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
        self.hists_managers.fill_histograms(
            self.events,
            self.weights_manager,
            self._categories,
            subsamples=self._subsamples[self._sample],
            shape_variation=variation,
            custom_fields=self.custom_histogram_fields,
        )
        # Saving the output
        for subs in self._subsamples[self._sample].keys():
            # Saving the output for each subsample
            for var, H in self.hists_managers.get_histograms(subs).items():
                self.output["variables"][var][subs] = H

    def fill_histograms_extra(self, variation):
        '''
        The function get called after the filling of the default histograms.
        Redefine it to fill custom histograms
        '''
        pass

    def define_column_accumulators(self):
        '''
        Define the ColumsManagers to handle the requested columns from the configuration.
        If Subsamples are defined a columnsmager is created for each of them.
        '''
        self.column_managers = {}
        for subs in self._subsamples[self._sample].keys():
            self.column_managers[subs] = ColumnsManager(
                self.cfg.columns[subs], subs, self._categories
            )

    def define_column_accumulators_extra(self):
        '''
        This function should be redefined to add column accumulators in the custom
        processor, if they cannot be defined from the configuration
        '''

    def fill_column_accumulators(self, variation):
        if variation != "nominal":
            return
        # TODO Fill column accumulator for different variations
        if self._hasSubsamples:
            # call the filling for each
            for subs in self._subsamples[self._sample].keys():
                # Calling hist manager with a subsample mask
                self.output["columns"][subs] = self.column_managers[subs].fill_columns(
                    self.events,
                    self._categories,
                    subsample_mask=self._subsamples[self._sample].get_mask(subs),
                )
        else:
            self.output["columns"][self._sample] = self.column_managers[
                self._sample
            ].fill_columns(
                self.events,
                self._categories,
            )

    def fill_column_accumulators_extra(self, variation):
        pass

    def process_extra_before_skim(self):
        pass

    def process_extra_before_presel(self, variation):
        pass

    def process_extra_after_presel(self, variation):
        pass

    @classmethod
    def available_variations(cls):
        '''
        Identifiers of the weights variabtions available thorugh this processor.
        By default they are all the weights defined in the WeightsManager
        '''
        vars = WeightsManager.available_variations()
        vars.update(
            [
                "JES_Total",
                'JES_FlavorQCD',
                'JES_RelativeBal',
                'JES_HF',
                'JES_BBEC1',
                'JES_EC2',
                'JES_Absolute',
                'JES_Absolute_2018',
                'JES_HF_2018',
                'JES_EC2_2018',
                'JES_RelativeSample_2018',
                'JES_BBEC1_2018',
                'JER',
            ]
        )
        return vars

    def get_shape_variations(self):
        '''
        Generator for shape variations.
        '''
        if not self._isMC:
            yield "nominal"
            return
        # nominal is assumed to be the first
        variations = ["nominal"] + self.cfg.available_shape_variations[self._sample]
        # TO be understood if a copy is needed
        # This is probably very wrong
        nominal_events = self.events

        hasJES = False
        # HACK: hasJER is set to True even if the JER is not included in the variations.
        # In the future, it is desirable to include a dedicated option in the config file to switch the nominal JER and JES on and off.
        hasJER = True
        for v in variations:
            if "JES" in v:
                hasJES = True
            if "JER" in v:
                hasJER = True

        if hasJES | hasJER:
            # correct the jets only once
            jec4_cache = cachetools.Cache(np.inf)
            jec8_cache = cachetools.Cache(np.inf)
            jets_with_JES = jet_correction(
                nominal_events,
                nominal_events.Jet,
                "AK4PFchs",
                self._year,
                jec4_cache,
                applyJER=hasJER,
                applyJESunc=hasJES,
            )
            # fatjets_with_JES = jet_correction(
            #     nominal_events,
            #     nominal_events.FatJet,
            #     "AK8PFPuppi",
            #     self._year,
            #     jec8_cache,
            #     applyJER=hasJER,
            #     applyJESunc=hasJES,
            # )
            #met_with_JES = met_correction(
            #    nominal_events.MET,
            #    jets_with_JES
            #)
        else:
            jets_with_JES = nominal_events.Jet
            fatjets_with_JES = nominal_events.FatJet
            #met_with_JES = nominal_events.MET

        for variation in variations:
            # Restore the nominal events record since for each variation
            # the event preselections are applied

            if variation == "nominal":
                self.events = nominal_events
                if hasJES | hasJER:
                    # put nominal shape
                    self.events["Jet"] = jets_with_JES
                    # self.events["FatJet"] = fatjets_with_JES
                    #self.events["MET"] = met_with_JES
                # Nominal is ASSUMED to be the first
                yield "nominal"
            elif ("JES" in variation) | ("JER" in variation):
                # JES_jes is the total. JES_[type] is for different variations
                self.events = nominal_events
                self.events["Jet"] = jets_with_JES[variation].up
                # self.events["FatJet"] = fatjets_with_JES[variation].up
                yield variation + "Up"
                # restore nominal before going to down
                self.events = nominal_events
                self.events["Jet"] = jets_with_JES[variation].down
                # self.events["FatJet"] = fatjets_with_JES[variation].down
                yield variation + "Down"

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

        self.events = events

        ###################
        # At the beginning of the processing the initial number of events
        # and the sum of the genweights is stored for later use
        #################
        self.load_metadata()
        self.load_metadata_extra()
        # Define the accumulator instance for this chunk
        self.output = copy.deepcopy(self.output_format)

        self.nEvents_initial = self.nevents
        self.output['cutflow']['initial'][self._sample] += self.nEvents_initial
        if self._isMC:
            # This is computed before any preselection
            self.output['sum_genweights'][self._sample] = ak.sum(self.events.genWeight)
            if self._hasSubsamples:
                for subs in self._subsamples[self._sample].keys():
                    self.output['sum_genweights'][subs] = self.output['sum_genweights'][
                        self._sample
                    ]

        self.weights_config = self.weights_config_allsamples[self._sample]
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

        # Create the HistManager and ColumnManager before systematic variations
        self.define_custom_axes_extra()
        self.define_histograms()
        self.define_histograms_extra()
        self.define_column_accumulators()
        self.define_column_accumulators_extra()

        for variation in self.get_shape_variations():
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

        return self.output

    def postprocess(self, accumulator):
        '''
        The function is called by coffea at the end of the processing.

        To add additional customatizaion redefine the `postprocessing` function,
        but remember to include a super().postprocess() call.
        '''

        return accumulator
