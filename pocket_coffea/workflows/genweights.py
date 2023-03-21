import numpy as np
import awkward as ak

import copy

from coffea.lumi_tools import LumiMask
from coffea.analysis_tools import PackedSelection

from .base import BaseProcessorABC
from ..utils.configurator import Configurator
from ..parameters.event_flags import event_flags, event_flags_data
from ..parameters.lumi import goldenJSON
from ..parameters.btag import btag

class genWeightsProcessor(BaseProcessorABC):
    def __init__(self, cfg: Configurator) -> None:
        super().__init__(cfg)
        self.output_format = {
            "sum_genweights": {},
            "cutflow": {
                "initial": {s: 0 for s in self.cfg.datasets},
                "skim": {s: 0 for s in self.cfg.datasets}
            }
        }

    def load_metadata(self):
        self._dataset = self.events.metadata["dataset"]
        self._sample = self.events.metadata["sample"]
        self._year = self.events.metadata["year"]
        self._btag = btag[self._year]
        self._isMC = self.events.metadata["isMC"] == "True"
        if self._isMC:
            self._era = "MC"
            self._xsec = self.events.metadata["xsec"]
        else:
            self._era = self.events.metadata["era"]
            self._goldenJSON = goldenJSON[self._year]

        # Check if the user specified any subsamples without performing any operation
        if self._sample in self._subsamplesCfg:
            self._hasSubsamples = True
        else:
            self._hasSubsamples = False

    def skim_events(self):
        self._skim_masks = PackedSelection()
        mask_flags = np.ones(self.nEvents_initial, dtype=np.bool)
        flags = event_flags[self._year]
        if not self._isMC:
            flags += event_flags_data[self._year]
        for flag in flags:
            mask_flags = getattr(self.events.Flag, flag)
        self._skim_masks.add("event_flags", mask_flags)

        self._skim_masks.add("PVgood", self.events.PV.npvsGood > 0)

        if not self._isMC:
            mask_lumi = LumiMask(self._goldenJSON)(
                self.events.run, self.events.luminosityBlock
            )
            self._skim_masks.add("lumi_golden", mask_lumi)

        for skim_func in self._skim:
            mask = skim_func.get_mask(
                self.events,
                year=self._year,
                sample=self._sample,
                btag=self._btag,
                isMC=self._isMC,
            )
            self._skim_masks.add(skim_func.id, mask)

        self.events = self.events[self._skim_masks.all(*self._skim_masks.names)]
        self.nEvents_after_skim = self.nevents
        self.output['cutflow']['skim'][self._dataset] += self.nEvents_after_skim
        self.has_events = self.nEvents_after_skim > 0

    def apply_object_preselection(self, variation):
        pass

    def count_objects(self, variation):
        pass

    def process(self, events: ak.Array):
        self.events = events

        self.load_metadata()
        self.output = copy.deepcopy(self.output_format)

        self.nEvents_initial = self.nevents
        self.output['cutflow']['initial'][self._dataset] += self.nEvents_initial
        if self._isMC:
            self.output['sum_genweights'][self._dataset] = ak.sum(self.events.genWeight)
            if self._hasSubsamples:
                raise Exception("This processor cannot compute the sum of genweights of subsamples.")

        self.process_extra_before_skim()
        self.skim_events()
        if not self.has_events:
            return self.output

        if self.cfg.save_skimmed_files:
            self.export_skimmed_chunk()
            return self.output

        return self.output
