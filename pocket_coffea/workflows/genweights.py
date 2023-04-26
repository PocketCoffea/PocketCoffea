import awkward as ak

import copy

from .base import BaseProcessorABC
from ..utils.configurator import Configurator


class genWeightsProcessor(BaseProcessorABC):
    def __init__(self, cfg: Configurator) -> None:
        super().__init__(cfg)
        self.output_format = {
            "sum_genweights": {},
            "cutflow": {"initial": {s: 0 for s in self.cfg.datasets}},
        }

    def load_metadata(self):
        self._dataset = self.events.metadata["dataset"]
        self._sample = self.events.metadata["sample"]
        self._year = self.events.metadata["year"]
        self._isMC = self.events.metadata["isMC"] == "True"
        if self._isMC:
            self._xsec = self.events.metadata["xsec"]

        # Check if the user specified any subsamples without performing any operation
        #if self._sample in self._subsamples:
        #    self._hasSubsamples = True
        #else:
        #    self._hasSubsamples = False

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
                raise Exception(
                    "This processor cannot compute the sum of genweights of subsamples."
                )

        return self.output
