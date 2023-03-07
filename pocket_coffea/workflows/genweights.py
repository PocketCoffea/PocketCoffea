import awkward as ak

import copy

from .base import BaseProcessorABC
from ..utils.configurator import Configurator

class genWeightsProcessor(BaseProcessorABC):
    def __init__(self, cfg: Configurator) -> None:
        super().__init__(cfg)
        self.output_format = {
            "sum_genweights": {},
            "sumw": {
                cat: {} for cat in self._categories
            },
            "cutflow": {
                "initial": {},
                "skim": {},
                "presel": {},
                **{cat: {} for cat in self._categories},
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

    def apply_object_preselection(self, variation):
        pass

    def count_objects(self, variation):
        pass

    def process(self, events: ak.Array):
        self.events = events

        self.load_metadata()
        self.output = copy.deepcopy(self.output_format)

        self.nEvents_initial = self.nevents
        #self.output['cutflow']['initial'][self._dataset] += self.nEvents_initial
        if self._isMC:
            self.output['sum_genweights'][self._dataset] = ak.sum(self.events.genWeight)
            if self._hasSubsamples:
                for subs in self._subsamples_names:
                    self.output['sum_genweights'][subs] = self.output['sum_genweights'][self._sample]

        return self.output
