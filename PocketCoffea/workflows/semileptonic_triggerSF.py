from coffea import hist, processor

from ..workflows.base import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object_with_variations

class semileptonicTriggerProcessor(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)
        pileup_axis   = hist.Cat("pileup", "Pileup")
        for histname, h in self._hist_dict.items():
            varname = h.fields[-1]
            varlabel = h.axis(varname).label
            self._hist_dict[histname] = hist.Hist(varlabel, *h.axes(), pileup_axis)
        self._accum_dict.update(self._hist_dict)
        self._accumulator = processor.dict_accumulator(self._accum_dict)

    def postprocess(self, accumulator):
        super().__init__(accumulator=accumulator)

        nevts = [n_key for n_key in accumulator.keys() if n_key.startswith('nevts_cat_')]
        nevts_inclusive = 'nevts_cat_inclusive'
        for n_key in nevts:
            n_dict = accumulator[n_key]
            efficiency = sum(n_dict.values())

        return accumulator
