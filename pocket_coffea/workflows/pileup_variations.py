from coffea import hist, processor

from .tthbb_base_processor import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object_with_variations

class pileupVariationsProcessor(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)
        pileup_axis   = hist.Cat("pileup", "Pileup")
        for histname, h in self._hist_dict.items():
            varname = h.fields[-1]
            varlabel = h.axis(varname).label
            self._hist_dict[histname] = hist.Hist(varlabel, *h.axes(), pileup_axis)
        self._accum_dict.update(self._hist_dict)
        self._accumulator = processor.dict_accumulator(self._accum_dict)

    def fill_histograms(self):
        for (obj, obj_hists) in zip([None], [self.nobj_hists]):
            fill_histograms_object_with_variations(self, obj, obj_hists, 'pileup', event_var=True)
        for (obj, obj_hists) in zip([self.events.MuonGood, self.events.ElectronGood, self.events.JetGood], [self.muon_hists, self.electron_hists, self.jet_hists]):
            fill_histograms_object_with_variations(self, obj, obj_hists, 'pileup')
