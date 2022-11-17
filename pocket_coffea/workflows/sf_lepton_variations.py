from coffea import hist, processor

from .tthbb_base_processor import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object_with_variations


class sfLeptonVariationsProcessor(ttHbbBaseProcessor):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)
        sf_ele_reco_axis = hist.Cat("sf_ele_reco", "SF (ele, reco)")
        sf_ele_id_axis = hist.Cat("sf_ele_id", "SF (ele, ID)")
        sf_mu_id_axis = hist.Cat("sf_mu_id", "SF (mu, ID)")
        sf_mu_iso_axis = hist.Cat("sf_mu_iso", "SF (mu, iso)")
        for histname, h in self._hist_dict.items():
            varname = h.fields[-1]
            self._hist_dict[histname] = hist.Hist(
                "$N_{events}$",
                *h.axes(),
                sf_ele_reco_axis,
                sf_ele_id_axis,
                sf_mu_id_axis,
                sf_mu_iso_axis
            )
        self._accum_dict.update(self._hist_dict)
        self._accumulator = processor.dict_accumulator(self._accum_dict)

    def fill_histograms(self):
        variations = ['sf_ele_reco', 'sf_ele_id', 'sf_mu_id', 'sf_mu_iso']
        for (obj, obj_hists) in zip([None], [self.nobj_hists]):
            fill_histograms_object_with_variations(
                self, obj, obj_hists, variations, event_var=True
            )
        for (obj, obj_hists) in zip(
            [self.events.MuonGood, self.events.ElectronGood, self.events.JetGood],
            [self.muon_hists, self.electron_hists, self.jet_hists],
        ):
            fill_histograms_object_with_variations(self, obj, obj_hists, variations)
