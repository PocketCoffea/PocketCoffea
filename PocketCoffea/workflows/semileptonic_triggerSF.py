import sys
import awkward as ak

from coffea import hist, processor
from coffea.processor import dict_accumulator, defaultdict_accumulator
from coffea.analysis_tools import Weights

from ..workflows.base import ttHbbBaseProcessor
from ..lib.pileup import sf_pileup_reweight
from ..lib.scale_factors import sf_ele_reco, sf_ele_id, sf_mu
from ..lib.fill import fill_histograms_object_with_variations
from ..parameters.lumi import lumi
from ..parameters.samples import samples_info

class semileptonicTriggerProcessor(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)
        variations_axis   = hist.Cat("var", "Variations")
        for histname, h in self._hist_dict.items():
            self._hist_dict[histname] = hist.Hist("$N_{events}$", *h.sparse_axes(), variations_axis, *h.dense_axes())
        for histname, h in self._hist2d_dict.items():
            self._hist2d_dict[histname] = hist.Hist("$N_{events}$", *h.sparse_axes(), variations_axis, *h.dense_axes())
        # Accumulator with sum of weights of the events passing each category for each sample
        self._eff_dict = {cat: defaultdict_accumulator(float) for cat in self._categories}
        self._accum_dict["trigger_efficiency"] = dict_accumulator(self._eff_dict)
        # Add 2D histogram for trigger efficiency computation
        self.efficiency_maps = ["hist2d_electron_etaSC_vs_electron_pt", "hist2d_electron_phi_vs_electron_pt", "hist2d_electron_etaSC_vs_electron_phi"]
        self.electron_hists = self.electron_hists + self.efficiency_maps
        self._accum_dict.update(self._hist_dict)
        self._accum_dict.update(self._hist2d_dict)
        self._accumulator = processor.dict_accumulator(self._accum_dict)
        if self.cfg.triggerSF != None:
            self._apply_triggerSF = True
        else:
            self._apply_triggerSF = False
        self.define_triggerSF_maps()

    # Define trigger SF map to apply
    def define_triggerSF_maps(self):
        if self._apply_triggerSF:
            self._triggerSF_map = {}
            for category in self._categories.keys():
                if 'Ele32_EleHT_pass' in category:
                    self._triggerSF_map[category] = 'sf_nominal'
                elif category in ['Ele32_EleHT_fail', 'inclusive']:
                    self._triggerSF_map[category] = 'identity'
                else:
                    sys.exit(f"The association of the category '{category}' to a trigger SF key is ambiguous")
        else:
            return

    def fill_histograms(self):
        systematics = ['pileup', 'sf_ele_reco', 'sf_ele_id']
        for (obj, obj_hists) in zip([None], [self.perevent_hists]):
            fill_histograms_object_with_variations(self, obj, obj_hists, systematics, event_var=True, split_eras=self.cfg.split_eras, triggerSF=self._apply_triggerSF)
        for (obj, obj_hists) in zip([self.events.MuonGood, self.events.ElectronGood, self.events.JetGood], [self.muon_hists, self.electron_hists, self.jet_hists]):
            fill_histograms_object_with_variations(self, obj, obj_hists, systematics, split_eras=self.cfg.split_eras, triggerSF=self._apply_triggerSF)

    def postprocess(self, accumulator):
        super().postprocess(accumulator=accumulator)

        den_mc   = sum(accumulator["sumw"]["inclusive"].values())
        den_data = accumulator["cutflow"]["inclusive"]["DATA"]
        for category, cuts in self._categories.items():
            num_mc = sum(accumulator["sumw"][category].values())
            num_data = accumulator["cutflow"][category]["DATA"]
            eff_mc   = num_mc/den_mc
            eff_data = num_data/den_data
            accumulator["trigger_efficiency"][category] = {}
            accumulator["trigger_efficiency"][category]["mc"] = eff_mc
            accumulator["trigger_efficiency"][category]["data"] = eff_data
            accumulator["trigger_efficiency"][category]["sf"] = eff_data/eff_mc

        return accumulator
