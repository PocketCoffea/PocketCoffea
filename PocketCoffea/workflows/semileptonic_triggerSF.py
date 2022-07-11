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
        pileup_axis   = hist.Cat("pileup", "Pileup")
        sf_ele_reco_axis = hist.Cat("sf_ele_reco", "SF (ele, reco)")
        sf_ele_id_axis   = hist.Cat("sf_ele_id", "SF (ele, ID)")
        for histname, h in self._hist_dict.items():
            self._hist_dict[histname] = hist.Hist("$N_{events}$", *h.axes(), pileup_axis, sf_ele_reco_axis, sf_ele_id_axis)
        for histname, h in self._hist2d_dict.items():
            self._hist2d_dict[histname] = hist.Hist("$N_{events}$", *h.axes(), pileup_axis, sf_ele_reco_axis, sf_ele_id_axis)
        # Accumulator with sum of weights of the events passing each category for each sample
        self._eff_dict = {cat: defaultdict_accumulator(float) for cat in self._categories}
        self._accum_dict["trigger_efficiency"] = dict_accumulator(self._eff_dict)
        # Add 2D histogram for trigger efficiency computation
        self.efficiency_maps = ["hist2d_electron_etaSC_vs_electron_pt", "hist2d_electron_phi_vs_electron_pt", "hist2d_electron_etaSC_vs_electron_phi"]
        self.electron_hists = self.electron_hists + self.efficiency_maps
        self._accum_dict.update(self._hist_dict)
        self._accum_dict.update(self._hist2d_dict)
        self._accumulator = processor.dict_accumulator(self._accum_dict)

    # Overwrite the method `compute_weights()` in order to take into account the weights of all the corrections previously applied
    # In this way, even if the method is modified in the base processor, only the weights reported here are used for the event reweighting.
    def compute_weights(self):
        self.weights = Weights(self.nevents)
        if self.isMC:
            self.weights.add('genWeight', self.events.genWeight)
            self.weights.add('lumi', ak.full_like(self.events.genWeight, lumi[self._year]))
            self.weights.add('XS', ak.full_like(self.events.genWeight, samples_info[self._sample]["XS"]))
            # Pileup reweighting with nominal, up and down variations
            self.weights.add('pileup', *sf_pileup_reweight(self.events, self._year))
            # Electron reco and id SF with nominal, up and down variations
            self.weights.add('sf_ele_reco', *sf_ele_reco(self.events, self._year))
            self.weights.add('sf_ele_id',   *sf_ele_id(self.events, self._year))
            # Muon id and iso SF with nominal, up and down variations
            self.weights.add('sf_mu_id',  *sf_mu(self.events, self._year, 'id'))
            self.weights.add('sf_mu_iso', *sf_mu(self.events, self._year, 'iso'))

    def fill_histograms(self):
        variations = ['pileup', 'sf_ele_reco', 'sf_ele_id']
        for (obj, obj_hists) in zip([self.events.MuonGood, self.events.ElectronGood, self.events.JetGood], [self.muon_hists, self.electron_hists, self.jet_hists]):
        #for (obj, obj_hists) in zip([self.events.ElectronGood], [self.electron_hists]):
            fill_histograms_object_with_variations(self, obj, obj_hists, variations)

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
