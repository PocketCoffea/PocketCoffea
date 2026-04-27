import awkward as ak
from pocket_coffea.workflows.base import BaseProcessorABC
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.objects import jet_selection, btagging
from pocket_coffea.lib.leptons import lepton_selection_promptMVA
from pocket_coffea.lib.xgboost_evaluator import ElectronMVAEvaluator, MuonMVAEvaluator


class PromptMVAProcessor(BaseProcessorABC):

    def __init__(self, cfg: Configurator):
        super().__init__(cfg)

    def apply_object_preselection(self, variation):
        if self._year in ["2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix"]:
            promptMVA_electron_evaluator = ElectronMVAEvaluator(
                self.params.lepton_scale_factors.electron_sf.promptMVA_jsons[self._year].model_weights
            )
            promptMVA_muon_evaluator = MuonMVAEvaluator(
                self.params.lepton_scale_factors.muon_sf.promptMVA_jsons[self._year].model_weights
            )
            _, presel_ele_mask = lepton_selection_promptMVA(
                self.events, "Electron", self.params, self._year, apply_mva_cut=False
            )
            promptMVA_ele = promptMVA_electron_evaluator.evaluate(
                self.events["Electron"], self.events["Jet"], mask=presel_ele_mask
            )
            self.events["Electron"] = ak.with_field(self.events["Electron"], promptMVA_ele, "mvaTTH_redo")

            _, presel_muon_mask = lepton_selection_promptMVA(
                self.events, "Muon", self.params, self._year, apply_mva_cut=False
            )
            promptMVA_muon = promptMVA_muon_evaluator.evaluate(
                self.events["Muon"], self.events["Jet"], mask=presel_muon_mask
            )
            self.events["Muon"] = ak.with_field(self.events["Muon"], promptMVA_muon, "mvaTTH_redo")

        self.events["ElectronGood"], _ = lepton_selection_promptMVA(
            self.events, "Electron", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Electron.id[self._year],
        )

        self.events["MuonGood"], _ = lepton_selection_promptMVA(
            self.events, "Muon", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Muon.id[self._year],
        )

        leptons = ak.with_name(
            ak.concatenate((self.events.MuonGood, self.events.ElectronGood), axis=1),
            name="PtEtaPhiMCandidate",
        )
        self.events["LeptonGood"] = leptons[ak.argsort(leptons.pt, ascending=False)]

        self.events["JetGood"], self.jetGoodMask = jet_selection(
            self.events, "Jet", self.params, self._year,
            leptons_collection="LeptonGood",
        )

        self.events["BJetGood"] = btagging(
            self.events["JetGood"],
            self.params.btagging.working_point[self._year],
            wp=self.params.object_preselection.Jet["btag"]["wp"],
        )

    def count_objects(self, variation):
        self.events["nElectronGood"] = ak.num(self.events.ElectronGood)
        self.events["nMuonGood"] = ak.num(self.events.MuonGood)
        self.events["nJetGood"] = ak.num(self.events.JetGood)
        self.events["nBJetGood"] = ak.num(self.events.BJetGood)

    def define_common_variables_before_presel(self, variation):
        pass
