# Basic workflow for testing debug logging
import awkward as ak
from pocket_coffea.workflows.base import BaseProcessorABC
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.objects import (
    lepton_selection,
    jet_selection,
    btagging,
)

class BasicProcessor(BaseProcessorABC):

    def __init__(self, cfg: Configurator):
        super().__init__(cfg)

    def apply_object_preselection(self, variation):
        '''Apply object preselections to the events.'''
        
        self.events["Electron"] = ak.with_field(
            self.events.Electron,
            self.events.Electron.eta + self.events.Electron.deltaEtaSC, "etaSC"
        )   
        self.events["ElectronGood"] = lepton_selection(
            self.events, "Electron", self.params
        )
        
        self.events["MuonGood"] = lepton_selection(
            self.events, "Muon", self.params
        )

        leptons = ak.with_name(
            ak.concatenate((self.events.MuonGood, self.events.ElectronGood), axis=1),
            name='PtEtaPhiMCandidate',
        )
        self.events["LeptonGood"] = leptons[ak.argsort(leptons.pt, ascending=False)]

        self.events["JetGood"], self.jetGoodMask = jet_selection(
            self.events, "Jet", self.params,
            self._year, 
            leptons_collection="LeptonGood"
        )

        self.events["BJetGood"] = btagging(
            self.events["JetGood"],
            self.params.btagging.working_point[self._year],
            wp=self.params.object_preselection.Jet["btag"]["wp"]
        )

    def count_objects(self, variation):
        '''Count preselected objects and save counts as event attributes.'''
        self.events["nElectronGood"] = ak.num(self.events.ElectronGood)
        self.events["nMuonGood"] = ak.num(self.events.MuonGood)
        self.events["nLeptonGood"] = ak.num(self.events.LeptonGood)
        self.events["nJetGood"] = ak.num(self.events.JetGood)
        self.events["nBJetGood"] = ak.num(self.events.BJetGood)
