import awkward as ak
import sys

from .base import BaseProcessorABC
from ..utils.configurator import Configurator
from ..lib.hist_manager import Axis
from ..lib.objects import (
    jet_correction,
    lepton_selection,
    jet_selection,
    btagging,
    get_dilepton,
)


class ttHbbBaseProcessor(BaseProcessorABC):
    def __init__(self, cfg: Configurator):
        super().__init__(cfg)
        # Additional axis for the year
        self.custom_axes.append(
            Axis(
                coll="metadata",
                field="year",
                name="year",
                bins=set(sorted(self.cfg.years)),
                type="strcat",
                growth=False,
                label="Year",
            )
        )

    def apply_object_preselection(self, variation):
        '''
        The ttHbb processor cleans
          - Electrons
          - Muons
          - Jets -> JetGood
          - BJet -> BJetGood

        '''
        # Include the supercluster pseudorapidity variable
        electron_etaSC = self.events.Electron.eta + self.events.Electron.deltaEtaSC
        self.events["Electron"] = ak.with_field(
            self.events.Electron, electron_etaSC, "etaSC"
        )
        # Build masks for selection of muons, electrons, jets, fatjets
        self.events["MuonGood"] = lepton_selection(
            self.events, "Muon", self.cfg.finalstate
        )
        self.events["ElectronGood"] = lepton_selection(
            self.events, "Electron", self.cfg.finalstate
        )
        leptons = ak.with_name(
            ak.concatenate((self.events.MuonGood, self.events.ElectronGood), axis=1),
            name='PtEtaPhiMCandidate',
        )
        self.events["LeptonGood"] = leptons[ak.argsort(leptons.pt, ascending=False)]

        self.events["JetGood"], self.jetGoodMask = jet_selection(
            self.events, "Jet", self.cfg.finalstate
        )
        self.events["BJetGood"] = btagging(self.events["JetGood"], self._btag)

        if self.cfg.finalstate == 'dilepton':
            self.events["ll"] = get_dilepton(
                self.events.ElectronGood, self.events.MuonGood
            )

    def count_objects(self, variation):
        self.events["nMuonGood"] = ak.num(self.events.MuonGood)
        self.events["nElectronGood"] = ak.num(self.events.ElectronGood)
        self.events["nLeptonGood"] = (
            self.events["nMuonGood"] + self.events["nElectronGood"]
        )
        self.events["nJetGood"] = ak.num(self.events.JetGood)
        self.events["nBJetGood"] = ak.num(self.events.BJetGood)
        # self.events["nfatjet"]   = ak.num(self.events.FatJetGood)

    # Function that defines common variables employed in analyses and save them as attributes of `events`
    def define_common_variables_before_presel(self, variation):
        self.events["JetGood_Ht"] = ak.sum(abs(self.events.JetGood.pt), axis=1)

    def fill_histograms_extra(self, variation):
        '''
        This processor saves a metadata histogram with the number of
        events for chunk
        '''

        # # Filling the special histograms for events if they are present
        # if self._hasSubsamples:
        #     for subs in self._subsamples_names:
        #         if self._isMC & (
        #             "events_per_chunk" in self.hists_managers[subs].histograms
        #         ):
        #             hepc = self.hists_managers[subs].get_histogram("events_per_chunk")
        #             hepc.hist_obj.fill(
        #                 cat=hepc.only_categories[0],
        #                 variation="nominal",
        #                 year=self._year,
        #                 nEvents_initial=self.nEvents_initial,
        #                 nEvents_after_skim=self.nEvents_after_skim,
        #                 nEvents_after_presel=self.nEvents_after_presel,
        #             )
        #             self.output["processing_metadata"]["events_per_chunk"][
        #                 subs
        #             ] = hepc.hist_obj
        # else:
        #     if self._isMC & (
        #         "events_per_chunk" in self.hists_managers[self._sample].histograms
        #     ):
        #         hepc = self.hists_managers[self._sample].get_histogram(
        #             "events_per_chunk"
        #         )
        #         hepc.hist_obj.fill(
        #             cat=hepc.only_categories[0],
        #             variation="nominal",
        #             year=self._year,
        #             nEvents_initial=self.nEvents_initial,
        #             nEvents_after_skim=self.nEvents_after_skim,
        #             nEvents_after_presel=self.nEvents_after_presel,
        #         )
        #         self.output["processing_metadata"]["events_per_chunk"][
        #             self._sample
        #         ] = hepc.hist_obj
