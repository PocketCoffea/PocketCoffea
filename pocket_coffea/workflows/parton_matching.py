import sys
import awkward as ak
from coffea import hist
import numba

from .tthbb_base_processor import ttHbbBaseProcessor
from ..lib.deltaR_matching import object_matching
from ..lib.parton_provenance import get_partons_provenance_ttHbb


class PartonMatchingProcessor(ttHbbBaseProcessor):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)
        self.dr_min = self.cfg.workflow_extra_options["parton_jet_min_dR"]

    def do_parton_matching(self) -> ak.Array:
        # Selects quarks at LHE level
        isOutgoing = self.events.LHEPart.status == 1
        isParton = (abs(self.events.LHEPart.pdgId) < 6) | (
            self.events.LHEPart.pdgId == 21
        )
        quarks = self.events.LHEPart[isOutgoing & isParton]

        # Select b-quarks at Gen level, coming from H->bb decay
        if self._sample == 'ttHTobb':
            higgs = self.events.GenPart[
                (self.events.GenPart.pdgId == 25)
                & (self.events.GenPart.hasFlags(['fromHardProcess']))
            ]
            higgs = higgs[ak.num(higgs.childrenIdxG, axis=2) == 2]
            higgs_partons = ak.with_field(
                ak.flatten(higgs.children, axis=2), 25, "from_part"
            )
            # DO NOT sort b-quarks by pt
            # if not we are not able to match them with the provenance
            quarks = ak.with_name(
                ak.concatenate((quarks, higgs_partons), axis=1),
                name='PtEtaPhiMCandidate',
            )

        # Get the interpretation
        if self._sample == "ttHTobb":
            prov = get_partons_provenance_ttHbb(
                ak.Array(quarks.pdgId, behavior={}), ak.ArrayBuilder()
            ).snapshot()
            self.events["HiggsParton"] = self.events.LHEPart[
                self.events.LHEPart.pdgId == 25
            ]
        else:
            prov = -1 * ak.ones_like(quarks)

        # Adding the provenance to the quark object
        quarks = ak.with_field(quarks, prov, "provenance")

        # Calling our general object_matching function.
        # The output is an awkward array with the shape of the second argument and None where there is no matching.
        # So, calling like this, we will get out an array of matched_quarks with the dimension of the JetGood.
        matched_quarks, matched_jets, deltaR_matched = object_matching(
            quarks, self.events.JetGood, dr_min=self.dr_min
        )

        # Saving leptons and neutrino parton level
        self.events["LeptonParton"] = self.events.LHEPart[
            (self.events.LHEPart.status == 1)
            & (abs(self.events.LHEPart.pdgId) > 10)
            & (abs(self.events.LHEPart.pdgId) < 15)
        ]

        self.events["Parton"] = quarks
        self.events["PartonMatched"] = ak.with_field(
            matched_quarks, deltaR_matched, "dRMatchedJet"
        )
        self.events["JetGoodMatched"] = ak.with_field(
            matched_jets, deltaR_matched, "dRMatchedJet"
        )
        self.matched_partons_mask = ~ak.is_none(self.events.JetGoodMatched, axis=1)

    def count_partons(self):
        self.events["nParton"] = ak.num(self.events.Parton, axis=1)
        self.events["nPartonMatched"] = ak.count(
            self.events.PartonMatched.pt, axis=1
        )  # use count since we have None

    def process_extra_after_presel(self, variation) -> ak.Array:
        self.do_parton_matching()
        self.count_partons()
