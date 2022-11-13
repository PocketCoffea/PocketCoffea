import sys
import awkward as ak
from coffea import hist
import numba

from .tthbb_base_processor import ttHbbBaseProcessor
from ..lib.fill import fill_column_accumulator
from ..lib.deltaR_matching import object_matching
from ..lib.parton_provenance import get_partons_provenance_ttHbb


class PartonMatchingProcessor(ttHbbBaseProcessor):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)
        self.dr_min = self.cfg.workflow_extra_options["parton_jet_min_dR"]

        # Defining the column accumulators for parton and jet pts,
        self.add_column_accumulator("npartons_tot", categories=["4j"], store_size=False)
        self.add_column_accumulator("parton_pt", categories=['4j'], store_size=False)
        self.add_column_accumulator("parton_eta", categories=['4j'], store_size=False)
        self.add_column_accumulator("parton_phi", categories=['4j'], store_size=False)
        self.add_column_accumulator("parton_pdgId", categories=['4j'], store_size=False)
        self.add_column_accumulator("parton_prov", categories=['4j'], store_size=False)

        self.add_column_accumulator(
            "npartons_matched", categories=["4j"], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_pt", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_eta", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_phi", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_pdgId", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_prov", categories=['4j'], store_size=False
        )

        self.add_column_accumulator("jet_pt", categories=['4j'], store_size=True)
        self.add_column_accumulator("jet_eta", categories=['4j'], store_size=False)
        self.add_column_accumulator("jet_phi", categories=['4j'], store_size=False)
        self.add_column_accumulator("jet_btag", categories=['4j'], store_size=False)

        self.add_column_accumulator(
            "matched_jet_pt", categories=['4j'], store_size=True
        )
        self.add_column_accumulator(
            "matched_jet_eta", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_jet_phi", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_jet_btag", categories=['4j'], store_size=False
        )
        self.add_column_accumulator(
            "matched_parton_jet_dR", categories=['4j'], store_size=False
        )

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
            # DO NOT sorty b-quarks by pt
            quarks = ak.with_name(
                ak.concatenate((quarks, higgs_partons), axis=1),
                name='PtEtaPhiMCandidate',
            )

        # Get the interpretation
        if self._sample == "ttHTobb":
            prov = get_partons_provenance_ttHbb(
                ak.Array(quarks.pdgId, behavior={}), ak.ArrayBuilder()
            ).snapshot()
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

        self.events["Parton"] = quarks
        self.events["PartonMatched"] = ak.with_field(
            matched_quarks, deltaR_matched, "dRMatchedJet"
        )
        self.events["JetGoodMatched"] = matched_jets
        self.matched_partons_mask = ~ak.is_none(self.events.JetGoodMatched, axis=1)

    def count_partons(self):
        self.events["nParton"] = ak.num(self.events.Parton, axis=1)
        self.events["nPartonMatched"] = ak.count(
            self.events.PartonMatched.pt, axis=1
        )  # use count since we have None

    def process_extra_after_presel(self) -> ak.Array:
        self.do_parton_matching()
        self.count_partons()

        # All the partons
        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "npartons_tot",
            ["4j"],
            self.events.nParton,
            save_size=False,
            flatten=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "parton_pt",
            ["4j"],
            self.events.Parton.pt,
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "parton_eta",
            ["4j"],
            self.events.Parton.eta,
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "parton_phi",
            ["4j"],
            self.events.Parton.phi,
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "parton_pdgId",
            ["4j"],
            self.events.Parton.pdgId,
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "parton_prov",
            ["4j"],
            self.events.Parton.provenance,
            save_size=False,
        )

        # Matched partons
        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "npartons_matched",
            ["4j"],
            self.events.nPartonMatched,
            save_size=False,
            flatten=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_pt",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.pt, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_eta",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.eta, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_phi",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.phi, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_pdgId",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.pdgId, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_prov",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.provenance, -999),
            save_size=False,
        )

        # Jets
        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "jet_pt",
            ["4j"],
            ak.fill_none(self.events.JetGood.pt, -999),
            save_size=True,
        )  # Saving also the size for all the others

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "jet_eta",
            ["4j"],
            ak.fill_none(self.events.JetGood.eta, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "jet_phi",
            ["4j"],
            ak.fill_none(self.events.JetGood.phi, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "jet_btag",
            ["4j"],
            ak.fill_none(self.events.JetGood.btagDeepFlavB, -999),
            save_size=False,
        )

        # Matched jets
        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_jet_pt",
            ["4j"],
            ak.fill_none(self.events.JetGoodMatched.pt, -999),
            save_size=True,
        )  # Saving also the size for all the others

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_jet_eta",
            ["4j"],
            ak.fill_none(self.events.JetGoodMatched.eta, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_jet_phi",
            ["4j"],
            ak.fill_none(self.events.JetGoodMatched.phi, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_jet_btag",
            ["4j"],
            ak.fill_none(self.events.JetGoodMatched.btagDeepFlavB, -999),
            save_size=False,
        )

        fill_column_accumulator(
            self.output["columns"],
            self._sample,
            "matched_parton_jet_dR",
            ["4j"],
            ak.fill_none(self.events.PartonMatched.dRMatchedJet, -999),
            save_size=False,
        )
