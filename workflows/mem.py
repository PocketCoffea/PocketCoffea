import awkward as ak
from coffea import hist

from workflows.base import ttHbbBaseProcessor

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, year='2017', cfg='test.py', hist_dir='histograms/', hist2d =False, DNN=False) -> None:
        super().__init__(year=year, cfg=cfg, hist_dir=hist_dir, hist2d=hist2d, DNN=DNN)
        self.bquark_hists = [histname for histname in self._hist_dict.keys() if 'bquark' in histname]

    def parton_matching(self, events: ak.Array) -> ak.Array:
        # Select b-quarks at LHE level
        isOutgoing = events.LHEPart.status == 1
        isB = abs(events.LHEPart.pdgId) == 5
        bquarks = events.LHEPart[isB & isOutgoing]

        # Select b-quarks at Gen level, coming from H->bb decay
        if events.metadata["dataset"] == 'ttHTobb':
            isHiggs = events.GenPart.pdgId == 25
            isHard = events.GenPart.hasFlags(['fromHardProcess'])
            hasTwoChildren = ak.num(events.GenPart.childrenIdxG, axis=2) == 2
            higgs = events.GenPart[isHiggs & isHard & hasTwoChildren]
            bquarks = ak.concatenate( (bquarks, ak.flatten(higgs.children, axis=2)), axis=1 )
            # Sort b-quarks by pt
            bquarks = ak.with_name(bquarks[ak.argsort(bquarks.pt)], name='PtEtaPhiMCandidate')

        # Compute deltaR(b, jet) and save the nearest jet (deltaR matching)
        max_len = ak.max(ak.num(bquarks))
        bquarks = ak.pad_none(bquarks, max_len)
        for i in range(max_len):
            parton = bquarks[:,i]
            deltaR = parton.delta_r(events.JetGood)
            hasJet = ak.sum(ak.mask(deltaR, deltaR<1),axis=-1) > 0
            sortedR = ak.argsort(ak.mask(deltaR, deltaR<1))
            if i == 0:
                jets_matched = ak.unflatten( ak.mask( ak.firsts(events.JetGood[sortedR]), hasJet ), 1 )
                idx_matched  = ak.unflatten( ak.mask( ak.firsts(sortedR), hasJet ), 1 )
                dr_matched   = ak.unflatten( ak.mask( ak.firsts(deltaR[sortedR]), hasJet ), 1 )
                hasUniqueMatch  = ak.ones_like(events.event, dtype=bool)
            else:
                jets_matched = ak.concatenate( ( jets_matched, ak.unflatten( ak.mask( ak.firsts(events.JetGood[sortedR]), hasJet ), 1 ) ), axis=1 )
                idx_matched  = ak.concatenate( ( idx_matched, ak.unflatten( ak.firsts(sortedR), 1 ) ), axis=1 )
                dr_matched   = ak.concatenate( ( dr_matched, ak.unflatten( ak.firsts(deltaR[sortedR]), 1 ) ), axis=1 )
                # Check that the jets are not double matched to different b-quarks
                pairs_to_check = [(j, i) for j in range(i)]
                for pair in pairs_to_check:
                    mask = ak.fill_none((idx_matched[:,pair[0]] != idx_matched[:,pair[1]]), True)
                    #print(i, "idx_matched[:,pair[0]]", idx_matched[:,pair[0]])
                    #print(i, "idx_matched[:,pair[1]]", idx_matched[:,pair[1]])
                    #print(i, "mask", mask)
                    hasUniqueMatch = hasUniqueMatch & mask

        hasMatch = ak.count(bquarks.pt, axis=1) == ak.count(idx_matched, axis=1)
        events["BQuark"] = ak.with_field(bquarks, dr_matched, "drMatchedJet")
        events["BQuark"] = ak.with_field(events.BQuark, hasMatch, "hasMatch")
        events["BQuark"] = ak.with_field(events.BQuark, hasUniqueMatch, "hasUniqueMatch")
        # Check that all the b-quarks are matched to a jet
        #print("jets_matched", jets_matched)
        #print("idx_matched", idx_matched)
        #print("drMatchedJet", events.BQuark.drMatchedJet)
        #print("hasMatch", hasMatch)
        print("hasUniqueMatch", hasUniqueMatch)
        #print("jets_matched[~hasUniqueMatch]", jets_matched[~hasUniqueMatch])
        print("idx_matched[~hasUniqueMatch]", idx_matched[~hasUniqueMatch])
        #print(bquarks.delta_r(events.BJetGood))

    #def fill_histograms_extra(self, histname, h, cut, events):
    #    if histname in self.bquark_hists:
    #        parton = events.BQuark
    #        weight = ak.flatten(self.weights.weight() * ak.Array(ak.fill_none(ak.ones_like(parton.pt), 0) * self._cuts.all(*self._selections[cut])))
    #        fields = {k: ak.flatten(ak.fill_none(parton[k], -9999)) for k in h.fields if k in dir(parton)}

    #    return fields, weight

    def fill_histograms_extra(self, output, events):
        for histname, h in output.items():
            if type(h) is not hist.Hist: continue
            if histname not in self.bquark_hists: continue
            mask_axis = hist.Cat("collection", "Collection")
            h = hist.Hist(h.label, *h.axes(), mask_axis)
            for cut in self._selections.keys():
                if histname in self.bquark_hists:
                    for (collection, mask) in zip(["matched", "not matched", "unique", "not unique"], [events.BQuark.hasMatch, ~events.BQuark.hasMatch, events.BQuark.hasUniqueMatch, ~events.BQuark.hasUniqueMatch]):
                        parton = ak.mask(events.BQuark, mask)
                        weight = ak.flatten(self.weights.weight() * ak.Array(ak.fill_none(ak.ones_like(parton.pt), 0) * self._cuts.all(*self._selections[cut])))
                        fields = {k: ak.flatten(ak.fill_none(parton[k], -9999)) for k in h.fields if k in dir(parton)}
                        h.fill(dataset=events.metadata["dataset"], cut=cut, year=self._year, collection=collection, **fields, weight=weight)
            output[histname] = h
        return output

    def process_extra(self, events: ak.Array) -> ak.Array:
        self.parton_matching(events)
