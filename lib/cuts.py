import awkward as ak

from parameters.selection import event_selection

def dilepton(events, year, finalstate):

    cuts = event_selection[finalstate]
    MET  = events[cuts["METbranch"][year]]

    # Masks for same-flavor (SF) and opposite-sign (OS)
    SF = ( ((events.nmuon == 2) & (events.nelectron == 0)) | ((events.nmuon == 0) & (events.nelectron == 2)) )
    OS = (events.ll.charge == 0)
    #SFOS = SF & OS
    not_SF = ( (events.nmuon == 1) & (events.nelectron == 1) )

    mask = ( (events.nlep == 2) & (ak.firsts(events.LeptonGood.pt) > cuts["pt_leading_lepton"]) & OS &
             (events.njet >= 2) & (events.nbjet >= 1) & (MET.pt > cuts["met"]) &
             (events.ll.mass > 20) & ((SF & ((events.ll.mass < 76) | (events.ll.mass > 106))) | not_SF) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)
