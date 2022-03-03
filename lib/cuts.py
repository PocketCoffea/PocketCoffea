import awkward as ak

def dilepton(processor, events):

    if processor._year == '2017':
        MET = events.METFixEE2017
    else:
        MET = events.MET

    # Masks for same-flavor (SF) and opposite-sign (OS)
    SF = ( ((events.nmuon == 2) & (events.nelectron == 0)) | ((events.nmuon == 0) & (events.nelectron == 2)) )
    OS = (events.ll.charge == 0)
    #SFOS = SF & OS
    not_SF = ( (events.nmuon == 1) & (events.nelectron == 1) )

    mask = ( (events.nlep == 2) & (events.nlepgood >= 1) & OS &
             (events.njet >= 2) & (events.nbjet >= 1) & (MET.pt > 40) &
             (events.ll.mass > 20) & ((SF & ((events.ll.mass < 76) | (events.ll.mass > 106))) | not_SF) )

    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)
