import awkward as ak

def passthrough(events, **kargs):
    return ak.full_like(events.event, True, dtype=bool)

def dilepton(events, params, year, sample):

    MET  = events[params["METbranch"][year]]

    # Masks for same-flavor (SF) and opposite-sign (OS)
    SF = ( ((events.nmuon == 2) & (events.nelectron == 0)) | ((events.nmuon == 0) & (events.nelectron == 2)) )
    OS = (events.ll.charge == 0)
    #SFOS = SF & OS
    not_SF = ( (events.nmuon == 1) & (events.nelectron == 1) )

    mask = ( (events.nlep == 2) &
             (ak.firsts(events.LeptonGood.pt) > params["pt_leading_lepton"]) &
             (events.njet >= params["njet"]) &
             (events.nbjet >= params["nbjet"]) &
             (MET.pt > params["met"]) &
             OS & # Opposite sign 
             (events.ll.mass > params["mll"]) &
             # If same-flavour we exclude a mll mass interval 
             ( ( SF & ( (events.ll.mass < params["mll_SFOS"]["low"]) | (events.ll.mass > params["mll_SFOS"]["high"]) ) )
               | not_SF) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)


def count_objects_gt(events,params,year,sample):
    mask = ak.count(events[params["object"]], axis=1) > params["value"]
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def count_objects_lt(events,params,year,sample):
    mask = ak.count(events[params["object"]], axis=1) < params["value"]
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)


