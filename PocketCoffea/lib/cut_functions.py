import awkward as ak
from .cut_definition import Cut

def passthrough(events, **kargs):
    return ak.full_like(events.event, True, dtype=bool)

###########################
## Functions to count objects
def count_objects_gt(events,params,year,sample):
    mask = ak.num(events[params["object"]], axis=1) > params["value"]
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def count_objects_lt(events,params,year,sample):
    mask = ak.num(events[params["object"]], axis=1) < params["value"]
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def count_objects_eq(events,params,year,sample):
    mask = ak.num(events[params["object"]], axis=1) == params["value"]
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

##################################
# Min number of objects with pt cut

def nObj(events, params, **kwargs):
    return ak.num(events[params['coll']]) >= params["N"]

def nObj_minPt(events, params, **kwargs):
    return ak.sum(events[params["coll"]].pt >= params["minpt"], axis=1) >= params["N"]

def get_nObj(N, minpt=None, coll="JetGood", name=None):
    if name == None:
        if minpt:
            name = f"n{coll}_min{N}_pt{minpt}"
        else:
            name = f"n{coll}_min{N}"
    if minpt:
        return Cut(
            name=name,
            params={"N": N, "coll":coll, "minpt": minpt},
            function=nObj_minPt
        )
    else:
        return Cut(
            name=name,
            params={"N": N, "coll":coll},
            function=nObj
        )

##########################################
# Min b-tagged jets with custom collection

def nBtag(events, params, btag, **kwargs):
    ''' Mask for min N jets with minpt and passing btagging.
    The btag params will come from the processor, not from the parameters
    '''
    if params["coll"] = "BJetGood":
        # Assume that the collection of clean bjets has been created
        if params["minpt"] > 0.:
            return ak.sum((events.BJetGood.pt >= params["minpt"]),
                      axis=1) >= params["N"]
        else:
            return events.nbjet >= params["N"]
    else:
        if params["minpt"] > 0.:
            return ak.sum((events[params["coll"]][btag["btagging_algorithm"]] > btag["btagging_WP"]) &
                      (events[params["coll"]].pt >= params["minpt"]),
                      axis=1) >= params["N"]
        else:
            return ak.sum((events[params["coll"]][btag["btagging_algorithm"]] > btag["btagging_WP"]),
                      axis=1) >= params["N"]
 
def get_nBtag(N, minpt=0, coll="JetGood", name=None):
    if name == None:
        name = f"n{coll}_btag_{N}_pt{minpt}"
    return Cut(
        name=name,
        params = {"N": N, "coll": coll, "minpt": minpt},
        function=nBtag
    )

##################
# Common preselection functions
def dilepton(events, params, year, sample, **kwargs):

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
               | not_SF ) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def semileptonic(events, params, year, sample, **kwargs):

    MET  = events[params["METbranch"][year]]

    has_one_electron = (events.nelectron == 1)
    has_one_muon     = (events.nmuon == 1)

    mask = ( (events.nlep == 1) &
              # Here we properly distinguish between leading muon and leading electron
             ( (has_one_electron & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_electron"][year])) |
               (has_one_muon & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_muon"][year])) ) &
             (events.njet >= params["njet"]) &
             (events.nbjet >= params["nbjet"]) &
             (MET.pt > params["met"]) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def semileptonic_triggerSF(events, params, year, sample, **kwargs):

    has_one_electron = (events.nelectron == 1)
    has_one_muon     = (events.nmuon == 1)

    mask = ( (events.nlep == 2) &
              # Here we properly distinguish between leading muon and leading electron
             ( (has_one_electron & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_electron"][year])) &
               (has_one_muon & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_muon"][year])) ) &
             (events.njet >= params["njet"]) )
             #& (events.nbjet >= params["nbjet"]) & (MET.pt > params["met"]) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)
