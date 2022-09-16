import awkward as ak
from .cut_definition import Cut
from ..parameters.btag import btag

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
    if f"n{params['coll']}" in events.fields:
        return events[f"n{params['coll']}"] >= params["N"]
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

def nBtag(events, params, year, **kwargs):
    ''' Mask for min N jets with minpt and passing btagging.
    The btag params will come from the processor, not from the parameters
    '''
    if params["coll"] == "BJetGood":
        # No need to apply the btaggin on the jet 
        # Assume that the collection of clean bjets has been created
        if params["minpt"] > 0.:
            return ak.sum((events.BJetGood.pt >= params["minpt"]),
                      axis=1) >= params["N"]
        else:
            return events.nBJetGood >= params["N"]
    else:
        btagparam = btag[year]
        if params["minpt"] > 0.:
            return ak.sum((events[params["coll"]][btagparam["btagging_algorithm"]] > btagparam["btagging_WP"]) &
                      (events[params["coll"]].pt >= params["minpt"]),
                      axis=1) >= params["N"]
        else:
            return ak.sum((events[params["coll"]][btagparam["btagging_algorithm"]] > btagparam["btagging_WP"]),
                      axis=1) >= params["N"]
 
def get_nBtag(N, minpt=0, coll="BJetGood", name=None):
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
    SF = ( ((events.nMuonGood == 2) & (events.nElectronGood == 0)) | ((events.nMuonGood == 0) & (events.nElectronGood == 2)) )
    OS = (events.ll.charge == 0)
    #SFOS = SF & OS
    not_SF = ( (events.nMuonGood == 1) & (events.nElectronGood == 1) )

    mask = ( (events.nLepGood == 2) &
             (ak.firsts(events.LeptonGood.pt) > params["pt_leading_lepton"]) &
             (events.nJetGood >= params["njet"]) &
             (events.nBJetGood >= params["nbjet"]) &
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

    has_one_electron = (events.nElectronGood == 1)
    has_one_muon     = (events.nMuonGood == 1)

    mask = ( (events.nLepGood == 1) &
              # Here we properly distinguish between leading muon and leading electron
             ( (has_one_electron & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_electron"][year])) |
               (has_one_muon & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_muon"][year])) ) &
             (events.nJetGood >= params["njet"]) &
             (events.nBJetGood >= params["nbjet"]) &
             (MET.pt > params["met"]) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)

def semileptonic_triggerSF(events, params, year, sample, **kwargs):

    has_one_electron = (events.nElectronGood == 1)
    has_one_muon     = (events.nMuonGood == 1)

    mask = ( (events.nLepGood == 2) &
              # Here we properly distinguish between leading muon and leading electron
             ( (has_one_electron & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_electron"][year])) &
               (has_one_muon & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_muon"][year])) ) &
             (events.nJetGood >= params["njet"]) )
             #& (events.nBJetGood >= params["nbjet"]) & (MET.pt > params["met"]) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), ~ak.is_none(mask), mask)
