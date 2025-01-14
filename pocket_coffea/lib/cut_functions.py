import awkward as ak
from .cut_definition import Cut
from .triggers import get_trigger_mask_byprimarydataset,  apply_trigger_mask
import correctionlib
import numpy as np
from coffea.lumi_tools import LumiMask



def passthrough_f(events, **kargs):
    '''
    Identity cut:  passthrough of all events.
    '''
    return ak.full_like(events.event, True, dtype=bool)


##############################
## Factory methods for HLT triggers

def get_HLTsel(primaryDatasets=None, invert=False):
    '''Create the HLT trigger mask

    The Cut function reads the triggers configuration and create the mask.
    For MC the OR of all the triggers in the specific configuration key is performed.
    For DATA only the corresponding primary dataset triggers are applied.
    if primaryDatasets param is passed, the correspoding triggers are applied, both
    on DATA and MC, overwriting any other configuration.

    This is useful to remove the overlap of primary datasets in data.

    :param primaryDatasets: (optional) list of primaryDatasets to use. Overwrites any other config
                                      both for Data and MC
    :param invert: invert the mask, if True the function returns events failing the HLT selection

    :returns: events mask
    '''
    name = "HLT_trigger"
    if primaryDatasets:
        name += "_" + "_".join(primaryDatasets)
    if invert:
        name += "_NOT"
    return Cut(
        name=name,
        params={"primaryDatasets": primaryDatasets, "invert": invert},
        function=lambda events, params, processor_params, year, isMC,  **kwargs:  get_trigger_mask_byprimarydataset(
            events,
            trigger_dict=processor_params.HLT_triggers,
            year=year,
            isMC=isMC,
            primaryDatasets=params["primaryDatasets"],
            invert=params["invert"])
    )

def get_HLTsel_custom(trigger_list, invert=False):
    '''Create the HLT trigger mask using a custom list of triggers.

    The Cut function does not read the triggers configuration, but uses the list of triggers provided dynamically.
    '''
    triggers_to_apply = [t.lstrip("HLT_") for t in trigger_list]
    return Cut(
        name="HLT_trigger_"+ "_".join(trigger_list),
        params={"triggers_to_apply": triggers_to_apply,  "invert": invert},
        function=lambda events, params, processor_params, year, isMC, **kwargs:  apply_trigger_mask(
            events,
            triggers_to_apply=params["triggers_to_apply"],
            year=year,
            invert=params["invert"])
    )

###########################
## Implementation of JetVetoMaps 
## (Recommended for Run 3, see https://cms-jerc.web.cern.ch/Recommendations/#jet-veto-maps)

def get_JetVetoMap(name="JetVetoMaps"):
    return Cut(
        name=name, params={}, function=get_JetVetoMap_Mask
    )
       
def get_JetVetoMap_Mask(events, params, year, processor_params, sample, isMC, **kwargs):
    jets = events.Jet
    mask_for_VetoMap = (
        ((jets.jetId & 2)==2) # Must fulfill tight jetId
        & (abs(jets.eta) < 5.19) # Must be within HCal acceptance
        & (jets.pt*(1-jets.muonSubtrFactor) > 15.) # May no be Muons misreconstructed as jets
        & ((jets["neEmEF"]+jets["chEmEF"])<0.9) # Energy fraction not dominated by ECal
    )
    jets = jets[mask_for_VetoMap]
    cset = correctionlib.CorrectionSet.from_file(
        processor_params.jet_scale_factors.vetomaps[year]["file"]
    )
    corr = cset[processor_params.jet_scale_factors.vetomaps[year]["name"]]
    etaFlat, phiFlat, etaCounts = ak.flatten(jets.eta), ak.flatten(jets.phi), ak.num(jets.eta)
    phiFlat = np.clip(phiFlat, -3.14159, 3.14159) # Needed since no overflow included in phi binning
    weight = ak.unflatten(
        corr.evaluate("jetvetomap", etaFlat, phiFlat),
        counts=etaCounts,
    )
    eventMask = ak.sum(weight, axis=-1)==0 # if at least one jet is vetoed, reject it event
    return ak.where(ak.is_none(eventMask), False, eventMask)



#########################
# Event flags
def apply_event_flags(events, params, year, processor_params, sample, isMC, **kwargs):
    '''
    Apply the event flags to the events
    '''
    mask = np.ones(len(events), dtype=bool)
    flags = processor_params.event_flags[year]
    if not isMC:
        flags += processor_params.event_flags_data[year]
    for flag in flags:
        mask &= getattr(events.Flag, flag).to_numpy()
        
    return mask

eventFlags = Cut(
    name="event_flags",
    params={},
    function=apply_event_flags
    )

###########################
# Golden JSON

def apply_golden_json(events, params, year, processor_params, sample, isMC, **kwargs):
    if not isMC:
        return LumiMask(processor_params.lumi.goldenJSON[year])(
                events.run, events.luminosityBlock)
    else:
        return np.ones(len(events), dtype=bool)

goldenJson = Cut(
    name="golden_json_lumi",
    params={},
    function=apply_golden_json
    )

###########################
## Functions to count objects
def count_objects_gt(events, params, **kwargs):
    '''
    Count the number of objects in `params["object"]` and
    keep only events with larger (>) amount than `params["value"]`.
    '''
    mask = ak.num(events[params["object"]], axis=1) > params["value"]
    return ak.where(ak.is_none(mask), False, mask)


def count_objects_lt(events, params, **kwargs):
    '''
    Count the number of objects in `params["object"]` and
    keep only events with smaller (<) amount than `params["value"]`.
    '''
    mask = ak.num(events[params["object"]], axis=1) < params["value"]
    return ak.where(ak.is_none(mask), False, mask)


def count_objects_eq(events, params, **kwargs):
    '''
    Count the number of objects in `params["object"]` and
    keep only events with same (==) amount than `params["value"]`.
    '''
    mask = ak.num(events[params["object"]], axis=1) == params["value"]
    return ak.where(ak.is_none(mask), False, mask)


##################################
# Min number of objects with pt cut


def min_nObj(events, params, **kwargs):
    if f"n{params['coll']}" in events.fields:
        return events[f"n{params['coll']}"] >= params["N"]
    return ak.num(events[params['coll']]) >= params["N"]


def min_nObj_minPt(events, params, **kwargs):
    return ak.sum(events[params["coll"]].pt >= params["minpt"], axis=1) >= params["N"]


def less_nObj(events, params, **kwargs):
    if f"n{params['coll']}" in events.fields:
        return events[f"n{params['coll']}"] < params["N"]
    return ak.num(events[params['coll']]) < params["N"]


def eq_nObj(events, params, **kwargs):
    if f"n{params['coll']}" in events.fields:
        return events[f"n{params['coll']}"] == params["N"]
    return ak.num(events[params['coll']]) == params["N"]


def eq_nObj_minPt(events, params, **kwargs):
    return ak.sum(events[params["coll"]].pt >= params["minpt"], axis=1) == params["N"]


def get_nObj_min(N, minpt=None, coll="JetGood", name=None):
    '''
    Factory function which creates a cut for minimum number of objects.
    Optionally a minimum pT is requested.

    :param N: request >= N objects
    :param coll: collection to use
    :param minpt: minimum pT
    :param name: name for the cut, by defaul it is built as n{coll}_min{N}_pt{minpt}

    :returns: a Cut object
    '''
    if name == None:
        if minpt:
            name = f"n{coll}_min{N}_pt{minpt}"
        else:
            name = f"n{coll}_min{N}"
    if minpt:
        return Cut(
            name=name,
            params={"N": N, "coll": coll, "minpt": minpt},
            function=min_nObj_minPt,
        )
    else:
        return Cut(name=name, params={"N": N, "coll": coll}, function=min_nObj)


def get_nObj_eq(N, minpt=None, coll="JetGood", name=None):
    '''
    Factory function which creates a cut for == number of objects.
    Optionally a minimum pT is requested.

    :param N: request == N objects
    :param coll: collection to use
    :param minpt: minimum pT
    :param name: name for the cut, by defaul it is built as n{coll}_eq{N}_pt{minpt}

    :returns: a Cut object
    '''
    if name == None:
        if minpt:
            name = f"n{coll}_eq{N}_pt{minpt}"
        else:
            name = f"n{coll}_eq{N}"
    if minpt:
        return Cut(
            name=name,
            params={"N": N, "coll": coll, "minpt": minpt},
            function=eq_nObj_minPt,
        )
    else:
        return Cut(name=name, params={"N": N, "coll": coll}, function=eq_nObj)


def get_nObj_less(N, coll="JetGood", name=None):
    '''
    Factory function which creates a cut for < number of objects.

    :param N: request < N objects
    :param coll: collection to use
    :param name: name for the cut, by defaul it is built as n{coll}_less{N}
    :returns: a Cut object
    '''
    if name == None:
        name = f"n{coll}_less{N}"
    return Cut(name=name, params={"N": N, "coll": coll}, function=less_nObj)


##########################################
# Min b-tagged jets with custom collection


def nBtagMin(events, params, year, processor_params, **kwargs):
    '''Mask for min N jets with minpt and passing btagging.
    The btag params will come from the processor, not from the parameters
    '''
    if params["coll"] == "BJetGood":
        # No need to apply the btaggin on the jet
        # Assume that the collection of clean bjets has been created
        if params["minpt"] > 0.0:
            return (
                ak.sum((events.BJetGood.pt >= params["minpt"]), axis=1) >= params["N"]
            )
        else:
            return events.nBJetGood >= params["N"]
    else:
        btagparam = processor_params.btagging.working_point[year]
        if params["minpt"] > 0.0:
            return (
                ak.sum(
                    (
                        events[params["coll"]][btagparam["btagging_algorithm"]]
                        > btagparam["btagging_WP"][params["wp"]]
                    )
                    & (events[params["coll"]].pt >= params["minpt"]),
                    axis=1,
                )
                >= params["N"]
            )
        else:
            return (
                ak.sum(
                    (
                        events[params["coll"]][btagparam["btagging_algorithm"]]
                        > btagparam["btagging_WP"][params["wp"]]
                    ),
                    axis=1,
                )
                >= params["N"]
            )


def nBtagEq(events, params, year, processor_params, **kwargs):
    '''Mask for == N jets with minpt and passing btagging.
    The btag params will come from the processor, not from the parameters
    '''
    if params["coll"] == "BJetGood":
        # No need to apply the btaggin on the jet
        # Assume that the collection of clean bjets has been created
        if params["minpt"] > 0.0:
            return (
                ak.sum((events.BJetGood.pt >= params["minpt"]), axis=1) == params["N"]
            )
        else:
            return events.nBJetGood == params["N"]
    else:
        btagparam = processor_params.btagging.working_point[year]
        if params["minpt"] > 0.0:
            return (
                ak.sum(
                    (
                        events[params["coll"]][btagparam["btagging_algorithm"]]
                        > btagparam["btagging_WP"][params["wp"]]
                    )
                    & (events[params["coll"]].pt >= params["minpt"]),
                    axis=1,
                )
                == params["N"]
            )
        else:
            return (
                ak.sum(
                    (
                        events[params["coll"]][btagparam["btagging_algorithm"]]
                        > btagparam["btagging_WP"][params["wp"]]
                    ),
                    axis=1,
                )
                == params["N"]
            )


def nElectron(events, params, year, **kwargs):
    '''Mask for min N electrons with minpt.'''
    if params["coll"] == "ElectronGood":
        return events.nElectronGood >= params["N"]
    elif params["coll"] == "Electron":
        return events.nElectron >= params["N"]
    else:
        raise Exception(f"The collection '{params['coll']}' does not exist.")


def nMuon(events, params, year, **kwargs):
    '''Mask for min N electrons with minpt.'''
    if params["coll"] == "MuonGood":
        return events.nMuonGood >= params["N"]
    elif params["coll"] == "Muon":
        return events.nMuon >= params["N"]
    else:
        raise Exception(f"The collection '{params['coll']}' does not exist.")


##########################33
## Factory methods


def get_nBtagMin(N, minpt=0, coll="BJetGood", wp="M", name=None):
    if name == None:
        name = f"n{coll}_btagMin{N}_pt{minpt}"
    return Cut(
        name=name, params={"N": N, "coll": coll, "minpt": minpt, "wp": wp}, function=nBtagMin
    )


def get_nBtagEq(N, minpt=0, coll="BJetGood", wp="M", name=None):
    if name == None:
        name = f"n{coll}_btagEq{N}_pt{minpt}"
    return Cut(
        name=name, params={"N": N, "coll": coll, "minpt": minpt, "wp": wp}, function=nBtagEq
    )


def get_nBtag(*args, **kwargs):
    raise Exception(
        "This cut function factory is deprecated!! Use get_nBtagMin or get_nBtagEq instead."
    )


def get_nElectron(N, minpt=0, coll="ElectronGood", name=None):
    if name == None:
        name = f"n{coll}_{N}_pt{minpt}"
    return Cut(
        name=name, params={"N": N, "coll": coll, "minpt": minpt}, function=nElectron
    )


def get_nMuon(N, minpt=0, coll="MuonGood", name=None):
    if name == None:
        name = f"n{coll}_{N}_pt{minpt}"
    return Cut(name=name, params={"N": N, "coll": coll, "minpt": minpt}, function=nMuon)


def get_nPVgood(N, name=None):
    if name == None:
        name = f"nPVgood_{N}"
    return Cut(name=name, params={"N": N},
               function=lambda events, params, **kwargs: events.PV.npvsGood >= params["N"])


