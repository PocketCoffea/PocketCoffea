import awkward as ak
from .cut_definition import Cut
from .triggers import get_trigger_mask


def passthrough_f(events, **kargs):
    '''
    Identity cut:  passthrough of all events.
    '''
    return ak.full_like(events.event, True, dtype=bool)


##############################
## Factory method for HLT
def _get_trigger_mask_proxy(events, params, processor_params, year, isMC, **kwargs):
    '''
    Helper function to call the HLT trigger mask
    '''
    return get_trigger_mask(
        events, processor_params.HLT_triggers, year, isMC, params["primaryDatasets"], params["invert"]
    )


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
        function=_get_trigger_mask_proxy,
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


def count_objects_lt(events, params, year, sample):
    '''
    Count the number of objects in `params["object"]` and
    keep only events with smaller (<) amount than `params["value"]`.
    '''
    mask = ak.num(events[params["object"]], axis=1) < params["value"]
    return ak.where(ak.is_none(mask), False, mask)


def count_objects_eq(events, params, year, sample):
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
                        > btagparam["btagging_WP"]
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
                        > btagparam["btagging_WP"]
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
                        > btagparam["btagging_WP"]
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
                        > btagparam["btagging_WP"]
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


def get_nBtagMin(N, minpt=0, coll="BJetGood", name=None):
    if name == None:
        name = f"n{coll}_btagMin{N}_pt{minpt}"
    return Cut(
        name=name, params={"N": N, "coll": coll, "minpt": minpt}, function=nBtagMin
    )


def get_nBtagEq(N, minpt=0, coll="BJetGood", name=None):
    if name == None:
        name = f"n{coll}_btagEq{N}_pt{minpt}"
    return Cut(
        name=name, params={"N": N, "coll": coll, "minpt": minpt}, function=nBtagEq
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
