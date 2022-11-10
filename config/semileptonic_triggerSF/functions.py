import numpy as np
import awkward as ak
from pocket_coffea.lib.cut_definition import Cut

def trigger_mask(events, params, **kwargs):
    mask = np.zeros(len(events), dtype='bool')
    for trigger in params["triggers"]:
        mask = mask | events.HLT[trigger]
    assert (params["category"] in ["pass", "fail"]), "The allowed categories for the trigger selection are 'pass' and 'fail'"
    if params["category"] == "fail":
        mask = ~mask
    return mask

def trigger_mask_2017(events, params, **kwargs):
    mask = np.zeros(len(events), dtype='bool')
    for trigger in params["triggers"]:
        if not trigger in events.HLT.fields: continue
        if trigger != 'Ele32_WPTight_Gsf_L1DoubleEG':
            mask = mask | events.HLT[trigger]
        elif ((trigger == 'Ele32_WPTight_Gsf_L1DoubleEG') & ('Ele32_WPTight' not in events.HLT.fields)):
            flag = ak.sum( (events.TrigObj.id == 11) & ((events.TrigObj.filterBits & 1024) == 1024), axis=1 ) > 0
            mask = mask | ( events.HLT[trigger] & flag )
    assert (params["category"] in ["pass", "fail"]), "The allowed categories for the trigger selection are 'pass' and 'fail'"
    if params["category"] == "fail":
        mask = ~mask
    return mask

def ht_above(events, params, **kwargs):
    mask = events["JetGood_Ht"] > params["minht"]
    return mask

def ht_below(events, params, **kwargs):
    mask = (events["JetGood_Ht"] >= 0) & (events["JetGood_Ht"] < params["maxht"])
    return mask

def get_trigger_passfail(triggers, category):
    return Cut(
        name=f"{'_'.join(triggers)}_{category}",
        params={"triggers": triggers, "category": category},
        function=trigger_mask
    )

def get_trigger_passfail_2017(triggers, category):
    return Cut(
        name=f"{'_'.join(triggers)}_{category}",
        params={"triggers": triggers, "category": category},
        function=trigger_mask_2017
    )

def get_ht_above(minht, name=None):
    if name == None:
        name = f"minht{minht}"
    return Cut(
        name=name,
        params={"minht": minht},
        function=ht_above
    )

def get_ht_below(maxht, name=None):
    if name == None:
        name = f"maxht{maxht}"
    return Cut(
        name=name,
        params={"maxht": maxht},
        function=ht_below
    )
