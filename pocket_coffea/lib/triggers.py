import numpy as np
import awkward as ak


def get_trigger_mask(events, trigger_dict, year, isMC, primaryDatasets=None, invert=False):
    '''Computes the HLT trigger mask

    The function reads the triggers configuration and create the mask.
    For MC the OR of all the triggers is performed.
    For DATA only the corresponding primary dataset triggers are applied.
    if primaryDataset param is passed, the correspoding triggers are applied, both
    on DATA and MC.

    :param events: Awkward arrays
    :param key: Key of the triggers config
    :param year: year of the dataset
    :param isMC: MC/data
    :param primaryDatasets: default None. Overwrite the configuration and applied the specified
                            list of primary dataset triggers both on MC and data
    :param invert: Invert the mask, returning which events do not path ANY of the triggers
    :returns: the events mask.
    '''
    cfg = trigger_dict[year]
    # If is MC
    triggers_to_apply = []
    if primaryDatasets:
        # if primary dataset is passed, take all the requested trigger
        for pd in primaryDatasets:
            triggers_to_apply += cfg[pd]
    else:
        if isMC:
            # If MC take the OR of all primary datasets
            for pd, trgs in cfg.items():
                triggers_to_apply += trgs
        else:
            # If Data take only the specific pd
            triggers_to_apply += cfg[events.metadata["primaryDataset"]]

    # create the mask
    trigger_mask = np.zeros(len(events), dtype="bool")

    for trigger in triggers_to_apply:
        # Special treatment for Ele32 in 2017
        if year == "2017" and (
            (trigger == 'Ele32_WPTight_Gsf_L1DoubleEG')
            & ('Ele32_WPTight' not in events.HLT.fields)
        ):
            flag = (
                ak.sum(
                    (events.TrigObj.id == 11)
                    & ((events.TrigObj.filterBits & 1024) == 1024),
                    axis=1,
                )
                > 0
            )
            trigger_mask = trigger_mask | (events.HLT[trigger] & flag)
        else:
            trigger_mask = trigger_mask | events.HLT[trigger.lstrip("HLT_")]

    if invert:
        trigger_mask = ~trigger_mask
    return trigger_mask
