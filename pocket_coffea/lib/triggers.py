import logging
import numpy as np
import awkward as ak

# Track trigger names already reported as missing so the warning is not emitted
# once per chunk (this module is imported once per worker process).
_missing_trigger_warned = set()


def remove_trigger_prefix(name, prefix):
    '''Remove `prefix` from the start of a trigger `name`.

    This is a genuine prefix removal: `str.lstrip(prefix)` must NOT be used because
    it strips any leading character contained in `prefix` (a character set), which
    silently mangles trigger names, e.g. ``"HLT_TkMu50".lstrip("HLT_")`` -> ``"kMu50"``.
    '''
    return name[len(prefix):] if name.startswith(prefix) else name


def apply_trigger_mask(events, triggers_to_apply, year, invert=False, trigger_type="HLT"):
    '''Computes the HLT/L1 trigger mask doing the OR of all the triggers in the list
    '''
    trigger_mask = np.zeros(len(events), dtype="bool")
    assert trigger_type in ["HLT", "L1"], "trigger_type must be HLT or L1"
    events_trigger= getattr(events, trigger_type)

    for trigger in triggers_to_apply:
        # Special treatment for Ele32 in 2017
        if year == "2017" and (
            (trigger == 'Ele32_WPTight_Gsf_L1DoubleEG')
            & ('Ele32_WPTight' not in events_trigger.fields)
        ):
            flag = (
                ak.sum(
                    (events.TrigObj.id == 11)
                    & ((events.TrigObj.filterBits & 1024) == 1024),
                    axis=1,
                )
                > 0
            )
            trigger_mask = trigger_mask | (events_trigger[trigger] & flag)
        else:
            if trigger in events_trigger.fields:
                trigger_mask = trigger_mask | events_trigger[trigger]
            else:
                # A requested trigger is not present in this NanoAOD file. This can be
                # legitimate (trigger menu evolution across eras / NanoAOD versions) but
                # is also how a mistyped or mangled trigger name would silently vanish
                # from the OR, so surface it loudly (once per name per worker).
                key = (trigger_type, year, trigger)
                if key not in _missing_trigger_warned:
                    _missing_trigger_warned.add(key)
                    logging.warning(
                        f"[triggers] {trigger_type} path '{trigger}' (year {year}) not found "
                        f"in events.{trigger_type} fields; it is skipped in the trigger OR. "
                        f"Check the trigger name if this is unexpected."
                    )

    if invert:
        trigger_mask = ~trigger_mask
    return trigger_mask


def get_trigger_mask_byprimarydataset(events, trigger_dict, year, isMC, primaryDatasets=None, invert=False, trigger_prefix="HLT_"):
    '''Computes the HLT/L1 trigger mask

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
    :param trigger_prefix: Prefix of the triggers in the events, can be HLT_ or L1_
    :returns: the events mask.
    '''
    cfg = trigger_dict[year]
    # If is MC
    triggers_to_apply = []
    if primaryDatasets:
        # if primary dataset is passed, take all the requested trigger
        for pd in primaryDatasets:
            triggers_to_apply += [remove_trigger_prefix(t, trigger_prefix) for t in cfg[pd]]
    else:
        if isMC:
            # If MC take the OR of all primary datasets
            for pd, trgs in cfg.items():
                triggers_to_apply += [remove_trigger_prefix(t, trigger_prefix) for t in trgs]
        else:
            # If Data take only the specific pd
            triggers_to_apply += [remove_trigger_prefix(t, trigger_prefix) for t in cfg[events.metadata["primaryDataset"]]]

    # trigger_prefix is "HLT_" or "L1_"; stripping the single trailing "_" is unambiguous.
    return apply_trigger_mask(events, triggers_to_apply, year, invert=invert, trigger_type=trigger_prefix.rstrip("_"))
