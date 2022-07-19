import numpy as np
import awkward as ak

def get_trigger_mask(events, triggers, finalstate):
    if finalstate == 'dilepton':
        trigger_mumu = np.zeros(len(events), dtype='bool')
        trigger_emu  = np.zeros(len(events), dtype='bool')
        trigger_ee   = np.zeros(len(events), dtype='bool')

        for trigger in triggers["mumu"]: trigger_mumu = trigger_mumu | events.HLT[trigger.lstrip("HLT_")]
        for trigger in triggers["emu"]:  trigger_emu  = trigger_emu  | events.HLT[trigger.lstrip("HLT_")]
        for trigger in triggers["ee"]:   trigger_ee   = trigger_ee   | events.HLT[trigger.lstrip("HLT_")]

        return ak.to_numpy(trigger_mumu | trigger_emu | trigger_ee)
    elif finalstate == 'semileptonic':
        trigger_mu = np.zeros(len(events), dtype='bool')
        trigger_e  = np.zeros(len(events), dtype='bool')

        if (triggers["mu"] == []) | (triggers["e"] == []): raise NotImplementedError

        for trigger in triggers["mu"]: trigger_mu = trigger_mu | events.HLT[trigger.lstrip("HLT_")]
        for trigger in triggers["e"]:
            if 'e_flag' in triggers:
                raise NotImplementedError
                #trigger_e  = trigger_e  | (events.HLT[trigger.lstrip("HLT_")] & events.Flag[triggers['e_flag']][0])
            else:
                trigger_e  = trigger_e  | events.HLT[trigger.lstrip("HLT_")]
        return ak.to_numpy(trigger_mu | trigger_e)
    elif finalstate == 'semileptonic_triggerSF':
        trigger_mu = np.zeros(len(events), dtype='bool')

        for trigger in triggers["mu"]: trigger_mu = trigger_mu | events.HLT[trigger.lstrip("HLT_")]

        return ak.to_numpy(trigger_mu)
    elif finalstate == 'mutag':
        trigger_mutag = np.zeros(len(events), dtype='bool')

        for trigger in triggers:
            t = trigger.lstrip("HLT_")
            if t not in events.HLT.fields:
                print(f"The HLT path '{trigger}' is not included in the Branches.")
                continue
            else:
                trigger_mutag = trigger_mutag | events.HLT[t]

        return ak.to_numpy(trigger_mutag)
    else:
        raise NotImplementedError
