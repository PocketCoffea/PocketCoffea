
from coffea import hist
from coffea.processor.accumulator import column_accumulator
import awkward as ak

def hist_is_in(histname, hist_list, accumulator):
    isHist = type(accumulator[histname]) == hist.Hist
    return ( isHist & (histname in hist_list) )

def fill_histograms_object(processor, obj, obj_hists, event_var=False, split_eras=False):
    accumulator = processor.output
    for histname in filter( lambda x : hist_is_in(x, obj_hists, accumulator), accumulator.keys() ):
        h = accumulator[histname]
        for category, cuts in processor._categories.items():
            if event_var:
                keys = [k for k in h.fields if k in histname]
                isnotnone = ~ak.is_none(getattr(processor.events, keys[0]))
                weight = ( processor.get_weight(category) * processor._cuts_masks.all(*cuts) )[isnotnone]
                fields = {k: getattr(processor.events, k)[isnotnone] for k in h.fields if k in histname}
            else:
                isnotnone = ak.flatten(~ak.is_none(obj, axis=1))
                weight = ak.flatten( processor.get_weight(category) * ak.Array(ak.ones_like(obj.pt) *
                                                        processor._cuts_masks.all(*cuts)) )[isnotnone]
                fields = {k: ak.flatten(obj[k])[isnotnone] for k in h.fields if k in dir(obj)}
            if split_eras:
                h.fill(sample=processor._sample, cat=category, year=processor._year, era=processor._era, **fields, weight=weight)
            else:
                h.fill(sample=processor._sample, cat=category, year=processor._year, **fields, weight=weight)

def fill_histograms_object_with_variations(processor, obj, obj_hists, systematics, event_var=False, split_eras=False, triggerSF=False):
    accumulator = processor.output
    for histname in filter( lambda x : hist_is_in(x, obj_hists, accumulator), accumulator.keys() ):
        h = accumulator[histname]
        if type(systematics) is str:
            systematics = [systematics]

        modifiers = { 'nominal' : None }
        # The systematic variations are done only for Monte Carlo samples. For data, only the 'nominal' variation is filled
        if processor._isMC:
            for syst in systematics:
                modifiers.update({ f'{syst}Up' : f'{syst}Up', f'{syst}Down' : f'{syst}Down' })

        for (var, modifier) in modifiers.items():
            for category, cuts in processor._categories.items():
                if event_var:
                    keys = [k for k in h.fields if k in histname]
                    isnotnone = ~ak.is_none(getattr(processor.events, keys[0]))
                    if triggerSF:
                        key_correction = processor._triggerSF_map[category]
                        weight = ( processor.triggerSF_weights[key_correction].weight() * processor.get_weight(category, modifier=modifier) * processor._cuts_masks.all(*cuts) )[isnotnone]
                    else:
                        weight = ( processor.get_weight(category, modifier=modifier) * processor._cuts_masks.all(*cuts) )[isnotnone]
                    fields = {k: getattr(processor.events, k)[isnotnone] for k in h.fields if k in histname}
                else:
                    isnotnone = ak.flatten(~ak.is_none(obj, axis=1))
                    if triggerSF:
                        key_correction = processor._triggerSF_map[category]
                        weight = ak.flatten( processor.triggerSF_weights[key_correction].weight() * processor.get_weight(category, modifier=modifier) * ak.Array(ak.ones_like(obj.pt) *
                                             processor._cuts_masks.all(*cuts)) )[isnotnone]
                    else:
                        weight = ak.flatten( processor.get_weight(category, modifier=modifier) * ak.Array(ak.ones_like(obj.pt) *
                                             processor._cuts_masks.all(*cuts)) )[isnotnone]
                    fields = {k: ak.flatten(obj[k])[isnotnone] for k in h.fields if k in dir(obj)}
                if split_eras:
                    h.fill(sample=processor._sample, cat=category, year=processor._year, era=processor._era, var=var, **fields, weight=weight)
                else:
                    h.fill(sample=processor._sample, cat=category, year=processor._year, var=var, **fields, weight=weight)

def fill_column_accumulator(output, sample,name, cats, awk_array, save_size=True, flatten=True):
    '''
    This function filles the column_accumulator of the processor flattening and converting to numpy
    the awk_array.
    If save_file is True the number of object for each event is also saved
    '''
    for cat in cats:
        if cat not in output:
            raise Exception("Category not found: " + cat)

        if flatten:
            output[cat][name][sample] = column_accumulator(ak.to_numpy(ak.flatten(awk_array)))
        else:
            output[cat][name][sample] = column_accumulator(ak.to_numpy(awk_array))
        if save_size and name+"_size" in output[cat]:
            output[cat][name+"_size"][sample] = column_accumulator(ak.to_numpy(ak.num(awk_array)))

