from coffea import hist
from coffea.processor.accumulator import column_accumulator
import awkward as ak

def hist_is_in(histname, hist_list, accumulator):
    isHist = type(accumulator[histname]) == hist.Hist
    return ( isHist & (histname in hist_list) )

def fill_histograms_object(processor, obj, obj_hists, event_var=False):
    accumulator = processor.output
    for histname in filter( lambda x : hist_is_in(x, obj_hists, accumulator), accumulator.keys() ):
        h = accumulator[histname]
        for category, cuts in processor._categories.items():
            if event_var:
                weight = processor.weights.weight() * processor._cuts_masks.all(*cuts)
                fields = {k: ak.fill_none(getattr(processor.events, k), -9999) for k in h.fields if k in histname}
            else:
                isnotnone = ak.flatten(~ak.is_none(obj, axis=1))
                weight = ak.flatten( processor.weights.weight() * ak.Array(ak.ones_like(obj.pt) *
                                                        processor._cuts_masks.all(*cuts)) )[isnotnone]
                fields = {k: ak.flatten(obj[k])[isnotnone] for k in h.fields if k in dir(obj)}
            h.fill(sample=processor._sample, cat=category, year=processor._year, **fields, weight=weight)

def fill_histograms_object_with_variations(processor, obj, obj_hists, systematics, event_var=False):
    accumulator = processor.output
    for histname in filter( lambda x : hist_is_in(x, obj_hists, accumulator), accumulator.keys() ):
        h = accumulator[histname]
        if type(systematics) is str:
            systematics = [systematics]

        for syst in systematics:
            for modifier, variation in zip([None, f'{syst}Up', f'{syst}Down'], ['nominal', 'up', 'down']):
                for category, cuts in processor._categories.items():
                    if event_var:
                        weight = processor.weights.weight(modifier=modifier) * processor._cuts_masks.all(*cuts)
                        fields = {k: ak.fill_none(getattr(processor.events, k), -9999) for k in h.fields if k in histname}
                    else:
                        weight = ak.flatten( processor.weights.weight(modifier=modifier) * ak.Array(ak.ones_like(obj.pt) *
                                                                processor._cuts_masks.all(*cuts)) )
                        fields = {k: ak.flatten(ak.fill_none(obj[k], -9999)) for k in h.fields if k in dir(obj)}
                    fields.update({syst : variation})
                    fields.update({s : 'nominal' for s in systematics if s != syst})
                    h.fill(sample=processor._sample, cat=category, year=processor._year, **fields, weight=weight)


def fill_column_accumulator(processor, name, cat, awk_array, save_size=False, flatten=True):
    '''
    This function filles the column_accumulator of the processor flattening and converting to numpy
    the awk_array.
    If save_file is True the number of object for each event is also saved
    '''
    acc = processor.output["columns"]
    if cat not in acc:
        raise Exception("Category not found: " + cat)

    if flatten:
        acc[cat][name][processor._sample] = column_accumulator(ak.to_numpy(ak.flatten(awk_array)))
    else:
        acc[cat][name][processor._sample] = column_accumulator(ak.to_numpy(awk_array))
    if save_size and name+"_size" in acc[cat]:
        acc[cat][name+"_size"][processor._sample] = column_accumulator(ak.to_numpy(ak.num(awk_array)))
