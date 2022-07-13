from coffea import hist
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
                keys = [k for k in h.fields if k in histname]
                isnotnone = ak.flatten(~ak.is_none(getattr(processor.events, keys[0])))
                weight = processor.weights.weight() * processor._cuts_masks.all(*cuts)[isnotnone]
                fields = {k: ak.flatten(getattr(processor.events, k))[isnotnone] for k in h.fields if k in histname}
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

        modifiers = { 'nominal' : None }
        # The systematic variations are done only for Monte Carlo samples. For data, only the 'nominal' variation is filled
        if processor._isMC:
            for syst in systematics:
                modifiers.update({ f'{syst}Up' : f'{syst}Up', f'{syst}Down' : f'{syst}Down' })

        for (var, modifier) in modifiers.items():
            for category, cuts in processor._categories.items():
                if event_var:
                    weight = processor.weights.weight(modifier=modifier) * processor._cuts_masks.all(*cuts)
                    fields = {k: ak.fill_none(getattr(processor.events, k), -9999) for k in h.fields if k in histname}
                else:
                    weight = ak.flatten( processor.weights.weight(modifier=modifier) * ak.Array(ak.ones_like(obj.pt) *
                                                            processor._cuts_masks.all(*cuts)) )
                    fields = {k: ak.flatten(ak.fill_none(obj[k], -9999)) for k in h.fields if k in dir(obj)}
                h.fill(sample=processor._sample, cat=category, year=processor._year, var=var, **fields, weight=weight)
