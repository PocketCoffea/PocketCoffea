import awkward as ak
import numba

@numba.jit
def get_matching_pairs_indices(idx_quark, idx_jets, builder, builder2):
    for ev_q, ev_j in zip(idx_quark, idx_jets):
        builder.begin_list()
        builder2.begin_list()
        q_done = []
        j_done = []
        for i, (q,j) in enumerate(zip(ev_q, ev_j)):
            if q not in q_done:
                if j not in j_done:
                    builder.append(i)
                    q_done.append(q)
                    j_done.append(j)
                else: 
                    builder2.append(i)
        builder.end_list()
        builder2.end_list()
    return builder, builder2

def metric_pt(obj, obj2):
    return abs(obj.pt - obj2.pt)

def object_matching(obj, obj2, dr_min, pt_min=None):
    # Compute deltaR(quark, jet) and save the nearest jet (deltaR matching)
    deltaR = ak.flatten(obj.metric_table(obj2), axis=2)
    # keeping only the pairs with a deltaR min
    maskDR = deltaR < dr_min
    if pt_min is not None:
        deltaPt = ak.flatten(obj.metric_table(obj2, metric=metric_pt), axis=2)
        maskPt = deltaPt < pt_min
        maskDR = maskDR & maskPt
    deltaRcut = deltaR[maskDR]
    idx_pairs_sorted = ak.argsort(deltaRcut, axis=1)
    pairs = ak.argcartesian([obj, obj2])[maskDR]
    pairs_sorted = pairs[idx_pairs_sorted]
    idx_obj, idx_obj2 = ak.unzip(pairs_sorted)
    
    _idx_matched_pairs, _idx_missed_pairs = get_matching_pairs_indices(idx_obj, idx_obj2, ak.ArrayBuilder(), ak.ArrayBuilder())
    idx_matched_pairs = _idx_matched_pairs.snapshot()
    idx_missed_pairs = _idx_missed_pairs.snapshot()
    # The invalid jet matches result in a None value. Only non-None values are selected.
    matched_obj  = obj[idx_obj[idx_matched_pairs]]
    matched_obj2 = obj2[idx_obj2[idx_matched_pairs]]
    deltaR_matched = deltaRcut[idx_matched_pairs]

    return matched_obj, matched_obj2, deltaR_matched