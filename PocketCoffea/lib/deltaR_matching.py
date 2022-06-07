from collections.abc import Iterable
import numpy as np
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

# This function takes as arguments the indices of two collections of objects that have been
# previously matched and ordered by pt, and return the indices padded with None values where
# no match has been found. In the implementation, the assumption is that the number of
# un-matched obj (gen-jets) is less or equal than the number of un-matched obj2 (jets).
@numba.jit
def get_matching_objects_indices_padnone(idx_matched_obj, idx_matched_obj2, builder, builder2):
    for ev1_match, ev2_match in zip(idx_matched_obj, idx_matched_obj2):
        builder.begin_list()
        builder2.begin_list()
        row1_length = 1 + max(ev1_match)
        row2_length = 1 + max(ev2_match)
        missed = 0
        for i in range(row2_length):
            if i in ev2_match:
                #print(i, row1_length)
                builder2.append(i)
                if i-missed < row1_length:
                    builder.append(ev1_match[i-missed])
            else:
                builder.append(None)
                builder2.append(None)
                missed += 1
        builder.end_list()
        builder2.end_list()
        #print()
    return builder, builder2

def metric_pt(obj, obj2):
    return abs(obj.pt - obj2.pt)

def object_matching(obj, obj2, dr_min, pt_min=None):
    # Compute deltaR(quark, jet) and save the nearest jet (deltaR matching)
    deltaR = ak.flatten(obj.metric_table(obj2), axis=2)
    # Keeping only the pairs with a deltaR min
    maskDR = deltaR < dr_min
    if pt_min is not None:
        deltaPt_table = obj.metric_table(obj2, metric=metric_pt)
        # Check if the pt cut is an iterable or a scalar
        if isinstance(pt_min, Iterable):
            # Broadcast and flatten pt_min array in order to match the shape of the metric_table()
            pt_min_broadcast = ak.broadcast_arrays(pt_min[:,np.newaxis], deltaPt_table)[0]
            pt_min  = ak.flatten(pt_min_broadcast, axis=2)
        deltaPt = ak.flatten(deltaPt_table, axis=2)
        maskPt = deltaPt < pt_min
        maskDR = maskDR & maskPt
    idx_pairs_sorted = ak.argsort(deltaR, axis=1)
    pairs = ak.argcartesian([obj, obj2])
    pairs_sorted  = pairs[idx_pairs_sorted]
    deltaR_sorted = deltaR[idx_pairs_sorted]
    maskDR_sorted = maskDR[idx_pairs_sorted]
    idx_obj, idx_obj2 = ak.unzip(pairs_sorted)
    
    _idx_matched_pairs, _idx_missed_pairs = get_matching_pairs_indices(idx_obj, idx_obj2, ak.ArrayBuilder(), ak.ArrayBuilder())
    idx_matched_pairs = _idx_matched_pairs.snapshot()
    idx_missed_pairs = _idx_missed_pairs.snapshot()
    # The indices related to the invalid jet matches are skipped
    idx_matched_obj  = idx_obj[idx_matched_pairs]
    idx_matched_obj2 = idx_obj2[idx_matched_pairs]
    deltaR_matched = deltaR_sorted[idx_matched_pairs]
    maskDR_matched = maskDR[idx_matched_pairs]
    # In order to keep track of the invalid jet matches, the indices of the second collection
    # are sorted, and the order of the first collection of indices is reshuffled accordingly
    obj2_order = ak.argsort(idx_matched_obj2)
    idx_obj_sorted  = idx_matched_obj[obj2_order]
    idx_obj2_sorted = idx_matched_obj2[obj2_order]
    deltaR_sorted = deltaR_matched[obj2_order]
    maskDR_sorted = maskDR_matched[obj2_order]
    idx_obj_sorted_padnone, idx_obj2_sorted_padnone = get_matching_objects_indices_padnone(idx_obj_sorted, idx_obj2_sorted, ak.ArrayBuilder(), ak.ArrayBuilder())
    # Finally the objects are sliced through the padded indices
    # In this way, to a None entry in the indices will correspond a None entry in the object
    matched_obj  = ak.mask(obj, idx_obj_sorted_padnone)
    matched_obj2 = ak.mask(obj2, idx_obj2_sorted_padnone)

    return matched_obj, matched_obj2, deltaR_matched