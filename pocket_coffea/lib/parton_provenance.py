from numba import njit
import numpy as np 



@njit
def reverse_index_array(idxGs, firstgenpart_idxG_numpy,
                        genparts_offsets, nevents):
    '''
    Convert the global index array (no masks) to a global index array
    considering masked events.

    - idxGs: array of global indices
    - firstgenpart_idxG_numpy: array of the global index of the first genpart of the array
    - genparts_offsets: array of the offsets of the genparts array in the masked array
    - nevents: number of events
    '''
    out = np.zeros(len(idxGs), dtype="int64")
    for i, idxG in enumerate(idxGs):
        event_idex = 0
        genpart_idx = 0
        found = False
        for j in range(nevents-1):
            if (firstgenpart_idxG_numpy[j+1] - idxG)>0:
                genpart_idx = idxG - firstgenpart_idxG_numpy[j]
                out[i] = genparts_offsets[j] + genpart_idx
                found=True
                break
        if not found:
            genpart_idx = idxG - firstgenpart_idxG_numpy[nevents-1]
            out[i] = genparts_offsets[nevents-1] + genpart_idx

    return out


@njit
def analyze_W_flat(W_idx, children_idx, genparts_statusFlags, genparts_pdgId,
                    firstgenpart_idxG_numpy, genparts_offsets, nevents):
    '''
    Get the W decay products for each W in the array.
    - W_idx: array of the global indices of the Ws
    - children_idx: array of the children global indices (no masks)
    - genparts_statusFlags/pdgId: array of genparts
    - firstgenpart_idxG_numpy: array of the global index of the first genpart of the array
    - genparts_offsets: array of the offsets of the genparts array in the masked array
    - nevents: number of events

    Returns:
    - is_leptonic: array of booleans, True if the W decayed leptonically
    - idx_children: array of the global indices of the children of the Ws (indices in the flat genparts array)
    '''
    
    is_leptonic = np.zeros(len(W_idx), dtype="bool")
    idx_children = np.zeros((len(W_idx),2), dtype="int")
    # We don't have a events, structure, using only flat collections
    
    # First go to children until don't find anymore the same copy
    for iev, W_id in enumerate(W_idx):
        # Special case where the W is not found
        # This is needed to allow the analysis of the direct decay case (no W saved)
        if W_id == -1:
            continue
        #print("-----\nevent: ", iev)
        current_part = W_id # start from the W
        while True:
            #print(iev, current_part, genparts[current_part].pdgId)
            if genparts_statusFlags[current_part] & 1<<13:  # is lastCopy
                break
            else:
                current_part = reverse_index_array(children_idx[current_part],
                                                   firstgenpart_idxG_numpy,
                                                   genparts_offsets,
                                                   nevents)[0]
                #print(current)
                # ASSUMING THAT THE FIRST CHILDREN IS THE COPY
#                 if abs(genparts[current_part].pdgId) != 24:
#                     print("BIG ERROR")
        
            
        # We have now at least 2 children, checking if they are leptonic
        for ich,  cidx in enumerate(reverse_index_array(children_idx[current_part],
                                                        firstgenpart_idxG_numpy,
                                                        genparts_offsets,
                                                        nevents)):
            if 11<= abs(genparts_pdgId[cidx]) <= 16:
                #print("Found leptonic")
                is_leptonic[iev] = True
            idx_children[iev, ich] = cidx
        
    return is_leptonic, idx_children


@njit
def analyze_parton_decays_flat_nomesons(parts_idx, children_idx,
                                        genparts_eta, genparts_phi, genparts_pt, genparts_pdgId,
                                        max_deltaR,
                                        firstgenpart_idxG_numpy,
                                        genparts_offsets, nevents):
    '''
    Analyze the decay of the partons in the array. Excludes mesons and only looking for quakrs and gluons after FSR.
    Inputs:
    - parts_idx: array of the global indices of the partons
    - children_idx: array of the children global indices (no masks)
    - genparts_eta/phi/pt/pdgId: array of genparts
    - max_deltaR: maximum deltaR to consider a decay
    - firstgenpart_idxG_numpy: array of the global index of the first genpart of the array
    - genparts_offsets: array of the offsets of the genparts array in the masked array
    - nevents: number of events
    
    Expects parts_idx in global index (with maskes).

    Returns:
    - out: array of the global indices of the children of the partons (indices in the flat genparts array)
    '''
    
    out = np.zeros(parts_idx.shape, dtype="int64")
    
    for iev in range(parts_idx.shape[0]):
        
    #for iev, (gp, ch_idx) in enumerate(zip(genparts, children_idx)):
        for ipart in range(parts_idx.shape[1]):
            p_id = parts_idx[iev][ipart]
            eta_original = genparts_eta[p_id]
            phi_original = genparts_phi[p_id]
            
            
            # Take all the children and consider the one with the highest pt. 
            max_pt = -1
            max_pt_idx = -1
            childr_idxs = reverse_index_array(children_idx[p_id],
                                              firstgenpart_idxG_numpy,
                                              genparts_offsets, nevents)
            
            for r_ich in childr_idxs:
                genp_eta = genparts_eta[r_ich]
                genp_phi = genparts_phi[r_ich]
                # Do no consider mesons
                if abs(genparts_pdgId[r_ich]) > 21:
                    continue
                
                if np.sqrt((eta_original-genp_eta)**2 + (phi_original-genp_phi)**2 ) > max_deltaR:
                    continue
                    
                child_pt = genparts_pt[r_ich]
                
                #print(child_pt)
                if child_pt > max_pt:
                    max_pt_idx = r_ich
                    max_pt = child_pt
            if max_pt == -1:
                max_pt_idx = p_id
            
            out[iev, ipart] = max_pt_idx
        
    return out

#############################################################
#############################################################




@njit
def get_partons_provenance_ttHbb(pdgIds, array_builder):
    """
    1=higgs,
    2=hadronic top bquark,
    3=leptonic top bquark,
    4=additional radiation
    5=hadronic W (from top) decay quarks
    """
    for ids in pdgIds:
        from_part = [-1] * len(ids)
        if len(ids) == 7:
            offset = 1
            # the first particle is the additional radiations
            from_part[0] = 4
        else:
            offset = 0
        # From part ==
        """
        1=higgs,
        2=hadronic top bquark,
        3=leptonic top b,
        4=additional radiation
        """
        if len(ids) in [6, 7]:
            top = []
            antitop = []
            hadr_top = 0  # 1==top, -1 antitop
            if ids[0 + offset] == 5:
                top.append(0 + offset)
            if ids[1 + offset] == -5:
                antitop.append(1 + offset)
            # Now looking at the top products
            # pair = [ids[2+offset], ids[3+offset]]
            # Antitop
            if ids[2 + offset] == 3 and ids[3 + offset] == -4:   
                antitop += [2 + offset, 3 + offset]
                hadr_top = -1
            if ids[2 + offset] == 3 and ids[3 + offset] == -2:
                antitop += [2 + offset, 3 + offset]
                hadr_top = -1
            if ids[2 + offset] == 1 and ids[3 + offset] == -2:
                antitop += [2 + offset, 3 + offset]
                hadr_top = -1
            if ids[2 + offset] == 1 and ids[3 + offset] == -4:
                antitop += [2 + offset, 3 + offset]
                hadr_top = -1
            # top
            if ids[2 + offset] == -3 and ids[3 + offset] == 4:
                top += [2 + offset, 3 + offset]
                hadr_top = 1
            if ids[2 + offset] == -3 and ids[3 + offset] == 2:
                top += [2 + offset, 3 + offset]
                hadr_top = 1
            if ids[2 + offset] == -1 and ids[3 + offset] == 2:
                top += [2 + offset, 3 + offset]
                hadr_top = 1
            if ids[2 + offset] == -1 and ids[3 + offset] == 4:
                top += [2 + offset, 3 + offset]
                hadr_top = 1

            if hadr_top == -1:
                # The antitop has decayed hadronically
                from_part[antitop[0]] = 2
                from_part[antitop[1]] = 5
                from_part[antitop[2]] = 5
                from_part[top[0]] = 3
            if hadr_top == 1:
                # The top has decayed hadronically
                from_part[top[0]] = 2
                from_part[top[1]] = 5
                from_part[top[2]] = 5
                from_part[antitop[0]] = 3

            # The higgs is at the bottom
            from_part[4 + offset] = 1
            from_part[5 + offset] = 1
        else:
            from_part[0 + offset] = 2  # 5
            from_part[1 + offset] = 3  # -5
            from_part[2 + offset] = 1  # Higgs
            from_part[3 + offset] = 1  # Higgs
            # This is not the semileptonic case
            # For the moment let's leave the -1 in the matching
        array_builder.begin_list()
        for i in from_part:
            array_builder.append(i)
        array_builder.end_list()
    return array_builder

#############################################################
#############################################################


@njit
def get_partons_provenance_ttHbb_dileptonic(pdgIds, array_builder):
    """
    This function assigns particle provenance (origin) for b-quarks in a dileptonic ttH -> bb process,
    where the Higgs decays into two b-quarks, and both the top and anti-top quarks decay, producing
    additional b-quarks.

    1 = higgs bquarks,
    2 = top bquark,
    3 = antitop bquark,
    4 = additional radiation (if present)
    """

    for ids in pdgIds:
        from_part = [-1] * max(4, len(ids))
        if len(ids) == 5:
            offset = 1
            from_part[0] = 4
        else:
            offset = 0

        if len(ids) == 4 or len(ids) == 5:
            if ids[0 + offset] == 5:
                from_part[0 + offset] = 2
            if ids[1 + offset] == -5:
                from_part[1 + offset] = 3

            from_part[2 + offset] = 1
            from_part[3 + offset] = 1
        else:

            from_part[0 + offset] = 2
            from_part[1 + offset] = 3
            from_part[2 + offset] = 1
            from_part[3 + offset] = 1

        array_builder.begin_list()
        for i in from_part:
            array_builder.append(i)
        array_builder.end_list()
    return array_builder

@njit
def get_partons_provenance_ttbb4F(pdgIds, array_builder):
    """
    1=g->bb,
    2=hadronic top bquark,
    3=leptonic top bquark,
    4=additional radiation
    5=hadronic W (from top) decay quarks
    """
    for ids in pdgIds:
        from_part = [-1] * len(ids)
        if len(ids) == 7:
            offset = 1
            # the third particle is the additional radiations
            from_part[2] = 4
        else:
            offset = 0
        # The first two particles are always the additional g->bb particles
        from_part[0] = 1
        from_part[1] = 1
        
        if len(ids) in [6,7]:
            top = []
            antitop = []
            hadr_top = 0  # 1==top, -1 antitop
            if ids[2 + offset] == 5:
                top.append(2 + offset)
            if ids[3 + offset] == -5:
                antitop.append(3 + offset)
            # Now looking at the top products
            # pair = [ids[4+offset], ids[5+offset]]
            
            first_idx = 4 + offset
            second_idx = 5 + offset
            # Antitop
            if ids[first_idx] == 3 and ids[second_idx] == -4:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 3 and ids[second_idx] == -2:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 1 and ids[second_idx] == -2:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 1 and ids[second_idx] == -4:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            # top
            if ids[first_idx] == -3 and ids[second_idx] == 4:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -3 and ids[second_idx] == 2:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -1 and ids[second_idx] == 2:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -1 and ids[second_idx] == 4:
                top += [first_idx, second_idx]
                hadr_top = 1
    
            #print(hadr_top, top, antitop)
            if hadr_top == -1:
                # The antitop has decayed hadronically
                from_part[antitop[0]] = 2
                from_part[antitop[1]] = 5
                from_part[antitop[2]] = 5
                from_part[top[0]] = 3
            if hadr_top == 1:
                # The top has decayed hadronically
                from_part[top[0]] = 2
                from_part[top[1]] = 5
                from_part[top[2]] = 5
                from_part[antitop[0]] = 3

        else:
            pass
            # For the moment let's leave the -1 in the matching
        array_builder.begin_list()
        for i in from_part:
            array_builder.append(i)
        array_builder.end_list()
    return array_builder


@njit
def get_partons_provenance_tt5F(pdgIds, array_builder):
    """
    2=hadronic top bquark,
    3=leptonic top bquark,
    4=additional radiation
    5=hadronic W (from top) decay quarks
    """
    for ids in pdgIds:
        from_part = [-1] * len(ids)
        if len(ids) == 5:
            offset = 1
            # the first particle is the additional radiation
            from_part[0] = 4
        else:
            offset = 0
        
        if len(ids) in [4,5]:
            top = []
            antitop = []
            hadr_top = 0  # 1==top, -1 antitop
            if ids[0 + offset] == 5:
                top.append(0 + offset)
            if ids[1 + offset] == -5:
                antitop.append(1 + offset)
            # Now looking at the top products
            # pair = [ids[4+offset], ids[5+offset]]
            
            first_idx = 2 + offset
            second_idx = 3 + offset
            # Antitop
            if ids[first_idx] == 3 and ids[second_idx] == -4:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 3 and ids[second_idx] == -2:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 1 and ids[second_idx] == -2:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            if ids[first_idx] == 1 and ids[second_idx] == -4:
                antitop += [first_idx, second_idx]
                hadr_top = -1
            # top
            if ids[first_idx] == -3 and ids[second_idx] == 4:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -3 and ids[second_idx] == 2:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -1 and ids[second_idx] == 2:
                top += [first_idx, second_idx]
                hadr_top = 1
            if ids[first_idx] == -1 and ids[second_idx] == 4:
                top += [first_idx, second_idx]
                hadr_top = 1
    
            #print(hadr_top, top, antitop)
            if hadr_top == -1:
                # The antitop has decayed hadronically
                from_part[antitop[0]] = 2
                from_part[antitop[1]] = 5
                from_part[antitop[2]] = 5
                from_part[top[0]] = 3
            if hadr_top == 1:
                # The top has decayed hadronically
                from_part[top[0]] = 2
                from_part[top[1]] = 5
                from_part[top[2]] = 5
                from_part[antitop[0]] = 3

        else:
            pass
            # For the moment let's leave the -1 in the matching
        array_builder.begin_list()
        for i in from_part:
            array_builder.append(i)
        array_builder.end_list()
    return array_builder
