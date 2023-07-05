from numba import njit


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
