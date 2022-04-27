import awkward as ak

# lepton scale factors
def compute_lepton_weights(leps, evaluator, SF_list, lepton_eta=None, year=None):

    lepton_pt = leps.pt
    if lepton_eta is None:
        lepton_eta = leps.eta
    weights = np.ones(len(lepton_pt))

    for SF in SF_list:
        if SF.startswith('mu'):
            if year=='2016':
                if 'trigger' in SF:
                    x = lepton_pt
                    y = np.abs(lepton_eta)
                else:
                    x = lepton_eta
                    y = lepton_pt
            else:
                x = lepton_pt
                y = np.abs(lepton_eta)
        elif SF.startswith('el'):
            if 'trigger' in SF:
                x = lepton_pt
                y = lepton_eta
            else:
                x = lepton_eta
                y = lepton_pt
        else:
            raise Exception(f'unknown SF name {SF}')
        weights = weights*evaluator[SF](x, y)
        #weights *= evaluator[SF](x, y)

    per_event_weights = ak.prod(weights, axis=1)
    return per_event_weights
