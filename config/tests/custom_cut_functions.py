import awkward as ak

##################
# Common preselection functions
def dilepton(events, params, year, sample, **kwargs):

    MET = events[params["METbranch"][year]]

    # Masks for same-flavor (SF) and opposite-sign (OS)
    SF = ((events.nMuonGood == 2) & (events.nElectronGood == 0)) | (
        (events.nMuonGood == 0) & (events.nElectronGood == 2)
    )
    OS = events.ll.charge == 0
    # SFOS = SF & OS
    not_SF = (events.nMuonGood == 1) & (events.nElectronGood == 1)

    mask = (
        (events.nLeptonGood == 2)
        & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_lepton"])
        & (events.nJetGood >= params["njet"])
        & (events.nBJetGood >= params["nbjet"])
        & (MET.pt > params["met"])
        & OS
        & (events.ll.mass > params["mll"])  # Opposite sign
        &
        # If same-flavour we exclude a mll mass interval
        (
            (
                SF
                & (
                    (events.ll.mass < params["mll_SFOS"]["low"])
                    | (events.ll.mass > params["mll_SFOS"]["high"])
                )
            )
            | not_SF
        )
    )

    # Pad None values with False
    return ak.where(ak.is_none(mask), False, mask)


def semileptonic(events, params, year, sample, **kwargs):

    MET = events[params["METbranch"][year]]

    has_one_electron = events.nElectronGood == 1
    has_one_muon = events.nMuonGood == 1

    mask = (
        (events.nLeptonGood == 1)
        &
        # Here we properly distinguish between leading muon and leading electron
        (
            (
                has_one_electron
                & (
                    ak.firsts(events.LeptonGood.pt)
                    > params["pt_leading_electron"][year]
                )
            )
            | (
                has_one_muon
                & (ak.firsts(events.LeptonGood.pt) > params["pt_leading_muon"][year])
            )
        )
        & (events.nJetGood >= params["njet"])
        & (events.nBJetGood >= params["nbjet"])
        & (MET.pt > params["met"])
    )

    # Pad None values with False
    return ak.where(ak.is_none(mask), False, mask)


def semileptonic_triggerSF(events, params, year, sample, **kwargs):

    has_one_electron = events.nElectronGood == 1
    has_one_muon = events.nMuonGood == 1

    mask = (
        (events.nLeptonGood == 2)
        &
        # Here we properly distinguish between leading muon and leading electron
        (
            (
                has_one_electron
                & (
                    ak.firsts(events.ElectronGood.pt)
                    > params["pt_leading_electron"][year]
                )
            )
            & (
                has_one_muon
                & (ak.firsts(events.MuonGood.pt) > params["pt_leading_muon"][year])
            )
        )
        & (events.nJetGood >= params["njet"])
    )
    # & (events.nBJetGood >= params["nbjet"]) & (MET.pt > params["met"]) )

    # Pad None values with False
    return ak.where(ak.is_none(mask), False, mask)
