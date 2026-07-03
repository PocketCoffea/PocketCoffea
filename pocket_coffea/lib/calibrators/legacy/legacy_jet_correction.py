import awkward as ak

from pocket_coffea.lib.jets import add_jec_variables, get_rho


def jet_correction(params, events, jets, factory, jet_type, chunk_metadata, cache):
    nano_version = chunk_metadata.get("nano_version", 9)
    rho = get_rho(events, nano_version)

    # Note: PNet jet regression should be applied via PNetRegressionCalibrator
    # before calling this function, not within jet_correction itself.
    # The regression code has been moved to pocket_coffea.lib.calibrators.common.pnet_regression.PNetRegressionCalibrator
             
    if chunk_metadata["isMC"]:
        corrected_jets = factory["MC"][jet_type][chunk_metadata["year"]].build(
            add_jec_variables(jets, rho, isMC=True), cache
        )
        # update the rawFactor of the corrected jets
        corrected_jets = ak.with_field(
            corrected_jets,
            ak.where(
                corrected_jets.pt != 0,
                1 - corrected_jets.pt_raw / corrected_jets.pt,
                0,
            ),
            "rawFactor",
        )
        return corrected_jets
    else:
        if chunk_metadata["era"] not in factory["Data"][jet_type][chunk_metadata["year"]]:
            raise Exception(f"Factory for {jet_type} in {chunk_metadata['year']} and era {chunk_metadata['era']} not found. Check your jet calibration files.")
        
        corrected_jets = factory["Data"][jet_type][chunk_metadata["year"]][chunk_metadata["era"]].build(
            add_jec_variables(jets, rho, isMC=False), cache
        )
        # update the rawFactor of the corrected jets
        corrected_jets = ak.with_field(
            corrected_jets,
            ak.where(
                corrected_jets.pt != 0,
                1 - corrected_jets.pt_raw / corrected_jets.pt,
                0,
            ),
            "rawFactor",
        )
        return corrected_jets