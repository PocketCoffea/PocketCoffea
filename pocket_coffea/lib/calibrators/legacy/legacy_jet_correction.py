import awkward as ak

from pocket_coffea.lib.jets import add_jec_variables


def jet_correction(params, events, jets, factory, jet_type, chunk_metadata, cache):
    if chunk_metadata["year"] in ['2016_PreVFP', '2016_PostVFP','2017','2018']:
        rho = events.fixedGridRhoFastjetAll
    else:
        rho = events.Rho.fixedGridRhoFastjetAll

    # Note: PNet jet regression should be applied via PNetRegressionCalibrator
    # before calling this function, not within jet_correction itself.
    # The regression code has been moved to pocket_coffea.lib.calibrators.common.pnet_regression.PNetRegressionCalibrator
             
    if chunk_metadata["isMC"]:
        corrected_jets = factory["MC"][jet_type][chunk_metadata["year"]].build(
            add_jec_variables(jets, rho, isMC=True), cache
        )
        # update the rawFactor of the corrected jets
        corrected_jets=ak.with_field(corrected_jets, 1 - corrected_jets.pt_raw / corrected_jets.pt, "rawFactor")
        return corrected_jets
    else:
        if chunk_metadata["era"] not in factory["Data"][jet_type][chunk_metadata["year"]]:
            raise Exception(f"Factory for {jet_type} in {chunk_metadata['year']} and era {chunk_metadata['era']} not found. Check your jet calibration files.")
        
        corrected_jets = factory["Data"][jet_type][chunk_metadata["year"]][chunk_metadata["era"]].build(
            add_jec_variables(jets, rho, isMC=False), cache
        )
        # update the rawFactor of the corrected jets
        corrected_jets=ak.with_field(corrected_jets, 1 - corrected_jets.pt_raw / corrected_jets.pt, "rawFactor")
        return corrected_jets