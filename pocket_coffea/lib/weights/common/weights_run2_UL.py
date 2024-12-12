import awkward as ak
import correctionlib
import numpy as np

from ..weights import WeightData, WeightDataMultiVariation, WeightLambda, WeightWrapper


def get_ele_trigger_sf(params, year, pt, eta, phi, counts, variations):
    """Compute the electron trigger scale factors for Run2.
    Note that this a custom implementation to apply the single electron trigger scale factors in the ttH(bb) Run-2 analysis.
    This implementation is not supported by the EGM-POG.
    The procedure to compute the trigger SF correction map can be found at https://github.com/PocketCoffea/AnalysisConfigs/tree/main/configs/ttHbb/semileptonic/semileptonic_triggerSF .
    The structure of the correction map and the naming convention follows the one of the implementation.
    """

    electronSF = params["lepton_scale_factors"]["electron_sf"]

    electron_correctionset = correctionlib.CorrectionSet.from_file(
        electronSF.trigger_sf[year]["file"]
    )
    map_name = electronSF.trigger_sf[year]["name"]

    output = {}
    for variation in variations:
        if variation == "nominal":
            output[variation] = [
                electron_correctionset[map_name].evaluate(
                    variation,
                    pt.to_numpy(),
                    eta.to_numpy(),
                )
            ]
            nominal = output[variation][0]
        else:
            # Systematic variations
            output[variation] = [
                nominal,
                electron_correctionset[map_name].evaluate(
                    f"{variation}Up",
                    pt.to_numpy(),
                    eta.to_numpy(),
                ),
                electron_correctionset[map_name].evaluate(
                    f"{variation}Down",
                    pt.to_numpy(),
                    eta.to_numpy(),
                ),
            ]
        for i, sf in enumerate(output[variation]):
            output[variation][i] = ak.unflatten(sf, counts)

    return output


# UL custom electron trigger scale factors
def sf_ele_trigger(params, events, year, variations=["nominal"]):
    """
    This function computes the semileptonic electron trigger SF by considering the leading electron in the event.
    This computation is valid only in the case of the semileptonic final state.
    Additionally, also the up and down variations of the SF for a set of systematic uncertainties are returned.
    """
    coll = params.lepton_scale_factors.electron_sf.collection
    ele_pt = events[coll].pt
    ele_eta = events[coll].etaSC

    ele_pt_flat, ele_eta_flat, ele_counts = (
        ak.flatten(ele_pt),
        ak.flatten(ele_eta),
        ak.num(ele_pt),
    )
    sf_dict = get_ele_trigger_sf(
        params,
        year,
        pt=ele_pt_flat,
        eta=ele_eta_flat,
        phi=None,
        counts=ele_counts,
        variations=variations,
    )

    for variation in sf_dict.keys():
        for i, sf in enumerate(sf_dict[variation]):
            sf_dict[variation][i] = ak.prod(sf, axis=1)

    return sf_dict


def sf_qcd_renorm_scale(events):
    '''Up and down variations for the renormalization scale weights.
    The up variation of the renormalization scale weight is defined as the ratio of the weight
    with renormalization scale increased by a factor of 2 to the nominal weight ([7]/[4]).
    The down variation of the renormalization scale weight is defined as the ratio of the weight
    with renormalization scale decreased by a factor of 2 to the nominal weight ([1]/[4]).
    Conventions for the LHEScaleWeight (valid for NanoAODv9) are:
    LHE scale variation weights (w_var / w_nominal);
    [0] is renscfact=0.5d0 facscfact=0.5d0 ;
    [1] is renscfact=0.5d0 facscfact=1d0 ;
    [2] is renscfact=0.5d0 facscfact=2d0 ;
    [3] is renscfact=1d0 facscfact=0.5d0 ;
    [4] is renscfact=1d0 facscfact=1d0 ;
    [5] is renscfact=1d0 facscfact=2d0 ;
    [6] is renscfact=2d0 facscfact=0.5d0 ;
    [7] is renscfact=2d0 facscfact=1d0 ;
    [8] is renscfact=2d0 facscfact=2d0'''
    sf_up = events.LHEScaleWeight[:,7]/events.LHEScaleWeight[:,4]
    sf_down = events.LHEScaleWeight[:,1]/events.LHEScaleWeight[:,4]
    nom = ak.ones_like(sf_up)

    return nom, sf_up, sf_down

def sf_qcd_factor_scale(events):
    '''Up and down variations for the factorization scale weights.
    The up variation of the factorization scale weight is defined as the ratio of the weight
    with factorization scale increased by a factor of 2 to the nominal weight ([5]/[4]).
    The down variation of the factorization scale weight is defined as the ratio of the weight
    with factorization scale decreased by a factor of 2 to the nominal weight ([3]/[4]).
    Conventions for the LHEScaleWeight (valid for NanoAODv9) are:
    LHE scale variation weights (w_var / w_nominal);
    [0] is renscfact=0.5d0 facscfact=0.5d0 ;
    [1] is renscfact=0.5d0 facscfact=1d0 ;
    [2] is renscfact=0.5d0 facscfact=2d0 ;
    [3] is renscfact=1d0 facscfact=0.5d0 ;
    [4] is renscfact=1d0 facscfact=1d0 ;
    [5] is renscfact=1d0 facscfact=2d0 ;
    [6] is renscfact=2d0 facscfact=0.5d0 ;
    [7] is renscfact=2d0 facscfact=1d0 ;
    [8] is renscfact=2d0 facscfact=2d0'''
    sf_up = events.LHEScaleWeight[:,5]/events.LHEScaleWeight[:,4]
    sf_down = events.LHEScaleWeight[:,3]/events.LHEScaleWeight[:,4]
    nom = ak.ones_like(sf_up)

    return nom, sf_up, sf_down


class SF_ele_trigger(WeightWrapper):
    name = "sf_ele_trigger"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # Getting the variations from the parameters depending on the year
        self._variations = (
            params.systematic_variations.weight_variations.sf_ele_trigger[
                metadata["year"]
            ]
        )

    def compute(self, events, size, shape_variation):
        if shape_variation == "nominal":
            out = sf_ele_trigger(
                self._params,
                events,
                self._metadata["year"],
                variations=["nominal"] + self._variations,
            )
            # This is a dict with variation: [nom, up, down]
            return WeightDataMultiVariation(
                name=self.name,
                nominal=out["nominal"][0],
                variations=self._variations,
                up=[out[var][1] for var in self._variations],
                down=[out[var][2] for var in self._variations],
            )

        else:
            out = sf_ele_trigger(
                self._params, events, self._metadata["year"], variations=["nominal"]
            )
            return WeightData(name=self.name, nominal=out["nominal"][0])

SF_QCD_renorm_scale = WeightLambda.wrap_func(
    name="sf_qcd_renorm_scale",
    function=lambda params, metadata, events, size, shape_variations:
        sf_qcd_renorm_scale(events),
    has_variations=True
    )

SF_QCD_factor_scale = WeightLambda.wrap_func(
    name="sf_qcd_factor_scale",
    function=lambda params, metadata, events, size, shape_variations:
        sf_qcd_factor_scale(events),
    has_variations=True
    )
