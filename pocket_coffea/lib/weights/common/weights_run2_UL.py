import awkward as ak
import correctionlib
import numpy as np

from ..weights import WeightData, WeightDataMultiVariation, WeightLambda, WeightWrapper


def get_ele_trigger_sf(params, year, pt, eta, phi, counts, variations):
    """Compute the electron trigger scale factors for Run2 (Custom Implementation)"""

    electronSF = params["lepton_scale_factors"]["electron_sf"]
    # translate the `year` key into the corresponding key in the correction file provided by the EGM-POG
    year_pog = electronSF["era_mapping"][year]

    electron_correctionset = correctionlib.CorrectionSet.from_file(
        electronSF.trigger_sf[year]["file"]
    )
    map_name = electronSF.trigger_sf[year]["name"]

    output = {}
    trigger_path = electronSF.trigger_sf[year]["path"]
    for variation in variations:
        if variation == "nominal":
            output[variation] = [
                electron_correctionset[map_name].evaluate(
                    year_pog,
                    "sf",
                    trigger_path,
                    eta.to_numpy(),
                    pt.to_numpy(),
                )
            ]
        else:
            # Nominal sf==1
            nominal = np.ones_like(pt.to_numpy())
            # Systematic variations
            output[variation] = [
                nominal,
                electron_correctionset[map_name].evaluate(
                    year_pog,
                    f"{variation}up",
                    trigger_path,
                    eta.to_numpy(),
                    pt.to_numpy(),
                ),
                electron_correctionset[map_name].evaluate(
                    year_pog,
                    f"{variation}down",
                    electronSF.trigger_sf[year]["path"],
                    eta.to_numpy(),
                    pt.to_numpy(),
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


########################################################################
# More complicated WeightWrapper defining dynamic variations depending
# on the data taking period


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
