from ..weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
import numpy as np

from pocket_coffea.lib.scale_factors import (
    sf_ele_reco,
    sf_ele_id,
    sf_ele_trigger,
    sf_mu,
    sf_btag,
    sf_btag_calib,
    sf_ctag,
    sf_ctag_calib,
    sf_jet_puId,
    sf_L1prefiring,
    sf_pileup_reweight,
)

genWeight = WeightLambda.wrap_func(
    name="genWeight",
    function=lambda params, metadata, events, size, shape_variations:
            events.genWeight,
    has_variations=False
    )

signOfGenWeight = WeightLambda.wrap_func(
    name="signOf_genWeight",
    function=lambda params, metadata, events, size, shape_variations:
       np.sign(events.genWeight),
    has_variations=False
    )

lumi = WeightLambda.wrap_func(
    name="lumi",
    function=lambda params, metadata, events, size, shape_variations:
        np.ones(len(events)) * params.lumi.picobarns[metadata["year"]]["tot"],
    has_variations=False
    )


XS = WeightLambda.wrap_func(
    name = "XS",
    function = lambda params, metadata, events, size, shape_variations:
         np.ones(len(events)) * metadata["xsec"],
    has_variations=False
)


pileup = WeightLambda.wrap_func(
    name="pileup",
    function=lambda params, metadata, events, size, shape_variations:
        sf_pileup_reweight(params, events, metadata["year"]),
    has_variations=True  # no list of variations it means only up and down
    )


sf_ele_reco = WeightLambda.wrap_func(
    name="sf_ele_reco",
    function=lambda params, metadata, events, size, shape_variations:
        sf_ele_reco(params, events, metadata["year"]),
    has_variations=True
    )

sf_ele_id = WeightLambda.wrap_func(
    name="sf_ele_id",
    function=lambda params, metadata, events, size, shape_variations:
        sf_ele_id(params, events, metadata["year"]),
    has_variations=True
    )

sf_mu_id = WeightLambda.wrap_func(
    name="sf_mu_id",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'id'),
    has_variations=True
    )

sf_mu_iso = WeightLambda.wrap_func(
    name="sf_mu_iso",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'iso'),
    has_variations=True
    )

sf_mu_trigger = WeightLambda.wrap_func(
    name="sf_mu_trigger",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'trigger'),
    has_variations=True
    )


# List with default classes for common weights
common_weights = [
    genWeight,
    signOfGenWeight,
    lumi,
    XS,
    pileup,
    sf_ele_reco,
    sf_ele_id,
    sf_mu_id,
    sf_mu_iso
]



