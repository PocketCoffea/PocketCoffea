from ..weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
from pocket_coffea.lib.scale_factors import sf_ele_trigger_EGM



SF_ele_trigger = WeightLambda.wrap_func(
    name="sf_ele_trigger_egm",
    function=lambda params, metadata, events, size, shape_variations:
        sf_ele_trigger_EGM(params, events, metadata["year"]),
    has_variations=True
    )