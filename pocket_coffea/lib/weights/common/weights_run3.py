from pocket_coffea.lib.scale_factors import sf_ele_trigger

from ..weights import WeightData, WeightDataMultiVariation, WeightLambda, WeightWrapper

SF_ele_trigger = WeightLambda.wrap_func(
    name="sf_ele_trigger",
    function=lambda params, metadata, events, size, shape_variations: sf_ele_trigger(
        params, events, metadata["year"]
    ),
    has_variations=True,
)
