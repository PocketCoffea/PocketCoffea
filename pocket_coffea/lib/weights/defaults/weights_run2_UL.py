from ..weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation


genWeight = WeightLambda.wrap_func(
    name="genWeight",
    function=lambda params, metadata, events, size, shape_variations:
    WeightData("genWeight", events.genWeight),
    )

signOfGenWeight = WeightLambda.wrap_func(
    name="signOfGenWeight",
    function=lambda params, metadata, events, size, shape_variations:
    WeightData("signOfGenWeight", np.sign(events.genWeight)),
    )

lumi = WeightLambda.wrap_func(
    name="lumi",
    function=lambda params, metadata, events, size, shape_variations:
    WeightData("lumi", ak.full_like(events.genWeight, params.lumi.picobarns[metadata["year"]["tot"]]))
    )


XS = WeightLambda.wrap_func(
    name = "XS",
    function = lambda params, metadata, events, size, shape_variations:
    WeightData("XS", ak.full_like(events.genWeight, metadata["xs"]))
)




# List with default classes for Run2 UL weights
run2_weights = [
    genWeight,
    signOfGenWeight,
    lumi,
    XS
]



