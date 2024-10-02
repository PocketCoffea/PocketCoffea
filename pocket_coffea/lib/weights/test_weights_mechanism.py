from weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation

weight_f = lambda params, metadata, events, size, shape_variations: np.ones(size)*events.Jet.pt[:,0]

l1 = WeightLambda.wrap_func("jet_pt", weight_f, False, None)


weight_f_variations = lambda params, metadata, events, size, shape_variations: np.ones(size)*events.Jet.pt[:,0]

l = WeightLambda.wrap_func("jet_pt2", weight_f, True, ["up", "down"])


class WeightBTag(WeightWrapper):
    name = "btag"
    has_variations = True

    def compute(self, events, size, shape_variation):
        return np.ones(size)*events.Jet.btag[:,0]



   
