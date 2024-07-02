import pytest
from pocket_coffea.lib.weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
import awkward as ak


def test_weight_name_redefinition():
    # It is not possible to redefine the name of a weight
    w1 = WeightLambda.wrap_func(name="genWeight_test",
                                function=None)

    with pytest.raises(ValueError):
        w2 = WeightLambda.wrap_func(name="genWeight_test",
                                function=None)


def test_weight_retrieval():
    # Test the retrieval of the weight classes
    w1 = WeightLambda.wrap_func(name="A",
                                function=None)
    w2 = WeightLambda.wrap_func(name="B",
                                function=None)

    assert WeightWrapper.weight_classes["A"] == w1
    assert WeightWrapper.weight_classes["B"] == w2


        
def test_weight_lambda_wrapper():
    
    def test_func(params, metadata, events, size, shape_variations):
        return WeightData("test_weight", events*2)

    genWeight = WeightLambda.wrap_func(name="genWeight2", function=test_func)

    g = genWeight()
    w = g.compute(ak.Array([1, 2, 3]), 3, "nominal")

    assert isinstance(w, WeightData)
    assert w.name == "genWeight2"
    assert ak.all(w.nominal == ak.Array([2, 4, 6]))
    

def test_weight_lambda_wrapper_multi_variation():
    
    def test_func(params, metadata, events, size, shape_variations):
        return WeightDataMultiVariation("test_weight", events*2, ["stat", "syst"],
                                        up=[events*3, events*4], down=[events*0.5, events*0.25])
    
    genWeight = WeightLambda.wrap_func(name="genWeight3", function=test_func)
    
    g = genWeight()
    data = ak.Array([1, 2, 3])
    w = g.compute(data, 3, "nominal")
    
    assert isinstance(w, WeightDataMultiVariation)
    assert w.name == "genWeight3"
    assert ak.all(w.nominal == data*2)
    assert ak.all(w.up[0] == data*3)
    assert ak.all(w.down[0] == data*0.5)
    assert ak.all(w.up[1] == data*4)
    assert ak.all(w.down[1] == data*0.25)


def test_weight_lambda_wrapper_wrong_output():
     def test_func(params, metadata, events, size, shape_variations):
         return "wrong"

     genWeight = WeightLambda.wrap_func(name="genWeight4", function=test_func)

     g = genWeight()
     with pytest.raises(ValueError):
         w = g.compute(ak.Array([1, 2, 3]), 3, "nominal")


def test_weight_lambda_wrapper_wrong_output_2():
        def test_func(params, metadata, events, size, shape_variations):
            return [1,2,3,4]  # only numpy arrays or awkward arrays are allowed
    
        genWeight = WeightLambda.wrap_func(name="genWeight5", function=test_func)
    
        g = genWeight()
        with pytest.raises(ValueError):
            w = g.compute(ak.Array([1, 2, 3]), 3, "nominal")
