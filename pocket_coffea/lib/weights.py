from dataclasses import dataclass
import inspect
import awkward as ak
import numpy as np
from collections.abc import Callable
from collections import defaultdict
from abc import ABC, abstractmethod

class WeightWrapperMeta(type):
    # Metaclass to store the dictionary
    # of all the weight classes
    weight_classes = {}
    def __new__(metacls, name, bases, clsdict):
        cls = super().__new__(metacls, name, bases, clsdict)
        if name not in metacls.weight_classes:
            metacls.weight_classes[name] = cls
        return cls


### Dataclasses to represent the weights to be passed to the WeightsManager
@dataclass
class WeightData:
    name: str
    nominal: np.ndarray
    up: np.ndarray = None
    down: np.ndarray = None

@dataclass
class WeightDataMultiVariation:
    name: str
    nominal: np.ndarray
    variations: list[str]
    up: list[np.ndarray]
    down: list[np.ndarray] = None

    
class WeightWrapper(ABC, metaclass=WeightWrapperMeta):
    '''
    Base class creating a schema creating Weights for the analysis.
    The class allows the user to define weights with variations
    that depends on the params and metadata of the specific chunk.
    WeightWrapper instances are passed to the WeightsManager and stored.
    During the processing, the processor object creates a WeightsManager
    object which uses the WeightWrapper object to get the weights.
    In that step, the `prepare` function is called to customize the
    weights computation depending on the current parameters and chunk metadata
    (like era, year, isMC, isData..).
    The variations are dynamically computed by the WeightWrapper object.
    The HistManager will ask the WeightsManager to have the available weights
    variations for the current chunk and will store them in the output file.
    The variations can be then customized by parameters and metadata.
  
    '''
    def __init__(self, name: str,
                 has_variations: bool):
        self.name = name
        self.has_variations = has_variations        
        self._variations = []

    def prepare(self, params, metadata) -> None:
        # setup things for specific year for example
        pass

    @abstractmethod
    def __call__(self, events, size,
                 shape_variation,
                 params=None, metadata=None) -> WeightData | WeightDataMultiVariation:
        pass

    @property
    def variations(self):
        return self._variations

    def serialize(self, src_code=False):
        out = {
            "name": self.name,
            "class": {
                "name": self.__class__.__name__,
                "module": self.__class__.__module__,
                "src_file": inspect.getsourcefile(self.__class__),
                "f_hash": hash(self.__class__),
            },
        }
        if src_code: #to be tested
            out["function"]["src_code"] = inspect.getsource(self.__class__)
        return out



class WeightLamba(WeightWrapper):
    '''
    WeightLambda is a WeightWrapper object for a lambda function that returns a weight.
    It is useful for quick definition of a weights without the need of defining a new derived class from WeightWrapper. 

    - name: name of the weight
    - function:  function defining the weights of the events chunk.
                 signuture (params, events, size, metadata:dict, shape_variation:str)
    - has_variations: if the function returns variations
    - variations: list of variations that the function returns

    The function must return the weights as WeightData or WeightDataMultiVariation objects.
    '''

    def __init__(self, name: str, function: Callable,
                 has_variations:  bool = False,
                 variations: list[str] = []):
        super().__init__(name, has_variations)
        self.function = function
        self._variations = variations

    def __call__(self, events, size,
                    shape_variation,
                    params=None, metadata=None) -> WeightData | WeightDataMultiVariation:
        return self.function(events, size, shape_variation, params, metadata)
    
    def serialize(self, src_code=False):
        out = {
            "name": self.name,
            "function": {
                "name": self.function.__name__,
                "module": self.function.__module__,
                "src_file": inspect.getsourcefile(self.function),
                "f_hash": hash(self.function),
            },
        }
        if src_code: #to be tested
            out["function"]["src_code"] = inspect.getsource(self.function)
        return out
