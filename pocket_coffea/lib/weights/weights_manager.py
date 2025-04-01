from dataclasses import dataclass
import inspect
import awkward as ak
import numpy as np
import copy
from collections.abc import Callable
from collections import defaultdict

from coffea.analysis_tools import Weights
from .weights import WeightData, WeightDataMultiVariation


class WeightsManager:
    '''
    The WeightManager class handles the
    weights.  Both centrally defined weights and user-defined weights
    are handled by the WeightWrapper class. 
    
    It handles inclusive or bycategory weights for each sample.
    Weights can be inclusive or by category.
    Moreover, different samples can have different weights, as defined in the weights_configuration.

    Each WeightWrapper object defines the available variation for the weights depending on the sample,
    datataking period and other parameters.

    The configuration of samples/categories is handled by the configuration, but the availability of the
    weight is defined in the WeightWrapper class.

    '''

    def __init__(
        self,
        params,
        weightsConf,
        weightsWrappers,
        metadata,
        storeIndividual=False,
    ):
        self.params = params
        self._sample = metadata["sample"]
        self._dataset = metadata["dataset"]
        self._year = metadata["year"]
        self._isMC = metadata["isMC"]
        self.weightsConf = weightsConf
        #load the weights objects from the wrappers
        self._weightsObj = {}
        for w in weightsWrappers:
            # Check if the weight can be applied on data
            if not self._isMC and w.isMC_only:
                continue
            # this allows to have variations depending on the metadata
            self._weightsObj[w.name] = w(params, metadata)
        # Store the available variations for the weights
        self._available_weights = list(self._weightsObj.keys())
        self._available_modifiers_byweight = {}
        for w in self._available_weights:
            # the variations are customized by parameters and metadata
            if self._weightsObj[w].has_variations:
                vars = self._weightsObj[w].variations
                if len(vars) == 0:
                    # only Up and Down variations
                    self._available_modifiers_byweight[w] = [w+"Up", w+"Down"]
                else:
                    self._available_modifiers_byweight[w] = [f"{w}_{v}{var}" for v in vars for var in ["Up", "Down"]]
            else:
                self._available_modifiers_byweight[w] = []
            
        self.storeIndividual = storeIndividual
        # Dictionary keeping track of which modifier can be applied to which region
        self._available_modifiers_inclusive = []
        self._available_modifiers_bycat = defaultdict(list)
        # Loading the map in the constructor so that the histManager can access it
        # Compute first the inclusive weights
        for w in self.weightsConf["inclusive"]:
            # Save the list of availbale modifiers
            if w in self._available_modifiers_byweight:
                self._available_modifiers_inclusive += self._available_modifiers_byweight[w]

        # Now weights for dedicated categories
        if self.weightsConf["is_split_bycat"]:
            # Create the weights object only if for the current sample
            # there is a weights_by_category configuration
            for cat, ws in self.weightsConf["bycategory"].items():
                if len(ws) == 0:
                    continue
                for w in ws:
                    self._available_modifiers_bycat[cat] += self._available_modifiers_byweight[w]

        # make the variations unique
        self._available_modifiers_inclusive = set(self._available_modifiers_inclusive)
        self._available_modifiers_bycat = {
            k: set(v) for k, v in self._available_modifiers_bycat.items()
        }
        
        
    def compute(self, events, size, shape_variation="nominal"):
        '''
        Load the weights for the current chunk following the user configuration.
        This created different Weights objects for the inclusive and bycategory weights.
        '''
        _weightsCache = {}
         # looping on the weights configuration to create the

         # Weights object for the current chunk and shape variation
        # Inclusive weights
        self._weightsIncl = Weights(size, self.storeIndividual)
        self._weightsByCat = {}
        self._installed_modifiers_inclusive = []
        self._installed_modifiers_bycat = defaultdict(list)

        def __add_weight(w, weight_obj):
            installed_modifiers = []
            if w not in self._available_weights:
                # it means that the weight is defined in a processor.
                # The configurator has already checked that it is defined somewhere.
                # DO nothing
                return
            if w not in _weightsCache:
                out = self._weightsObj[w].compute(
                    events, size, shape_variation
                )
                # the output is a WeightData or WeightDataMultiVariation object
                _weightsCache[w] = out
            else:
                out = _weightsCache[w]

            # Copy the WeightData object to avoid modifying the original
            # FIXME: there is a coffea bug which modifies in place the weight variation array
            out = copy.deepcopy(out)
                
            if isinstance(out, WeightData):
                weight_obj.add(out.name, out.nominal, out.up, out.down)
                if self._weightsObj[w].has_variations:
                    installed_modifiers.append(out.name + "Up")
                    installed_modifiers.append(out.name + "Down")
            elif isinstance(out, WeightDataMultiVariation):
                weight_obj.add_multivariation(out.name, out.nominal,
                                              out.variations,
                                              out.up, out.down)
                installed_modifiers += [f"{out.name}_{v}{var}" for v in out.variations for var in ["Up", "Down"]]
            else:
                raise ValueError("The WeightWrapper must return a WeightData or WeightDataMultiVariation object")

            return installed_modifiers
            

        # Compute first the inclusive weights
        for w in self.weightsConf["inclusive"]:
            # print(f"Adding weight {w} inclusively")
            modifiers = __add_weight(w, self._weightsIncl)
            # Save the list of availbale modifiers
            self._installed_modifiers_inclusive += modifiers

        # Now weights for dedicated categories
        if self.weightsConf["is_split_bycat"]:
            # Create the weights object only if for the current sample
            # there is a weights_by_category configuration
            for cat, ws in self.weightsConf["bycategory"].items():
                if len(ws) == 0:
                    continue
                self._weightsByCat[cat] = Weights(size, self.storeIndividual)
                for w in ws:
                    modifiers = __add_weight(w, self._weightsByCat[cat])
                    self._installed_modifiers_bycat[cat] += modifiers

        # make the variations unique
        self._installed_modifiers_inclusive = set(self._installed_modifiers_inclusive)
        self._installed_modifiers_bycat = {
            k: set(v) for k, v in self._installed_modifiers_bycat.items()
        }

        _weightsCache.clear()

    def get_available_modifiers_byweight(self, weight:str):
        '''
        Return the available modifiers for the specific weight
        '''
        if weight in self._available_modifiers_byweight:
            return self._available_modifiers_byweight[weight]
        else:
            raise ValueError(f"Weight {weight} not available in the WeightsManager")

    def get_available_modifiers_bycategory(self, category=None):
        '''
        Return the available modifiers for the specific category
        '''
        if category == None:
            return self._available_modifiers_inclusive
        elif category in self._available_modifiers_bycat:
            return self._available_modifiers_bycat[category] + self._available_modifiers_inclusive


    def add_weight(self, name, nominal, up=None, down=None, category=None):
        '''
        Add manually a weight to a specific category
        '''
        if category == None:
            # add the weights to all the categories (inclusive)
            self._weightsIncl.add(name, nominal, up, down)
        else:
            self._weightsBinCat[category].add(name, nominal, up, down)

    def get_weight(self, category=None, modifier=None):
        '''
        The function returns the total weights stored in the processor for the current sample.
        If category==None the inclusive weight is returned.
        If category!=None but weights_split_bycat=False, the inclusive weight is returned.
        Otherwise the inclusive*category specific weight is returned.
        The requested variation==modifier must be available or in the
        inclusive weights, or in the bycategory weights.
        '''
        if (
            category == None
            or self.weightsConf["is_split_bycat"] == False
            or category not in self._weightsByCat
        ):
            # return the inclusive weight
            if modifier != None and modifier not in self._installed_modifiers_inclusive:
                raise ValueError(
                    f"Modifier {modifier} not available in inclusive category"
                )
            return self._weightsIncl.weight(modifier=modifier)

        elif category and self.weightsConf["is_split_bycat"] == True:
            # Check if this category has a bycategory configuration
            if modifier == None:
                # Get the nominal both for the inclusive and the split weights
                return (
                    self._weightsIncl.weight() * self._weightsByCat[category].weight()
                )
            else:
                mod_incl = modifier in self._installed_modifiers_inclusive
                mod_bycat = modifier in self._installed_modifiers_bycat[category]

                if mod_incl and not mod_bycat:
                    # Get the modified inclusive and nominal bycat
                    return (
                        self._weightsIncl.weight(modifier=modifier)
                        * self._weightsByCat[category].weight()
                    )
                elif not mod_incl and mod_bycat:
                    # Get the nominal by cat and modified inclusive
                    return self._weightsIncl.weight() * self._weightsByCat[
                        category
                    ].weight(modifier=modifier)
                else:
                    raise ValueError(
                        f"Modifier {modifier} not available in category {category}"
                    )
