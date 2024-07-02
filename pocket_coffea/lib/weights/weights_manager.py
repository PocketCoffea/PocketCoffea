from dataclasses import dataclass
import inspect
import awkward as ak
import numpy as np
from collections.abc import Callable
from collections import defaultdict

from coffea.analysis_tools import Weights
from weights import WeightData, WeightDataMultiVariation


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
        size,
        events,
        shape_variation,
        metadata,
        storeIndividual=False,
    ):
        self.params = params
        self._sample = metadata["sample"]
        self._dataset = metadata["dataset"]
        self._year = metadata["year"]
        self._shape_variation = shape_variation
        self.weightsConf = weightsConf
        #load the weights objects from the wrappers
        self._weightsObj = {}
        for w in weightsWrappers:
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
                    self._available_modifiers_byweight[w] = ["Up", "Down"]
                else:
                    self._available_modifiers_byweight[w] = [v + var for v in vars for var in ["Up", "Down"]]
            
        
        self.storeIndividual = storeIndividual
        self.size = size
        # looping on the weights configuration to create the
        # Weights object for the current sample
        # Inclusive weights
        self._weightsIncl = Weights(size, storeIndividual)
        self._weightsByCat = {}
        # Dictionary keeping track of which modifier can be applied to which region
        self._available_modifiers_inclusive = []
        self._available_modifiers_bycat = defaultdict(list)

        
    def compute(self):
        '''
        Load the weights for the current chunk following the user configuration.
        This created different Weights objects for the inclusive and bycategory weights.
        '''
        _weightsCache = {}

        def __add_weight(w, weight_obj):
            installed_modifiers = []
            if w not in self._available_weights:
                # it means that the weight is defined in a processor.
                # The configurator has already checked that it is defined somewhere.
                # DO nothing
                return
            if w not in _weightsCache:
                out = self._weightsObj[w].compute(
                    events, self.size, self._shape_variation
                )
                # the output is a WeightData or WeightDataMultiVariation object
                _weightsCache[w] = out
                if isinstance(out, WeightData):
                    weight_obj.add(out.name, out.nominal, out.up, out.down)
                    if self._weightsObj[w].has_variations:
                        installed_modifiers.append(out.name + "Up")
                        installed_modifiers.append(out.name + "Down")
                elif isinstance(out, WeightDataMultiVariation):
                    weight_obj.add_multivariation(out.name, out.nominal, out.variations, out.up, out.down)
                    installed_modifiers += [out.name + v + var for v in out.variations for var in ["Up", "Down"]]
                else:
                    raise ValueError("The WeightWrapper must return a WeightData or WeightDataMultiVariation object")
            return installed_modifiers
            

        # Compute first the inclusive weights
        for w in self.weightsConf["inclusive"]:
            # print(f"Adding weight {w} inclusively")
            modifiers = __add_weight(w, self._weightsIncl)
            # Save the list of availbale modifiers
            self._available_modifiers_inclusive += modifiers

        # Now weights for dedicated categories
        if self.weightsConf["is_split_bycat"]:
            # Create the weights object only if for the current sample
            # there is a weights_by_category configuration
            for cat, ws in self.weightsConf["bycategory"].items():
                if len(ws) == 0:
                    continue
                self._weightsByCat[cat] = Weights(size, storeIndividual)
                for w in ws:
                    modifiers = __add_weight(w, self._weightsByCat[cat])
                    self._available_modifiers_bycat[cat] += modifiers

        # make the variations unique
        self._available_modifiers_inclusive = set(self._available_modifiers_inclusive)
        self._available_modifiers_bycat = {
            k: set(v) for k, v in self._available_modifiers_bycat.items()
        }

        # print("Weights modifiers inclusive", self._available_modifiers_inclusive)
        # print("Weights modifiers bycat", self._available_modifiers_bycat)
        # Clear the cache once the Weights objects have been added
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
        
    def _compute_weight(self, weight_name, events, shape_variation):
        '''
        Predefined common weights.
        The function return a list of tuples containing a
        weighs and its variations in each tuple::

                [("name", nominal, up, donw),
                 ("name", nominal, up, down)]

        Each variation is then added to the Weights object by the caller
        in the constructor.
        '''
        if weight_name == "genWeight":
            return [('genWeight', events.genWeight)]
        if weight_name == "signOf_genWeight":
            return [('signOf_genWeight', np.sign(events.genWeight))]
        elif weight_name == 'lumi':
            return [
                (
                    'lumi',
                    ak.full_like(
                        events.genWeight, self.params.lumi.picobarns[self._year]["tot"]
                    ),
                )
            ]
        elif weight_name == 'XS':
            return [('XS', ak.full_like(events.genWeight, self._xsec))]
        elif weight_name == 'pileup':
            # Pileup reweighting with nominal, up and down variations
            return [('pileup', *sf_pileup_reweight(self.params, events, self._year))] 
        elif weight_name == 'sf_ele_reco':
            # Electron reco and id SF with nominal, up and down variations
            return [('sf_ele_reco', *sf_ele_reco(self.params, events, self._year))]
        elif weight_name == "sf_ele_id":
            return [('sf_ele_id', *sf_ele_id(self.params, events, self._year))]
        elif weight_name == 'sf_mu_id':
            # Muon id and iso SF with nominal, up and down variations
            return [('sf_mu_id', *sf_mu(self.params, events, self._year, 'id'))]
        elif weight_name == "sf_mu_iso":
            return [('sf_mu_iso', *sf_mu(self.params, events, self._year, 'iso'))]
        elif weight_name == "sf_mu_trigger":
            return [
                ('sf_mu_trigger', *sf_mu(self.params, events, self._year, 'trigger'))
            ]
        elif weight_name == "sf_ele_trigger":

            # Get all the nominal and variation SF
            if shape_variation == "nominal":
                sf_ele_trigger_vars = (
                    self.params.systematic_variations.weight_variations.sf_ele_trigger[
                        self._year
                    ]
                )
                triggersf = sf_ele_trigger(
                    self.params,
                    events,
                    self._year,
                    variations=["nominal"] + sf_ele_trigger_vars,
                )
                # BE AWARE --> COFFEA HACK FOR MULTIPLE VARIATIONS
                for var in sf_ele_trigger_vars:
                    # Rescale the up and down variation by the nominal one to
                    # avoid double counting of the central SF when adding the weights
                    # as separate entries in the Weights object.
                    triggersf[var][1] = triggersf[var][1] / triggersf["nominal"][0]
                    triggersf[var][2] = triggersf[var][2] / triggersf["nominal"][0]
            else:
                # Only the nominal if there is a shape variation
                triggersf = sf_ele_trigger(
                    self.params, events, self._year, variations=["nominal"]
                )

            # return the nominal and everything
            return [
                (f"sf_ele_trigger_{var}", *weights)
                for var, weights in triggersf.items()
            ]

        elif weight_name == 'sf_btag':
            # Get all the nominal and variation SF
            if shape_variation == "nominal":
                btag_vars = self.params.systematic_variations.weight_variations.sf_btag[
                    self._year
                ]
                btagsf = sf_btag(
                    self.params,
                    events.JetGood,
                    self._year,
                    njets=events.nJetGood,
                    variations=["central"] + btag_vars,
                )
                # BE AWARE --> COFFEA HACK FOR MULTIPLE VARIATIONS
                for var in btag_vars:
                    # Rescale the up and down variation by the central one to
                    # avoid double counting of the central SF when adding the weights
                    # as separate entries in the Weights object.
                    btagsf[var][1] = btagsf[var][1] / btagsf["central"][0]
                    btagsf[var][2] = btagsf[var][2] / btagsf["central"][0]

            elif "JES_" in shape_variation:
                # Compute the special version of the btagSF for JES variation
                # The name conversion is done inside the btag sf function.
                btagsf = sf_btag(
                    self.params,
                    events.JetGood,
                    self._year,
                    njets=events.nJetGood,
                    variations=[shape_variation],
                )

            else:
                # Only the nominal if there is a shape variation (?)
                btagsf = sf_btag(
                    self.params,
                    events.JetGood,
                    self._year,
                    njets=events.nJetGood,
                    variations=["central"],
                )

            # return the nominal and everything
            return [(f"sf_btag_{var}", *weights) for var, weights in btagsf.items()]

        elif weight_name == 'sf_btag_calib':
            # This variable needs to be defined in another method
            jetsHt = ak.sum(abs(events.JetGood.pt), axis=1)
            return [
                (
                    "sf_btag_calib",
                    sf_btag_calib(
                        self.params, self._sample, self._year, events.nJetGood, jetsHt
                    ),
                )
            ]

        elif weight_name == 'sf_ctag':
            #print("Doing ctag SFs", shape_variation)

            if shape_variation == "nominal":
                ctag_vars = self.params.systematic_variations.weight_variations.sf_ctag[
                    self._year
                ]
                ctagsf = sf_ctag(
                    self.params,
                    events.JetGood,
                    self._year,
                    njets=events.nJetGood,
                    variations=["central"] + ctag_vars,
                )

                for var in ctag_vars:
                    #print("ctag_var", var)
                    #print(ctagsf[var])
                    # Rescale the up and down variation by the central one to
                    # avoid double counting of the central SF when adding the weights
                    # as separate entries in the Weights object.
                    ctagsf[var][1] = ctagsf[var][1] / ctagsf["central"][0]
                    ctagsf[var][2] = ctagsf[var][2] / ctagsf["central"][0]

            else:
                ctagsf = sf_ctag(
                    self.params,
                    events.JetGood,
                    self._year,
                    njets=events.nJetGood,
                    variations=["central"],
                )

            return [(f"sf_ctag_{var}", *weights) for var, weights in ctagsf.items()]

        elif weight_name == 'sf_ctag_calib':
            jetsHt = ak.sum(abs(events.JetGood.pt), axis=1)
            return [
                (
                    "sf_ctag_calib",
                    sf_ctag_calib(
                        self.params, self._dataset, self._year, events.nJetGood, jetsHt
                    ),
                )
            ]


        elif weight_name == 'sf_jet_puId':
            return [
                (
                    'sf_jet_puId',
                    *sf_jet_puId(
                        self.params,
                        events.JetGood,
                        self._year,
                        njets=events.nJetGood,
                    ),
                )
            ]
        elif weight_name == 'sf_L1prefiring':
            return [('sf_L1prefiring', *sf_L1prefiring(events))]

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
            if modifier != None and modifier not in self._available_modifiers_inclusive:
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
                mod_incl = modifier in self._available_modifiers_inclusive
                mod_bycat = modifier in self._available_modifiers_bycat[category]

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