from itertools import chain
from dataclasses import dataclass, field
from functools import wraps
from collections.abs import Callable
from coffea.analysis_tools import Weights
# Scale factors functions
from .scale_factors import sf_ele_reco, sf_ele_id, sf_mu, sf_btag, sf_btag_calib, sf_jet_puId
# Framework parameters
from ..parameters.lumi import lumi, goldenJSON
from ..parameters.samples import samples_info
from ..parameters.btag import btag, btag_variations

@dataclass
class WeightCustom():
    name: str
    variations: List[str] = None
    function: Callable[[ak.Array, Weights], ak.Array]


class WeightManager():    
    '''
    The WeightManager class handles the
    weights defined by the framework and custom weights created
    by the user in the processor or by configuration.

    It handles inclusive or bycategory weights for each sample.
    Weights can be inclusive or by category.
    Moreover, different samples can have different weights, as defined in the weights_configuration.
    The name of the weights available in the current workflow are defined in the class methods "available_weights"
     
    '''
    @classmethod
    def available_weights(cls):
        return ['genWeight', 'lumi', 'XS', 'pileup',
                'sf_ele_reco_id', 'sf_mu_id_iso', 'sf_btag',
                'sf_btag_calib', 'sf_jet_puId']

    @classmethod
    def available_variations(cls):
        return ["nominal", "pileup","sf_ele_reco", "sf_ele_id",
                "sf_mu_id", "sf_mu_iso","sf_jet_puId"] + \
                set(chain.from_iterable(btag_variations))


    def __init__(self, weightsConf, size, events, storeIndividual=False):
        self._sample = events.metadata["sample"]
        self._year = events.metadata["year"]
        self.weightsConf = weightConf
        self.storeIndividual = storeIndividual
        self.size = size
        # looping on the weights configuration to create the
        # Weights object for the current sample        
        #Inclusive weights
        self._weightsIncl = Weights(size, storeIndividual)
        self._weightsByCat = {}

        _weightsCache = {}

        def __add_weight(w, weight_obj):
            # If the Weight is a name look into the predefined weights
            if isinstance(w, str):
                if w not in _weightsCache:
                    _weightsCache[w] = self._compute_weight(w, events)
                for we in _weightsCache[w]:
                    weight_obj.add(*we)
            # If the Weight is a Custom weight just run the function
            elif isinstance(w, WeightCustom):
                if w.name not in _weightsCache:
                    _weightsCache[w.name] =  w.function( self._weights_incl, events)
                for we in _weightsCache[w.name]:
                    weight_obj.add(*we)

        # Compute first the inclusive weights
        for w in self.weightConf["inclusive"]:
            __add_weight(w, self._weightsIncl)

        # Now weights for dedicated categories
        if self.weightConf["is_split_bycat"]:
            #Create the weights object only if for the current sample there is a weights_by_category
            #configuration
            for cat, ws in self.weightConf["bycategory"].items():
                if len(ws) == 0: continue
                self._weightsByCat[cat] = Weights(size, storeIndividual)
                for w in ws:
                    __add_weight(w, self._weightsByCat[cat])

        #Clear the cache once the Weights objects have been added
        _weightsCache.clear()
                       
    def _compute_weight(self, weight_name, events):
        '''
        Predefined common weights
        '''
        if weight_name == "genWeight":
            return [('genWeight', events.genWeight)]
        elif weight_name == 'lumi':
            return [('lumi', ak.full_like(events.genWeight, lumi[self._year]["tot"]))]
        elif weight_name == 'XS':
            return [('XS', ak.full_like(events.genWeight, samples_info[self._sample]["XS"]))]
        elif weight_name == 'pileup':
            # Pileup reweighting with nominal, up and down variations
            return [('pileup', *sf_pileup_reweight(events, self._year))]
        elif weight_name == 'sf_ele_reco_id':
            # Electron reco and id SF with nominal, up and down variations
            return [('sf_ele_reco', *sf_ele_reco(events, self._year)),
                    ('sf_ele_id',   *sf_ele_id(events, self._year))]
        elif weight_name == 'sf_mu_id_iso':
            # Muon id and iso SF with nominal, up and down variations
            return [('sf_mu_id',  *sf_mu(events, self._year, 'id')),
                    ('sf_mu_iso', *sf_mu(events, self._year, 'iso'))]
        elif weight_name == 'sf_btag':
            btag_vars = btag_variations[self._year]
            # Get all the nominal and variation SF
            btagsf = sf_btag(events.JetGood, events.metadata["btag"]['btagging_algorithm'], self._year,
                             variations=["central"]+btag_vars,
                             njets = events.njet)
            # BE AWARE --> COFFEA HACK
            for var in btag_vars:
                # Rescale the up and down variation by the central one to
                # avoid double counting of the central SF when adding the weights
                # as separate entries in the Weights object.
                btagsf[var][1] /= btagsf["central"][0]
                btagsf[var][2] /= btagsf["central"][0]

            return [(f"sf_btag_{var}", *weights) for var, weights in btasf.items()]

        elif weight_name == 'sf_btag_calib':
            # This variable needs to be defined in another method
            jetsHt = ak.sum(abs(events.JetGood.pt), axis=1)
            return [("sf_btag_calib", sf_btag_calib(self._sample, self._year, events.njet, jetsHt ) )]

        elif weight_name == 'sf_jet_puId':
            return [('sf_jet_puId', *sf_jet_puId(events.JetGood, events.metadata["finalstate"],
                                                       self._year, njets=events.njet))]

                
    def add_weight(self, name, nominal, up=None, down=None, category=None):
        '''
        Add manually a weight to a specific category'''
        if category== None:
            # add the weights to all the categories (inclusive)
            self._weightsIncl.add(name, nominal, up, down)
        else:
            self._weightsByCat[category].add(name, nominal, up, down)


    
    def get_weight(self, category=None, modifier=None):
        '''
        The function returns the total weights stored in the processor for the current sample.
        If category==None the inclusive weight is returned.
        If category!=None but weights_split_bycat=False, the inclusive weight is returned.
        Otherwise the inclusive*category specific weight is returned.
        '''
        if category==None or self.weightConfig["is_split_bycat"] == False:
            # return the inclusive weight
            return self._weightsIncl.weight(modifier=modifier)
        
        elif category and self.weightConfig["is_split_bycat"] == True:
            if category not in self._categories:
                raise Exception(f"Requested weights for non-existing category: {category}")
            # moltiply the inclusive weight and the by category one
            return self._weightsIncl.weight(modifier=modifier) * self._weightsByCat[category].weight(modifier=modifier)


    

