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


### Simple WeightWrappers from functions defined in the lib

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


SF_ele_reco = WeightLambda.wrap_func(
    name="sf_ele_reco",
    function=lambda params, metadata, events, size, shape_variations:
        sf_ele_reco(params, events, metadata["year"]),
    has_variations=True
    )

SF_ele_id = WeightLambda.wrap_func(
    name="sf_ele_id",
    function=lambda params, metadata, events, size, shape_variations:
        sf_ele_id(params, events, metadata["year"]),
    has_variations=True
    )

SF_mu_id = WeightLambda.wrap_func(
    name="sf_mu_id",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'id'),
    has_variations=True
    )

SF_mu_iso = WeightLambda.wrap_func(
    name="sf_mu_iso",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'iso'),
    has_variations=True
    )

SF_mu_trigger = WeightLambda.wrap_func(
    name="sf_mu_trigger",
    function=lambda params, metadata, events, size, shape_variations:
        sf_mu(params, events, metadata["year"], 'trigger'),
    has_variations=True
    )


########################################################################
# More complicated WeightWrapper defining dynamic variations depending
# on the data taking period

class SF_ele_trigger(WeightWrapper):
    name = "sf_ele_trigger"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # Getting the variations from the parameters depending on the year
        self._variations = params.systematic_variations.weight_variations.sf_ele_trigger[metadata["year"]]

    def compute(self, events, size, shape_variation):
        if shape_variation == "nominal":
            out = sf_ele_trigger(self._params, events, self._metadata["year"],
                             variations= ["nominal"] + self._variations,
                             )
            # This is a dict with variation: [nom, up, down]
            return WeightDataMultiVariation(
                 name = self.name,
                 nominal = out["nominal"][0],
                 variations = self._variations,
                 up = [out[var][1] for var in self._variations],
                 down = [out[var][2] for var in self._variations]
            )

        else:
            out = sf_ele_trigger(self._params, events, self._metadata["year"],
                             variations= ["nominal"] )
            return WeightData(
                name = self.name,
                nominal = out["nominal"][0]
            )
        
########################################
# Btag scale factors have weights depending on the shape_variation

class SF_btag(WeightWrapper):
    name = "sf_btag"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # Getting the variations from the parameters depending on the year
        self._variations = params.systematic_variations.weight_variations.sf_btag[metadata["year"]]
        self.jet_coll = params.jet_scale_factors.jet_collection.btag

    def compute(self, events, size, shape_variation):
        
        if shape_variation == "nominal":
            out = sf_btag(self._params,
                          events[self.jet_coll],
                          self._metadata["year"],
                          # Assuming n*JetCollection* is defined
                          njets=events[f"n{self.jet_coll}"],
                          variations=["central"] + self._variations,
                          )
            # This is a dict with variation: [nom, up, down]
            return WeightDataMultiVariation(
                name = self.name,
                nominal = out["central"][0],
                variations = self._variations,
                up = [out[var][1] for var in self._variations],
                down = [out[var][2] for var in self._variations]
            )


        elif "JES_" in shape_variation:
            out = sf_btag_calib(self._params,
                                events[self.jet_coll],
                                self._metadata["year"],
                                # Assuming n*JetCollection* is defined
                                njets=events[f"n{self.jet_coll}"],
                                variations=[shape_variation],
                                )
            return WeightData(
                name = self.name,
                nominal = out[shape_variation][0]
                )       
            
        else:
            out = sf_btag(self._params,
                          events[self.jet_coll],
                          self._metadata["year"],
                          # Assuming n*JetCollection* is defined
                          njets=events[f"n{self.jet_coll}"],
                          variations=["central"],
                          )
            return WeightData(
                name = self.name,
                nominal = out["central"][0]
            )
        
        
    
# List with default classes for common weights
common_weights = [
    genWeight,
    signOfGenWeight,
    lumi,
    XS,
    pileup,
    SF_ele_reco,
    SF_ele_id,
    SF_mu_id,
    SF_mu_iso,
    SF_ele_trigger
]



