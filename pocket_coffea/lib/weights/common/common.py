from ..weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
import numpy as np
import awkward as ak

from pocket_coffea.lib.scale_factors import (
    sf_ele_reco,
    sf_ele_id,
    sf_photon,
    sf_mu,
    sf_btag,
    sf_btag_calib,
    sf_ctag,
    sf_ctag_calib,
    sf_jet_puId,
    sf_L1prefiring,
    sf_pileup_reweight,
    sf_partonshower_isr,
    sf_partonshower_fsr
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
        np.ones(len(events)) * float(metadata["xsec"]),
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


SF_L1prefiring = WeightLambda.wrap_func(
    name="sf_L1prefiring",
    function=lambda params, metadata, events, size, shape_variations:
        sf_L1prefiring(events),
    has_variations=True
    )


SF_PSWeight_isr = WeightLambda.wrap_func(
    name="sf_partonshower_isr",
    function=lambda params, metadata, events, size, shape_variations:
        sf_partonshower_isr(events),
    has_variations=True
    )

SF_PSWeight_fsr = WeightLambda.wrap_func(
    name="sf_partonshower_fsr",
    function=lambda params, metadata, events, size, shape_variations:
        sf_partonshower_fsr(events),
    has_variations=True
    )


SF_pho_pxseed = WeightLambda.wrap_func(
    name="sf_pho_pxseed",
    function=lambda params, metadata, events, size, shape_variations:
        sf_photon(params, events, metadata["year"], 'pxseed'),
    has_variations=True
    )

SF_pho_id = WeightLambda.wrap_func(
    name="sf_pho_id",
    function=lambda params, metadata, events, size, shape_variations:
        sf_photon(params, events, metadata["year"], 'id'),
    has_variations=True
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


        elif shape_variation.startswith("JES"):
            out = sf_btag(self._params,
                                events[self.jet_coll],
                                self._metadata["year"],
                                # Assuming n*JetCollection* is defined
                                njets=events[f"n{self.jet_coll}"],
                                variations=[shape_variation],
                                )
            return WeightData(
                name = self.name,
                nominal = out["central"][0]
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

class SF_btag_calib(WeightWrapper):
    name = "sf_btag_calib"
    has_variations = False

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        self.jet_coll = params.jet_scale_factors.jet_collection.btag

    def compute(self, events, size, shape_variation):
        jetsHt = ak.sum(events[self.jet_coll].pt, axis=1)
        out = sf_btag_calib(self._params,
                            sample=self._metadata["sample"],
                            year=self._metadata["year"],
                            # Assuming n*JetCollection* is defined
                            njets=events[f"n{self.jet_coll}"],
                            jetsHt=jetsHt
                            )
        return WeightData(
            name = self.name,
            nominal = out, #out[0] only if has_variations = True
            #up = out[1],
            #down = out[2]
            )


########################################
# Ctag scale factors have weights depending on the shape_variation

class SF_ctag(WeightWrapper):
    name = "sf_ctag"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # Getting the variations from the parameters depending on the year
        self._variations = params.systematic_variations.weight_variations.sf_ctag[metadata["year"]]
        self.jet_coll = params.jet_scale_factors.jet_collection.ctag

    def compute(self, events, size, shape_variation):
        
        if shape_variation == "nominal":
            out = sf_ctag(self._params,
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
        else:
            #only the nominal weight for shape variations != nominal
            out = sf_ctag(self._params,
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

class SF_ctag_calib(WeightWrapper):
    name = "sf_ctag_calib"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        self.jet_coll = params.jet_scale_factors.jet_collection.ctag

    def compute(self, events, size, shape_variation):
        jetsHt = ak.sum(events[self.jet_coll].pt, axis=1)
        out = sf_ctag_calib(self._params,
                            dataset=self._metadata["dataset"],
                            year=self._metadata["year"],
                            # Assuming n*JetCollection* is defined
                            njets=events[f"n{self.jet_coll}"],
                            jetsHt=jetsHt
                            )
        return WeightData(
            name = self.name,
            nominal = out[0],
            up = out[1],
            down = out[2]
            )

#############################################################
class SF_jet_puId(WeightWrapper):
    name = "sf_jet_puId"
    has_variations = True

    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        self.jet_coll = params.jet_scale_factors.jet_collection.puId

    def compute(self, events, size, shape_variation):
        out = sf_jet_puId(self._params,
                          events[self.jet_coll],
                          year=self._metadata["year"],
                          # Assuming n*JetCollection* is defined
                          njets=events[f"n{self.jet_coll}"],
                          )
        return WeightData(
            name = self.name,
            nominal = out[0],
            up = out[1],
            down = out[2]
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
    SF_pho_pxseed,
    SF_pho_id,
    SF_mu_id,
    SF_mu_iso,
    SF_mu_trigger,
    SF_btag,
    SF_btag_calib,
    SF_ctag,
    SF_ctag_calib,
    SF_jet_puId,
    SF_PSWeight_isr,
    SF_PSWeight_fsr
]



