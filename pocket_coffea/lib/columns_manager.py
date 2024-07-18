from dataclasses import dataclass
from typing import List
from coffea.processor.accumulator import column_accumulator
import awkward as ak
from collections import defaultdict

@dataclass
class ColOut:
    collection: str  # Collection
    columns: List[str]  # list of columns to export
    flatten: bool = True  # Flatten by default
    store_size: bool = True
    fill_none: bool = True
    fill_value: float = -999.0  # by default the None elements are filled
    pos_start: int = None  # First position in the collection to export. If None export from the first element
    pos_end: int = None  # Last position in the collection to export. If None export until the last element


class ColumnsManager:
    def __init__(self, cfg, categories_config, variations_config, year, processor_params, isMC=True):
        self.cfg = cfg
        self.categories_config = categories_config
        self.variations_config = variations_config
        self.year = year
        self.isMC = isMC
        self.available_weights_variations = ["nominal"]
        self.available_shape_variations = []
        self.processor_params = processor_params
        if self.isMC:
            self.available_weights_variations_bycat = defaultdict(list)
            self.available_shape_variations_bycat = defaultdict(list)
            # weights variations
            for cat, vars in self.variations_config["weights"].items():
                self.available_weights_variations_bycat[cat].append("nominal")
                for var in vars:
                    # Check if the variation is a wildcard and the systematic requested has subvariations
                    # defined in the parameters
                    if (
                        var
                        in self.processor_params.systematic_variations.weight_variations
                    ):
                        for (
                            subvariation
                        ) in self.processor_params.systematic_variations.weight_variations[
                            var
                        ][
                            self.year
                        ]:
                            self.available_weights_variations += [
                                f"{var}_{subvariation}Up",
                                f"{var}_{subvariation}Down",
                            ]
                            self.available_weights_variations_bycat[cat] += [
                                f"{var}_{subvariation}Up",
                                f"{var}_{subvariation}Down",
                            ]
                    else:
                        vv = [f"{var}Up", f"{var}Down"]
                        self.available_weights_variations += vv
                        self.available_weights_variations_bycat[cat] += vv

            for cat, vars in self.variations_config["shape"].items():
                for var in vars:
                    # Check if the variation is a wildcard and the systematic requested has subvariations
                    # defined in the parameters
                    if (
                        var
                        in self.processor_params.systematic_variations.shape_variations
                    ):
                        for (
                            subvariation
                        ) in self.processor_params.systematic_variations.shape_variations[
                            var
                        ][
                            self.year
                        ]:
                            self.available_weights_variations += [
                                f"{var}_{subvariation}Up",
                                f"{var}_{subvariation}Down",
                            ]
                            self.available_weights_variations_bycat[cat] += [
                                f"{var}_{subvariation}Up",
                                f"{var}_{subvariation}Down",
                            ]
                    else:
                        vv = [f"{var}Up", f"{var}Down"]
                        self.available_shape_variations += vv
                        self.available_shape_variations_bycat[cat] += vv
            self.available_weights_variations = set(self.available_weights_variations)
            self.available_shape_variations = set(self.available_shape_variations)

    @property
    def ncols(self):
        return sum([len(cols) for cols in self.cfg.values()])

    def add_column(self, cfg: ColOut, categories=None):
        if categories is None:
            categories = self.categories_config.keys()
        for cat in categories:
            self.cfg[cat].append(cfg)

    def fill_columns_accumulators(self, events, cuts_masks, subsample_mask=None, weights_manager=None, shape_variation="nominal", save_variations=False):
        self.output = {}
        for category, outarrays in self.cfg.items():
            self.output[category] = {}
            # Computing mask
            mask = cuts_masks.get_mask(category)
            if subsample_mask is not None:
                mask = mask & subsample_mask

            # Getting the weights
            if weights_manager:
                if shape_variation=="nominal" and save_variations:
                    for variation in self.available_weights_variations_bycat[category]:
                        if variation == "nominal":
                            self.output[category][f"weight_{variation}"] = column_accumulator(                                                                                                                                                              ak.to_numpy(weights_manager.get_weight(category)[mask], allow_missing=False))
                        else:
                            # Check if the variation is available in this category
                            self.output[category][f"weight_{variation}"] = column_accumulator(                                                                                                                                                              ak.to_numpy(weights_manager.get_weight(category, modifier=variation)[mask], allow_missing=False))
                else:
                    # Save only the nominal weights if a shape variation is being processed
                    self.output[category]["weight"] = column_accumulator(
                    ak.to_numpy(weights_manager.get_weight(category)[mask], allow_missing=False))
                    
                
            for outarray in outarrays:
                # Check if the cut is multidimensional
                # if so we need to check the collection
                if mask.ndim > 1:
                    if (
                        outarray.collection
                        != self.categories_config.multidim_collection
                    ):
                        raise Exception(
                            f"Applying mask on collection {self.categories_config.multidim_collection}, \
                            while exporting collection {outarray.collection}! Please check your categorization"
                        )
                # Applying mask after getting the collection
                if(outarray.collection=="events"):
                    data = events[mask]
                else:
                    data = events[outarray.collection][mask]

                # Filtering the position in the collection if needed
                if outarray.pos_start and outarray.pos_end:
                    data = data[:, outarray.pos_start : outarray.pos_end]
                elif outarray.pos_start and not outarray.pos_end:
                    data = data[:, outarray.pos_start :]
                elif not outarray.pos_start and outarray.pos_end:
                    data = data[:, : outarray.pos_end]

                if outarray.store_size and data.ndim > 1:
                    N = ak.num(data)
                    self.output[category][
                        f"{outarray.collection}_N"
                    ] = column_accumulator(ak.to_numpy(N, allow_missing=False))
                # looping on the columns
                for col in outarray.columns:
                    if data.ndim > 1 and not outarray.flatten:
                        # Check if the array is regular or not
                        # Cannot export to numpy irregular array --> need to be flattened!
                        if (isinstance(data[col].layout, ak.layout.ListOffsetArray64)
                            or isinstance(data[col].layout, ak.layout.ListArray64)):
                            raise Exception(
                                f"Trying to export a multidimensional column {col} without flattening it! Please check your configuration"
                            )
                    if data.ndim > 1 and outarray.flatten:
                        if outarray.fill_none:
                            out = ak.fill_none(
                                ak.flatten(data[col]),
                                outarray.fill_value,
                            )
                        else:
                            out = ak.flatten(data[col])
                    else:
                        if outarray.fill_none:
                            out = ak.fill_none(
                                data[col],
                                outarray.fill_value,
                            )
                        else:
                            out = data[col]

                    self.output[category][
                        f"{outarray.collection}_{col}"
                    ] = column_accumulator(
                        ak.to_numpy(
                            out,
                            allow_missing=False,
                        )
                    )
        return self.output

    def fill_ak_arrays(self, events, cuts_masks, subsample_mask=None, weights_manager=None):
        self.output = {}
        for category, outarrays in self.cfg.items():
            if len(outarrays)==0:
                continue
            out_by_cat = {}
            # Computing mask
            mask = cuts_masks.get_mask(category)
            if subsample_mask is not None:
                mask = mask & subsample_mask

            # Getting the weights
            # Only for nominal variation for the moment
            if weights_manager:
                out_by_cat["weight"] = weights_manager.get_weight(category)[mask]
            

            for outarray in outarrays:
                # Check if the cut is multidimensional
                # if so we need to check the collection
                if mask.ndim > 1:
                    if (
                        outarray.collection
                        != self.categories_config.multidim_collection
                    ):
                        raise Exception(
                            f"Applying mask on collection {self.categories_config.multidim_collection}, \
                            while exporting collection {outarray.collection}! Please check your categorization"
                        )
                # Applying mask after getting the collection
                if(outarray.collection=="events"):
                    data = events[mask]
                else:
                    data = events[outarray.collection][mask]

                # Filtering the position in the collection if needed
                if outarray.pos_start and outarray.pos_end:
                    data = data[:, outarray.pos_start : outarray.pos_end]
                elif outarray.pos_start and not outarray.pos_end:
                    data = data[:, outarray.pos_start :]
                elif not outarray.pos_start and outarray.pos_end:
                    data = data[:, : outarray.pos_end]

                if outarray.store_size and data.ndim > 1:
                    N = ak.num(data)
                    out_by_cat[f"{outarray.collection}_N"] = N

                # looping on the columns
                for col in outarray.columns:
                    if outarray.flatten and data.ndim > 1:
                        if outarray.fill_none:
                            out = ak.fill_none(
                                ak.flatten(data[col]),
                                outarray.fill_value,
                            )
                        else:
                            out = ak.flatten(data[col])
                    else:
                        if outarray.fill_none:
                            out = ak.fill_none(
                                data[col],
                                outarray.fill_value,
                            )
                        else:
                            out = data[col]

                    out_by_cat[f"{outarray.collection}_{col}"] = out

            #zipping all the arrays by cat
            self.output[category] = ak.zip(out_by_cat, depth_limit=1)
        #return full output with all categories
        return self.output
