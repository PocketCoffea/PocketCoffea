from dataclasses import dataclass
from typing import List
from coffea.processor.accumulator import column_accumulator
import awkward as ak


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
    def __init__(self, cfg, categories_config):
        self.cfg = cfg
        self.categories_config = categories_config

    @property
    def ncols(self):
        return sum([len(cols) for cols in self.cfg.values()])

    def add_column(self, cfg: ColOut, categories=None):
        if categories is None:
            categories = self.categories_config.keys()
        for cat in categories:
            self.cfg[cat].append(cfg)

    def fill_columns_accumulators(self, events, cuts_masks, subsample_mask=None, weights_manager=None):
        self.output = {}
        for category, outarrays in self.cfg.items():
            self.output[category] = {}
            # Computing mask
            mask = cuts_masks.get_mask(category)
            if subsample_mask is not None:
                mask = mask & subsample_mask

            # Getting the weights
            # Only for nominal variation for the moment
            if weights_manager:
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
            if weights_manager: # no present for data
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

                # looping on the columns
                try:
                    for col in outarray.columns:
                        if outarray.flatten and data.ndim > 1:
                            if outarray.fill_none:
                                out = ak.fill_none(
                                    ak.flatten(getattr(data, col)),
                                    outarray.fill_value,
                                )
                            else:
                                out = ak.flatten(getattr(data, col))
                        else:
                            if outarray.fill_none:
                                out = ak.fill_none(
                                    getattr(data, col),
                                    outarray.fill_value,
                                )
                            else:
                                out = getattr(data, col)

                        out_by_cat[f"{outarray.collection}_{col}"] = out
                except Exception as e:
                    print(f"Error in category {category} and collection {outarray.collection} and column {col}")
                    print(e)
                    raise e

            # Check that all the columns have the same first axis size
            # get dim from first key
            dim = len(out_by_cat[list(out_by_cat.keys())[0]])
            for k, v in out_by_cat.items():
                if len(v) != dim:
                    raise Exception(
                        f"Columns {k} has a different size than the weight column! Please check your configuration. \
                        When exporting akward arrays all the columns should have flatten=False option to keep the event dimension consistent"
                    )
            
            # Zipping all the arrays by cat
            self.output[category] = ak.zip(out_by_cat, depth_limit=1)
                
        #return full output with all categories
        return self.output
