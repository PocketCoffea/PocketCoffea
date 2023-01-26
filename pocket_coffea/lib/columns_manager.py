from dataclasses import dataclass
from typing import List
from coffea.processor.accumulator import column_accumulator
import awkward as ak


@dataclass
class ColOut:
    collection: str  # Collection
    columns: List[str]  # list of columns to export
    flatten: bool = True  # Flatten by defaul
    store_size: bool = True
    fill_none: bool = True
    fill_value: float = -999.0  # by default the None elements are filled
    pos: int = (
        None  # Position in the collection to export. If None export all the elements
    )


class ColumnsManager:
    def __init__(self, cfg, sample, categories_config):
        self.cfg = cfg
        self.categories_config = categories_config

    def add_column(self, cfg: ColOut, categories=None):
        if categories is None:
            categories = self.categories_config.keys()
        for cat in categories:
            self.cfg[cat].append(cfg)

    def fill_columns(self, events, cuts_masks, subsample_mask=None):
        self.output = {}
        for category, outarrays in self.cfg.items():
            self.output[category] = {}
            # Computing mask
            mask = cuts_masks.get_mask(category)
            if subsample_mask is not None:
                mask = mask & subsample_mask

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
                data = events[outarray.collection][mask]

                if outarray.store_size:
                    N = ak.num(data)
                    self.output[category][
                        f"{outarray.collection}_N"
                    ] = column_accumulator(ak.to_numpy(N, allow_missing=False))
                # looping on the columns
                for col in outarray.columns:
                    if outarray.flatten:
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
