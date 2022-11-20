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


class ColumnsManager:
    def __init__(self, cfg, sample, categories_config):
        self.cfg = cfg[sample]
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
            mask = cuts_masks.all(*self.categories_config[category])
            if subsample_mask is not None:
                mask = mask & subsample_mask
            for outarray in outarrays:
                if outarray.store_size:
                    N = ak.num(events[mask][outarray.collection])
                    self.output[category][f"{outarray.collection}_N"] = column_accumulator(
                        ak.to_numpy(N, allow_missing=False)
                    )
                # looping on the columns
                for col in outarray.columns:
                    if outarray.flatten:
                        self.output[category][
                            f"{outarray.collection}_{col}"
                        ] = column_accumulator(
                            ak.to_numpy(
                                ak.flatten(events[mask][outarray.collection][col]),
                                allow_missing=False,
                            )
                        )
                    else:
                        self.output[category][
                            f"{outarray.collection}_{col}"
                        ] = column_accumulator(
                            ak.to_numpy(events[mask][outarray.collection][col]),
                            allow_missing=False,
                        )
            return self.output
