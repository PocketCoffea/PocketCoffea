'''
This module handles the definition of "categorical" cuts which are
combined in a cartesian product to produce a large number of categories.

The use must provided a list of MultiCut objects which splits the
events in a defined number of subcategories.

The final subcategories are the cartesian product of the categories
'''

import awkward as ak
from .cut_definition import Cut
from typing import List
from coffea.analysis_tools import PackedSelection
from itertools import product


class MultiCut:
    """Class for keeping track of a cut and its parameters."""

    def __init__(self, name: str, cuts: List[Cut], cuts_names: List[str] = None):
        self.name = name
        self.cuts = cuts
        if cuts_names:
            self.cuts_names = cuts_names
        else:
            self.cuts_names = [c.name for c in self.cuts]
        if len(self.cuts) != len(self.cuts_names):
            raise ValueError(f"Number of cuts and cuts_names in MultiCut {name} differ! Check you configuration file.")

    def prepare(self, events, year, sample, isMC):
        # Redo the selector every time to clean up between variations
        self.selector = PackedSelection()
        for cut in self.cuts:
            self.selector.add(
                cut.id, cut.get_mask(events, year=year, sample=sample, isMC=isMC)
            )

    @property
    def ncuts(self):
        return len(self.cuts)

    def get_mask(self, cut_index):
        return self.selector.all(self.cuts[cut_index].id)


class CartesianSelection:
    def __init__(self, multicuts: List[MultiCut], common_cats: dict[str, Cut] = None):
        self.multicuts = multicuts
        if common_cats:
            self.common_cats = common_cats
        else:
            self.common_cats = {}
        self.categories = list(self.common_cats.keys()) + [
            "_".join(p) for p in product(*[mc.cuts_names for mc in self.multicuts])
        ]
        self.cat_multi_index = list(self.common_cats.keys()) + list(
            product(*[range(mc.ncuts) for mc in self.multicuts])
        )
        self.categories_dict = dict(zip(self.categories, self.cat_multi_index))

    def prepare(self, events, year, sample, isMC):
        # packed selection for common categories
        self.common_cats_masks = PackedSelection()
        for ccat, cut in self.common_cats.items():
            self.common_cats_masks.add(
                ccat, cut.get_mask(events, year=year, sample=sample, isMC=isMC)
            )
        # Now preparing the multicut
        for multicut in self.multicuts:
            multicut.prepare(events, year, sample, isMC)

    def __getmask(self, multi_index):
        if isinstance(multi_index, str):
            # this is a common category
            return self.common_cats_masks.all(multi_index)
        # If not we need to load the multicuts for the cartesian
        masks = []
        for multicut, index in zip(self.multicuts, multi_index):
            masks.append(multicut.get_mask(index))
        final_mask = (
            ak.prod(ak.concatenate([m[:, None] for m in masks], axis=1), axis=1) == 1
        )
        return final_mask

    def get_masks(self):
        for category, multi_index in zip(self.categories, self.cat_multi_index):
            yield category, self.__getmask(multi_index)

    def get_mask(self, category):
        if category not in self.categories_dict:
            raise ValueError(f"Requested category ({category}) does not exists")
        return self.__getmask(self.categories_dict[category])

    def keys(self):
        return self.categories

    def items(self):
        return zip(self.categories, self.cat_multi_index)

    def __str__(self):
        return f"CartesianSelection {[m.name for m in self.multicuts]}, ({len(self.categories)} categories)"

    def __repr__(self):
        return f"CartesianSelection {[m.name for m in self.multicuts]}, ({len(self.categories)} categories)"

    def __iter__(self):
        return iter(self.categories)
