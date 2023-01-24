'''
This module handles the definition of "categorical" cuts which are
combined in a cartesian product to produce a large number of categories.

The use must provided a list of MultiCut objects which splits the
events in a defined number of subcategories.

The final subcategories are the cartesian product of the categories
'''

import awkward as ak
import numpy as np
from .cut_definition import Cut
from typing import List
from coffea.analysis_tools import PackedSelection
from itertools import product


class MaskStorage:
    '''
    The MaskStorage class stores in a PackedSelection
    multidimensional cuts: it does so by flattening the mask and keeping track of
    the counts of elements for the unflattening.
    Dim=1 and dim=2 cuts can be stored together if the storage is initialize with
    dim=2.
    '''

    def __init__(self, dim=1, counts=None):
        self.dim = dim
        if self.dim > 2:
            raise NotImplementedError(
                "MasksStorage with more than dim=2 is not implemented yet"
            )
        self.is_multidim = self.dim > 1
        self.counts = counts
        if self.is_multidim:
            if self.counts is None:
                raise Exception(
                    "You need to provide the number of elements to unflatten the mask if dim>1"
                )
            else:
                self.tot_elements = ak.sum(counts)
                self.template_mask = ak.unflatten(
                    ak.Array(np.ones(self.tot_elements) == 1), self.counts
                )
        else:
            self.tot_elements = None

        self.cache = PackedSelection(dtype="uint64")

    @property
    def names(self):
        return self.cache.names

    def add(self, id, mask):
        if mask.ndim == 1 and self.is_multidim:
            # we need to broadcast the mask to the dim=2
            flat_mask = ak.flatten(mask & self.template_mask)
            self.cache.add(id, ak.to_numpy(flat_mask))
        elif mask.ndim > 1:
            if not self.is_multidim:
                raise Exception(
                    "You are trying to add a dim>1 mask to a dim=1 storage. Please create a dim=2 storage"
                )
            else:
                # Flatten the mask and check the size
                flat_mask = ak.flatten(mask)
                if len(flat_mask) != self.tot_elements:
                    raise Exception(
                        f"Mask {f} has the wrong number of total entries. Check the collection"
                    )
                self.cache.add(id, ak.to_numpy(flat_mask))
        else:
            self.cache.add(id, ak.to_numpy(mask))

    def all(self, cut_ids, unflatten=True):
        mask = self.cache.all(*cut_ids)
        if self.is_multidim and unflatten:
            return ak.unflatten(mask, self.counts)
        else:
            return mask

    def __repr__(self):
        return f"MaskStorage(dim={self.dim}, masks={self.cache.names})"

    @property
    def masks(self):
        return self.cache.names


class MultiCut:
    """Class for keeping track of a list of cuts and their masks by index.
    The class is built from a list of Cut objects and cuts names.

    The `prepare()` methods instantiate a `PackedSelection` object
    to keep track of the masks. If the cuts are more than 64 the current
    implementation fails.

    The single masks can be retrieved by their index in the list
    with the function `get_mask()`.

    This object is useful to build a 1D binning to be combined
    with a CartesianSelection with other binnings.
    """

    def __init__(self, name: str, cuts: List[Cut], cuts_names: List[str] = None):
        self.name = name
        self.cuts = cuts
        # Checking the collection of the list of cuts to decide the dimension of the
        # mask storage
        self.is_multidim = False
        self.multidim_collection = None
        for cut in self.cuts:
            if cut.collection != "events":
                # the cut is multidim
                self.is_multidim = True
                self.multidim_collection = cut.collection
                break
        # Check that all cut with dim=2 are on the same collection
        for cut in self.cuts:
            if (
                cut.collection != "events"
                and cut.collection != self.multidim_collection
            ):
                raise Exception(
                    f"Building a StandardSelection with cuts on differenct collections: {cut.collection} and {self.multidim_collection}"
                )

        if cuts_names:
            self.cuts_names = cuts_names
        else:
            self.cuts_names = [c.name for c in self.cuts]
        if len(self.cuts) != len(self.cuts_names):
            raise ValueError(
                f"Number of cuts and cuts_names in MultiCut {name} differ! Check you configuration file."
            )

    def prepare(self, events, year, sample, isMC):
        # Redo the selector every time to clean up between variations
        if self.is_multidim:
            dim = 2
            # count the objects in the collection
            if f"n{self.multidim_collection}" in events.fields:
                counts = events[f"n{self.multidim_collection}"]
            else:
                counts = ak.num(events[self.multidim_collection])
        else:
            counts = None
            dim = 1
        # Create the mask storage
        self.storage = MaskStorage(dim=dim, counts=counts)
        for cut in self.cuts:
            self.storage.add(
                cut.id, cut.get_mask(events, year=year, sample=sample, isMC=isMC)
            )

    @property
    def ncuts(self):
        return len(self.cuts)

    def get_mask(self, cut_index):
        return self.storage.all([self.cuts[cut_index].id])

    def __str__(self):
        return f"MultiCut({len(self.storage.masks)} categories: {[m for m in self.storage.masks]})"

    def __repr__(self):
        return f"MultiCut({len(self.storage.masks)} categories: {[m for m in self.storage.masks]})"


######################################


class StandardSelection:
    def __init__(self, categories):
        # read the dictionary of categories
        self.categories = {}
        self.cut_functions = []
        for k, cuts in categories.items():
            self.categories[k] = []
            for c in cuts:
                self.categories[k].append(c.id)
                self.cut_functions.append(c)

        # The selection is ready after the prepare method has been called
        self.ready = False

        # Now make sure to have unique fucntions
        self.cut_functions = set(self.cut_functions)
        for cat in self.categories:
            self.categories[cat] = set(self.categories[cat])

        self.is_multidim = False
        self.multidim_collection = None
        for cut in self.cut_functions:
            if cut.collection != "events":
                # the cut is multidim
                self.is_multidim = True
                self.multidim_collection = cut.collection
                break
        # Check that all cut with dim=2 are on the same collection
        for cut in self.cut_functions:
            if (
                cut.collection != "events"
                and cut.collection != self.multidim_collection
            ):
                raise Exception(
                    f"Building a StandardSelection with cuts on differenct collections: {cut.collection} and {self.multidim_collection}"
                )

    def prepare(self, events, year, sample, isMC):
        # Creating the maskstorage for the categorization.
        if self.is_multidim:
            dim = 2
            # count the objects in the collection
            if f"n{self.multidim_collection}" in events.fields:
                counts = events[f"n{self.multidim_collection}"]
            else:
                counts = ak.num(events[self.multidim_collection])
        else:
            counts = None
            dim = 1

        # Create the mask storage
        self.storage = MaskStorage(dim=dim, counts=counts)
        for cut in self.cut_functions:
            self.storage.add(
                cut.id, cut.get_mask(events, year=year, sample=sample, isMC=isMC)
            )
        self.ready = True

    def get_mask(self, category):
        if not self.ready:
            raise Exception(
                "Before using the selection, call the prepare method to fill the masks"
            )
        return self.storage.all(self.categories[category], unflatten=True)

    def get_masks(self):
        for category in self.categories.keys():
            yield category, self.get_mask(category)

    def keys(self):
        return list(self.categories.keys())

    def items(self):
        return self.categories.items()

    def __str__(self):
        return f"StandardSelection {[m for m in self.categories]}, ({len(self.categories)} categories)"

    def __repr__(self):
        return f"StandardSelection {[m for m in self.categories]}, ({len(self.categories)} categories)"

    def __iter__(self):
        return iter(self.categories)


###############################################################


class CartesianSelection:
    """
    The CartesianSelection class is needed for a special type of categorization:
    - you need to fully loop on the combination of different binnings.
    - you can express each binning with a MultiCut object.

    The cartesian product of a list of MultiCut object is build automatically:
    the class provides a generator computing the and of the masks on the fly.
    Computed masks are cached for multiple use between calls to `prepare()`.

    A dictionary of common categories to be applied outside of the
    cartesian product can be provided for convinience.

    """

    def __init__(
        self, multicuts: List[MultiCut], common_cats: StandardSelection = None
    ):
        self.multicuts = multicuts
        if common_cats:
            if type(common_cats) != StandardSelection:
                raise Exception(
                    "The common_cats of a CartesianSelection needs to be a StandardSelection object"
                )
            self.common_cats = common_cats
            self.has_common_cats = True
        else:
            self.common_cats = {}
            self.has_common_cats = False
        # list of string identifiers
        self.categories = list(self.common_cats.keys()) + [
            "_".join(p) for p in product(*[mc.cuts_names for mc in self.multicuts])
        ]
        # List of internal identifiers.
        # - str for the common categories
        # - list of indices for the cartesian product: e.g. (0,3,4)
        self.cat_multi_index = list(self.common_cats.keys()) + list(
            product(*[range(mc.ncuts) for mc in self.multicuts])
        )
        # Dictionary from identifier to ID
        self.categories_dict = dict(zip(self.categories, self.cat_multi_index))
        # Cache the multiindex categories for multiple use
        self.cache = {}

    def prepare(self, events, year, sample, isMC):
        # clean the cache
        self.cache.clear()
        # Prepare the common cut:
        if self.has_common_cats:
            self.common_cats.prepare(events, year, sample, isMC)
        # Now preparing the multicut
        for multicut in self.multicuts:
            multicut.prepare(events, year, sample, isMC)

    def __getmask(self, multi_index):
        if isinstance(multi_index, str):
            # this is a common category
            return self.common_cats.get_mask(multi_index)
        if multi_index in self.cache:
            return self.cache[multi_index]
        # If not we need to load the multicuts for the cartesian
        masks_multidim = []
        masks_onedim = []
        for multicut, index in zip(self.multicuts, multi_index):
            if multicut.is_multidim:
                masks_multidim.append(multicut.get_mask(index))
            else:
                masks_onedim.append(multicut.get_mask(index))
        # We need to do the and separately for onedime and multidim
        if len(masks_multidim) > 0 and len(masks_onedim) > 0:
            final_mask = (
                ak.prod(
                    ak.concatenate([ak.singletons(m) for m in masks_onedim], axis=-1),
                    axis=-1,
                )
                == 1
            ) & (
                ak.prod(
                    ak.concatenate([ak.singletons(m) for m in masks_multidim], axis=-1),
                    axis=-1,
                )
                == 1
            )
        elif len(masks_multidim) == 0 and len(masks_onedim) > 0:
            final_mask = (
                ak.prod(
                    ak.concatenate([ak.singletons(m) for m in masks_onedim], axis=-1),
                    axis=-1,
                )
                == 1
            )
        elif len(masks_multidim) > 0 and len(masks_onedim) == 0:
            final_mask = (
                ak.prod(
                    ak.concatenate([ak.singletons(m) for m in masks_multidim], axis=-1),
                    axis=-1,
                )
                == 1
            )

        self.cache[multi_index] = final_mask
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
