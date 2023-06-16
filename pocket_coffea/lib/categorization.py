'''
This module defines several helper classes to handle Masks and Selections.

- The MaskStorage is a generalization of the PackedSelection utility to be able to store
  effectively dim=2 masks.

- MultiCut objects which splits the events in a defined number of subcategories.

- StandardSelection: handles the definition of categories from a dictionary of Cut objects.
  All the cuts are handled by a single MaskStorage. The StandardSelection object stores which cuts
  are applied in each category.

- CartesianSelection: handles the definition of cartesian product of categories. The class
  keeps a list of MultiCut objects, each defining a set of subcategories. Then, it defines automatically
  categories which are the cartesian products of the categories defined by each MultiCut.
  A StandardSelection object can be embeeded in the CartesianSelection to defined categories not used in the
  cartesian product.

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
                        f"Mask {id} has the wrong number of total entries. Check the collection"
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
  
    This class just wraps a list of Cuts to be used along with the CartesianSelection class.
    This class is useful to build a 1D binning to be combined with other binnings
    with a CartesianSelection.

    The `prepare()` methods instantiate a `PackedSelection` object
    to keep track of the masks. If the cuts are more than 64 the current
    implementation fails.

    The single masks can be retrieved by their index in the list
    with the function `get_mask(cut_index)`
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
                    f"Building a MultiCut with cuts on differenct collections: {cut.collection} and {self.multidim_collection}"
                )

        if cuts_names:
            self.cuts_names = cuts_names
        else:
            self.cuts_names = [c.name for c in self.cuts]
        if len(self.cuts) != len(self.cuts_names):
            raise ValueError(
                f"Number of cuts and cuts_names in MultiCut {name} differ! Check you configuration file."
            )
        # Flag indicates the MultiCut has been prepared
        self.ready = False

    def prepare(self, events, processor_params, **kwargs):
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
            self.storage.add(cut.id, cut.get_mask(events, processor_params, **kwargs))
        self.ready = True

    @property
    def ncuts(self):
        return len(self.cuts)

    def get_mask(self, cut_index):
        if not self.ready:
            raise Exception(
                "Before using the selection, call the prepare method to fill the masks"
            )
        return self.storage.all([self.cuts[cut_index].id])

    def __str__(self):
        return f"MultiCut(multidim: {self.is_multidim}, {len(self.cuts)} categories: {[c.name for c in self.cuts]})"

    def __repr__(self):
        return f"MultiCut(multidim: {self.is_multidim}, {len(self.cuts)} categories: {[c.name for c in self.cuts]})"

    def serialize(self):
        return {
            "type": "multicut",
            "cuts": [c.serialize() for c in self.cuts],
            "is_multidim": self.is_multidim,
            "multidim_collection": self.multidim_collection,
        }


######################################


class StandardSelection:
    '''
    The StandardSelection class defined the simplest categorization.

    Each category is identified by a label string and has a list of Cut object:
    the AND of the Cut objects defines the category mask.

    The class stores all the Cut objects mask in a single MaskStorage to
    reduce the memory overhead. Then, when a category mask is asked, the cut objects
    associated to the category are used.

    The object can handle a mixture of dim=1 and dim=2 Cut objects.
    The MaskStorage is instantiated automatically with dim=2 if at least one
    Cut is defined over a collection different from "events".

    '''

    def __init__(self, categories):
        # read the dictionary of categories
        self.categories = {}
        self.cut_functions = []
        self.cut_dict = {}
        for k, cuts in categories.items():
            self.categories[k] = []
            for c in cuts:
                if not isinstance(c, Cut):
                    raise Exception(f"The cut is not defined as a Cut object: {cut}")
                self.categories[k].append(c.id)
                self.cut_functions.append(c)
                self.cut_dict[c.id] = c

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

    def prepare(self, events, processor_params, **kwargs):
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
            self.storage.add(cut.id, cut.get_mask(events, processor_params, **kwargs))
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

    @property
    def template_mask(self):
        if self.ready and self.ismulti_dim:
            return self.storage.template_mask
        else:
            return None

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

    def serialize(self):
        serialized_cuts = {}
        for k, cuts in self.categories.items():
            serialized_cuts[k] = [self.cut_dict[c].serialize() for c in cuts]
        return {
            "type": "StandardSelection",
            "categories": serialized_cuts,
            "is_multidim": self.is_multidim,
            "multidim_collection": self.multidim_collection,
        }


###############################################################


class CartesianSelection:
    """
    The CartesianSelection class is needed for a special type of categorization:
    - you need to fully loop on the combination of different binnings.
    - you can express each binning with a MultiCut object.

    The cartesian product of a list of MultiCut object is build automatically:
    the class provides a generator computing the and of the masks on the fly.
    Computed masks are cached for multiple use between calls to `prepare()`.

    Common categories to be applied outside of the
    cartesian product can be defined with a StandardSelection object.

    The Cuts can be on events or on collections (dim=2).
    The StandardSelection is independent from the MultiCut cartesian product.
    If one of the multicut is dim=2, all the cartesian product will be multidimensional.
    """

    def __init__(
        self, multicuts: List[MultiCut], common_cats: StandardSelection = None
    ):
        self.multicuts = multicuts
        if common_cats:
            if isinstance(common_cats, dict):
                self.common_cats = StandardSelection(common_cats)
                self.has_common_cats = True
            elif isinstance(common_cats, StandardSelection):
                self.common_cats = common_cats
                self.has_common_cats = True
            else:
                raise Exception(
                    "The common_cats of a CartesianSelection needs to be a dictionary or StandardSelection object"
                )

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

        # Check if it is multidim
        self.is_multidim = False
        self.multidim_collection = None
        if self.has_common_cats:
            if self.common_cats.is_multidim:
                self.is_multidim = True
                self.multidim_collection = self.common_cats.multidim_collection

        for cut in self.multicuts:
            if cut.is_multidim:
                # the cut is multidim
                self.is_multidim = True
                if self.multidim_collection is None:
                    self.multidim_collection = cut.multidim_collection
                break

        # check if they are multidim on the same collection
        for cut in self.multicuts:
            if cut.is_multidim and cut.multidim_collection != self.multidim_collection:
                raise Exception(
                    f"Building a CartesianSelection with cuts on differenct collections: {cut.multidim_collection} and {self.multidim_collection}"
                )

    def prepare(self, events, processor_params, **kwargs):
        # clean the cache
        self.cache.clear()
        # Prepare the common cut:
        if self.has_common_cats:
            self.common_cats.prepare(events, processor_params, **kwargs)
        # Now preparing the multicut
        for multicut in self.multicuts:
            multicut.prepare(events, processor_params, **kwargs)

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

    @property
    def template_mask(self):
        if self.ready and self.ismulti_dim:
            return self.storage.template_mask
        else:
            return None

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

    def serialize(self):
        return {
            "type": "CartesianSelection",
            "common_categories": self.common_cats.serialize(),
            "multicuts": [m.serialize() for m in self.multicuts],
            "is_multidim": self.is_multidim,
            "multidim_collection": self.multidim_collection,
        }
