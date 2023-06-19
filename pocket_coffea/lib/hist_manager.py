import hist
import awkward as ak
from collections import defaultdict
from coffea.analysis_tools import PackedSelection
from typing import List, Tuple
from dataclasses import dataclass, field
from copy import deepcopy
import logging


@dataclass
class Axis:
    field: str  # variable to plot
    label: str  # human readable label for the axis
    bins: int = None
    start: float = None
    stop: float = None
    coll: str = "events"  # Collection or events or metadata or custom
    name: str = None  # Identifier of the axis: By default is built as coll.field, if not provided
    pos: int = None  # index in the collection to plot. If None plot all the objects on the same histogram
    type: str = "regular"  # regular/variable/integer/intcat/strcat
    transform: str = None
    lim: Tuple[float] = (0, 0)
    underflow: bool = True
    overflow: bool = True
    growth: bool = False


@dataclass
class HistConf:
    axes: List[Axis]
    storage: str = "weight"
    autofill: bool = True  # Handle the filling automatically
    variations: bool = True
    only_variations: List[str] = None
    exclude_samples: List[str] = None
    only_samples: List[str] = None
    exclude_categories: List[str] = None
    only_categories: List[str] = None
    no_weights: bool = False  # Do not fill the weights
    metadata_hist: bool = False  # Non-event variables, for processing metadata
    hist_obj = None
    collapse_2D_masks = False  # if 2D masks are applied on the events
    # and the data_ndim=1, when collapse_2D_mask=True the OR
    # of the masks on the axis=2 is performed to get the mask
    # on axis=1, otherwise an exception is raised
    collapse_2D_masks_mode = "OR"  # Use OR or AND to collapse 2D masks for data_ndim=1 if collapse_2D_masks == True

    def serialize(self):
        out = {**self.__dict__}
        out["axes"] = []
        for a in self.axes:
            ax_dict = {}
            for k,v in a.__dict__.items():
                if k !="transform":
                    ax_dict[k] = v
        return out


def get_hist_axis_from_config(ax: Axis):
    if ax.name == None:
        ax.name = f"{ax.coll}.{ax.field}"
    if ax.type == "regular" and isinstance(ax.bins, list):
        ax.type = "variable"
    if ax.type == "regular":
        return hist.axis.Regular(
            name=ax.name,
            bins=ax.bins,
            start=ax.start,
            stop=ax.stop,
            label=ax.label,
            transform=ax.transform,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth,
        )
    elif ax.type == "variable":
        if not isinstance(ax.bins, list):
            raise ValueError(
                "A list of bins edges is needed as 'bins' parameters for a type='variable' axis"
            )
        return hist.axis.Variable(
            ax.bins,
            name=ax.name,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth,
        )
    elif ax.type == "int":
        return hist.axis.Integer(
            name=ax.name,
            start=ax.start,
            stop=ax.stop,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth,
        )
    elif ax.type == "intcat":
        return hist.axis.IntCategory(
            ax.bins,
            name=ax.name,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth,
        )
    elif ax.type == "strcat":
        return hist.axis.StrCategory(
            ax.bins, name=ax.name, label=ax.label, growth=ax.growth
        )


class HistManager:
    def __init__(
        self,
        hist_config,
        year,
        sample,
        subsamples,
        categories_config,
        variations_config,
        processor_params,
        custom_axes=None,
        isMC=True,
    ):
        self.processor_params = processor_params
        self.isMC = isMC
        self.year = year
        self.subsamples = subsamples
        self.histograms = defaultdict(dict)
        self.variations_config = variations_config
        self.categories_config = categories_config
        self.available_categories = set(self.categories_config.keys())
        self.available_weights_variations = ["nominal"]
        self.available_shape_variations = []
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

            # Shape variations
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
            # Reduce to set over all the categories
            self.available_weights_variations = set(self.available_weights_variations)
            self.available_shape_variations = set(self.available_shape_variations)
        # Prepare the variations Axes summing all the required variations
        # The variation config is organized as the weights one, by sample and by category

        for name, hcfg in deepcopy(hist_config).items():
            # Check if the histogram is active for the current sample
            # We only check for the parent sample, not for subsamples
            if hcfg.only_samples != None:
                if self.sample not in cfg.only_samples:
                    continue
            elif hcfg.exclude_samples != None:
                if self.sample in hcfg.exclude_samples:
                    continue
            # Now we handle the selection of the categories
            cats = []
            for c in self.available_categories:
                if hcfg.only_categories != None:
                    if c in hcfg.only_categories:
                        cats.append(c)
                elif hcfg.exclude_categories != None:
                    if c not in hcfg.exclude_categories:
                        cats.append(c)
                else:
                    cats.append(c)
            # Update the histConf to save the only category
            hcfg.only_categories = list(sorted(cats))
            # Create categories axis
            cat_ax = hist.axis.StrCategory(
                hcfg.only_categories, name="cat", label="Category", growth=False
            )

            # Variation axes
            if hcfg.variations:
                # Get all the variation
                allvariat = []
                for c in cats:
                    # Summing all the variations for all the categories
                    allvariat += self.variations_config["weights"][c]
                    allvariat += self.variations_config["shape"][c]
                allvariat = set(allvariat)
                if hcfg.only_variations != None:
                    # filtering the variation list with the available ones
                    allvariat = set(
                        filter(lambda v: v in hcfg.only_variations, allvariat)
                    )

                hcfg.only_variations = ["nominal"] + list(
                    sorted(
                        [var + "Up" for var in allvariat]
                        + [var + "Down" for var in allvariat]
                    )
                )
            else:
                hcfg.only_variations = ["nominal"]
            # Defining the variation axis
            var_ax = hist.axis.StrCategory(
                hcfg.only_variations, name="variation", label="Variation", growth=False
            )
            # Axis in the configuration + custom axes
            if self.isMC:
                all_axes = [cat_ax, var_ax]
            else:
                # no variation axis for data
                all_axes = [cat_ax]
            # the custom axis get included in the hcfg for future use
            hcfg.axes = custom_axes + hcfg.axes
            # Then we add those axes to the full list
            for ax in hcfg.axes:
                all_axes.append(get_hist_axis_from_config(ax))
            # Creating an histogram object for each subsample
            for subsample in self.subsamples:
                hcfg_subs = deepcopy(hcfg)
                # Build the histogram object with the additional axes
                hcfg_subs.hist_obj = hist.Hist(
                    *all_axes, storage=hcfg.storage, name="Counts"
                )
                # Save the hist in the configuration and store the full config object
                self.histograms[subsample][name] = hcfg_subs

    def get_histograms(self, subsample):
        # Exclude by default metadata histo
        return {
            key: h.hist_obj
            for key, h in self.histograms[subsample].items()
            if not h.metadata_hist
        }

    def get_metadata_histograms(self, subsample):
        return {
            key: h.hist_obj
            for key, h in self.histograms[subsample].items()
            if h.metadata_hist
        }

    def get_histogram(self, subsample, name):
        return self.histograms[subsample][name]

    def __prefetch_weights(self, weights_manager, category, shape_variation):
        weights = {}
        if shape_variation == "nominal":
            for variation in self.available_weights_variations_bycat[category]:
                if variation == "nominal":
                    weights["nominal"] = weights_manager.get_weight(category)
                else:
                    # Check if the variation is available in this category
                    weights[variation] = weights_manager.get_weight(
                        category, modifier=variation
                    )
        else:
            # Save only the nominal weights if a shape variation is being processed
            weights["nominal"] = weights_manager.get_weight(category)
        return weights

    def fill_histograms(
        self,
        events,
        weights_manager,
        categories,
        shape_variation="nominal",
        subsamples=None,  # This is a dictionary with name:ak.Array(bool)
        custom_fields=None,
        custom_weight=None,  # it should be a dictionary {variable:weight}
    ):
        '''
        We loop on the configured histograms only
        Doing so the catergory, sample, variation selections are handled correctly (by the constructor).

        Custom_fields is a dict of additional array. The expected lenght of the first dimension is the number of
        events. The categories mask will be applied.
        '''

        # Preloading weights
        if self.isMC:
            weights = {}
            for category in self.available_categories:
                weights[category] = self.__prefetch_weights(
                    weights_manager, category, shape_variation
                )
        # Cleaning the weights cache decorator between calls.
        weights_cache.cache_.clear()

        # Looping on the histograms to read the values only once
        # Then categories, subsamples and weights are applied and masked correctly

        # ASSUNTION, the histograms are the same for each subsample
        # we can take the configuration of the first subsample
        for name, histo in self.histograms[self.subsamples[0]].items():
            # logging.info(f"\thisto: {name}")
            if not histo.autofill:
                continue
            if histo.metadata_hist:
                continue  # TODO dedicated function for metadata histograms

            # Check if a shape variation is under processing and
            # if the current histogram does not require that
            if (
                shape_variation != "nominal"
                and shape_variation not in histo.hist_obj.axes["variation"]
            ):
                continue

            # Get the filling axes --> without any masking.
            # The flattening has to be applied as the last step since the categories and subsamples
            # work at event level

            fill_categorical = {}
            fill_numeric = {}
            data_ndim = None

            for ax in histo.axes:
                # Checkout the collection type
                if ax.type in ["regular", "variable", "int"]:
                    if ax.coll == "events":
                        # These are event level information
                        data = events[ax.field]
                    elif ax.coll == "metadata":
                        data = events.metadata[ax.field]
                    elif ax.coll == "custom":
                        # taking the data from the custom_fields argument
                        # IT MUST be a per-event number, so we expect an array to mask
                        data = custom_fields[ax.field]
                    else:
                        if ax.coll not in events.fields:
                            raise ValueError(
                                f"Collection {ax.coll} not found in events!"
                            )
                        # General collections
                        if ax.pos == None:
                            data = events[ax.coll][ax.field]
                        elif ax.pos >= 0:
                            data = ak.pad_none(
                                events[ax.coll][ax.field], ax.pos + 1, axis=1
                            )[:, ax.pos]
                        else:
                            raise Exception(
                                f"Invalid position {ax.pos} requested for collection {ax.coll}"
                            )

                    # Flattening
                    if data_ndim == None:
                        data_ndim = data.ndim
                    elif data_ndim != data.ndim:
                        raise Exception(
                            f"Incompatible shapes for Axis {ax} of hist {histo}"
                        )
                    # If we have multidim data we need to flatten it
                    # but we will do it after the event masking of each category

                    # Filling the numerical axes
                    fill_numeric[ax.name] = data

                #### --> end of numeric axes
                # Categorical axes (not appling the mask)
                else:
                    if ax.coll == "metadata":
                        data = events.metadata[ax.field]
                        fill_categorical[ax.name] = data
                    elif ax.coll == "custom":
                        # taking the data from the custom_fields argument
                        data = custom_fields[ax.field]
                        fill_categorical[ax.name] = data
                    else:
                        raise NotImplementedError()

            # Now the variables have been read for all the events
            # We need now to iterate on categories and subsamples
            # Mask the events, the weights and then flatten and remove the None correctly
            for category, cat_mask in categories.get_masks():
                # loop directly on subsamples
                for subsample, subs_mask in subsamples.get_masks():
                    # logging.info(f"\t\tcategory {category}, subsample {subsample}")
                    mask = cat_mask & subs_mask
                    # Skip empty categories and subsamples
                    if ak.sum(mask) == 0:
                        continue

                    # Check if the required data is dim=1, per event,
                    # and the mask is by collection.
                    # In this case the mask is reduced to per-event mask
                    # doing a logical OR only if explicitely allowed by the user
                    # WARNING!! POTENTIAL PROBLEMATIC BEHAVIOUR
                    # The user must be aware of the behavior.

                    if data_ndim == 1 and mask.ndim > 1:
                        if histo.collapse_2D_masks:
                            if histo.collapse_2D_masks_mode == "OR":
                                mask = ak.any(mask, axis=1)
                            elif histo.collapse_2D_masks_mode == "AND":
                                mask = ak.all(mask, axis=1)
                            else:
                                raise Exception(
                                    "You want to collapse the 2D masks on 1D data but the `collapse_2D_masks_mode` is not 'AND/OR'"
                                )

                        else:
                            raise Exception(
                                "+++++ BE AWARE! This is a possible mis-behavior! +++++\n"
                                + f"You are trying to fill the histogram {name} with data of dimention 1 (variable by event)"
                                + "and masking it with a mask with more than 1 dimension (e.g. mask on Jets)\n"
                                + "This means that you are either performing a cut on a collections (e.g Jets),"
                                + " or you are using subsamples with cuts on collections.\n"
                                + "\n As an example of a bad behaviour would be saving the pos=1 of a collection e.g. `JetGood.pt[1]`\n"
                                + "while also having a 2D cut on the `JetGood` collection --> this is not giving you the second jet passing the cut!\n"
                                + "In that case the 2nd JetGood.pt will be always plotted even if masked by the 2D cut: in fact "
                                + "the 2D masks would be collapsed to the event dimension. \n\n"
                                + "If you really wish to save the histogram with a single value for event (data dim=1)"
                                + "you can do so by configuring the histogram with `collapse_2D_masks=True\n"
                                + "The 2D masks will be collapsed on the event dimension (axis=1) doing an OR (default) or an AND\n"
                                + "You can configure this behaviour with `collapse_2D_masks_mode='OR'/'AND'` in the histo configuration."
                            )

                    # Mask the variables and flatten them
                    # save the isnotnone and datastructure
                    # to be able to broadcast the weight
                    has_none_mask = False
                    all_axes_isnotnone = None
                    has_data_structure = False
                    data_structure = None
                    fill_numeric_masked = {}
                    # loop on the cached numerical filling
                    for field, data in fill_numeric.items():
                        masked_data = data[mask]
                        # For each field we need to mask and flatten
                        if data_ndim > 1:
                            # We need to flatten and
                            # save the data structure for weights propagation
                            if not has_data_structure:
                                data_structure = ak.ones_like(masked_data)
                                has_data_structure = True
                            # flatten the data in one dimension
                            masked_data = ak.flatten(masked_data)

                        # check isnotnone AFTER the flattening
                        if not has_none_mask:  # this is the first axis analyzed
                            all_axes_isnotnone = ~ak.is_none(masked_data)
                            has_none_mask = True
                        else:
                            all_axes_isnotnone = all_axes_isnotnone & (
                                ~ak.is_none(masked_data)
                            )
                        # Save the data for the filling
                        fill_numeric_masked[field] = masked_data

                    # Now apply the isnone mask to all the numeric fields already masked
                    for key, value in fill_numeric_masked.items():
                        # we also convert it to numpy to speedup the hist filling
                        fill_numeric_masked[key] = ak.to_numpy(
                            value[all_axes_isnotnone], allow_missing=False
                        )

                    # Ok, now we have all the numerical axes with
                    # data that has been masked, flattened
                    # removed the none value --> now we need weights for each variation
                    if not histo.no_weights and self.isMC:
                        if shape_variation == "nominal":
                            # if we are working on nominal we fill all the weights variations
                            for variation in histo.hist_obj.axes["variation"]:
                                if variation in self.available_shape_variations:
                                    # We ignore other shape variations when
                                    # we are already working on a shape variation
                                    continue
                                # Only weights variations, since we are working on nominal sample
                                # Check if this variation exists for this category
                                if variation not in weights[category]:
                                    # it means that the variation is in the axes only
                                    # because it is requested for another category
                                    # In this case we fill with the nominal variation
                                    # We get the weights for the current category
                                    weight_varied = weights[category]["nominal"]
                                else:
                                    # We get the weights for the current category
                                    weight_varied = weights[category][variation]

                                # Broadcast and mask the weight (using the cached value if possible)
                                weight_varied = mask_and_broadcast_weight(
                                    category,
                                    subsample,
                                    variation,
                                    weight_varied,
                                    mask,
                                    data_structure,
                                )
                                if custom_weight != None and name in custom_weight:
                                    weight_varied = weight_varied * mask_and_broadcast_weight(
                                        category + "customW",
                                        subsample,
                                        variation,
                                        custom_weight[
                                            name
                                        ],  # passing the custom weight to be masked and broadcasted
                                        mask,
                                        data_structure,
                                    )

                                # Then we apply the notnone mask
                                weight_varied = weight_varied[all_axes_isnotnone]
                                # Fill the histogram
                                try:
                                    self.histograms[subsample][name].hist_obj.fill(
                                        cat=category,
                                        variation=variation,
                                        weight=weight_varied,
                                        **{**fill_categorical, **fill_numeric_masked},
                                    )
                                except Exception as e:
                                    raise Exception(
                                        f"Cannot fill histogram: {name}, {histo} {e}"
                                    )
                        else:
                            # Working on shape variation! only nominal weights
                            # (also using the cache which is cleaned for each shape variation
                            # at the beginning of the function)
                            weights_nom = mask_and_broadcast_weight(
                                category,
                                subsample,
                                "nominal",
                                weights[category]["nominal"],
                                mask,
                                data_structure,
                            )
                            if custom_weight != None and name in custom_weight:
                                weight_varied = weight_varied * mask_and_broadcast_weight(
                                    category + "customW",
                                    subsample,
                                    "nominal",
                                    custom_weight[
                                        name
                                    ],  # passing the custom weight to be masked and broadcasted
                                    mask,
                                    data_structure,
                                )
                            # Then we apply the notnone mask
                            weights_nom = weights_nom[all_axes_isnotnone]
                            # Fill the histogram
                            try:
                                self.histograms[subsample][name].hist_obj.fill(
                                    cat=category,
                                    variation=shape_variation,
                                    weight=weights_nom,
                                    **{**fill_categorical, **fill_numeric_masked},
                                )
                            except Exception as e:
                                raise Exception(
                                    f"Cannot fill histogram: {name}, {histo} {e}"
                                )

                    elif (
                        histo.no_weights and self.isMC
                    ):  # NO Weights modifier for the histogram
                        try:
                            self.histograms[subsample][name].hist_obj.fill(
                                cat=category,
                                variation="nominal",
                                **{**fill_categorical, **fill_numeric_masked},
                            )
                        except Exception as e:
                            raise Exception(
                                f"Cannot fill histogram: {name}, {histo} {e}"
                            )

                    elif not self.isMC:
                        # Fill histograms for Data
                        try:
                            self.histograms[subsample][name].hist_obj.fill(
                                cat=category,
                                **{**fill_categorical, **fill_numeric_masked},
                            )
                        except Exception as e:
                            raise Exception(
                                f"Cannot fill histogram: {name}, {histo} {e}"
                            )
                    else:
                        raise Exception(
                            f"Cannot fill histogram: {name}, {histo}, not implemented combination of options"
                        )


###################
# Utilities to handle the Weights cache


def weights_cache(fun):
    '''
    Function decorator to cache the weights calculation when they are ndim=1 on data_structure of ndim=1.
    The weight is cached by (category, subsample, variation)
    '''
    weights_cache.cache_ = {}

    def inner(category, subsample, variation, weight, mask, data_structure):
        if mask.ndim == 2:
            # Do not cache
            return fun(weight, mask, data_structure)
        # Cache only in the "by event" weight, which does not need to be
        # broadcasted on the data dimension.
        elif mask.ndim == 1 and (
            (data_structure is None) or (data_structure.ndim == 1)
        ):
            name = (category, subsample, variation)
            if name not in weights_cache.cache_:
                weights_cache.cache_[name] = fun(weight, mask, data_structure)

            return weights_cache.cache_[name]
        else:
            # if the mask is 2d, do not cache
            return fun(weight, mask, data_structure)

    return inner


@weights_cache
def mask_and_broadcast_weight(weight, mask, data_structure):
    '''
    The function mask the weights and broadcast them to the correct dimension.
    The `data_structure` input is an array of 1-value with the structure of the data ALREADY masked.
    We need instead to mask the weight value and broadcast it.

    We need to handle different cases:
    - Mask dimension=1 (mask on events):
       If the data_structure.dim = 2 it means that we want to plot a collection
       - we mask the weights by events (data is already masked)
       - broadcast weight to the collection by multiplying to the datastructure (1-like array)
       - flatten the final weight
       If the data_structure.dim = 1:
       - We just mask the weight by event

    - Mask dimension=2 (mask on the collection)
      It means that we are masking the collection, not the events.
      - First we broadcast the weight to the structure of the mask
      - Then we apply the mask
      - Then we flatten the weight

    '''
    if mask.ndim == 1 and not (data_structure is None) and data_structure.ndim == 2:
        # If the mask has dim =1 and the data dim =2
        # we need to mask the weight on dim=1, then to broadcast
        # on the data_structure -> then flatten
        allow_missing = False
        if ak.sum(ak.is_none(data_structure, axis=-1)) > 0:
            data_structure = ak.fill_none(data_structure, 0.)

        return ak.to_numpy(
            ak.flatten(data_structure * (weight[mask])), allow_missing=False
        )

    elif mask.ndim == 2:
        # First we broadcast the weight then we mask
        # if the mask is ndim==2 also the data is ndim==2.
        # The weights are broadcasted at collection level, then masked, then flattened.
        return ak.to_numpy(
            ak.flatten((ak.ones_like(mask) * weight)[mask]), allow_missing=False
        )
    else:
        return ak.to_numpy(weight[mask], allow_missing=False)
