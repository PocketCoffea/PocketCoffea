import hist
import awkward as ak
from collections import defaultdict
from typing import List, Tuple
from dataclasses import dataclass, field


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

    def serialize(self):
        out = {**self.__dict__}
        out["axes"] = [a.__dict__ for a in self.axes]
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
        sample,
        categories_config,
        variations_config,
        custom_axes=[],
        isMC=True,
    ):
        self.isMC = isMC
        self.histograms = {}
        self.categories_config = categories_config
        self.variations_config = variations_config
        self.available_categories = set(self.categories_config.keys())
        self.available_variations = ["nominal"]
        if self.isMC:
            self.available_variations_bycat = defaultdict(list)
            for cat, vars in self.variations_config["weights"].items():
                self.available_variations_bycat[cat].append("nominal")
                for var in vars:
                    vv = [f"{var}Up", f"{var}Down"]
                    self.available_variations += vv
                    self.available_variations_bycat[cat] += vv
            self.available_variations = set(self.available_variations)
        # Prepare the variations Axes summing all the required variations
        # The variation config is organized as the weights one, by sample and by category

        for name, hcfg in hist_config.items():
            # Check if the histogram is active for the current sample
            if hcfg.only_samples != None:
                if sample not in cfg.only_samples:
                    continue
            elif hcfg.exclude_samples != None:
                if sample in hcfg.exclude_samples:
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
            for ax in hcfg.axes:
                all_axes.append(get_hist_axis_from_config(ax))

            # Build the histogram object with the additional axes
            hcfg.hist_obj = hist.Hist(*all_axes, storage=hcfg.storage, name="Counts")
            # Save the hist in the configuration and store the full config object
            self.histograms[name] = hcfg

    def get_histograms(self):
        # Exclude by default metadata histo
        return {
            key: h.hist_obj for key, h in self.histograms.items() if not h.metadata_hist
        }

    def get_metadata_histograms():
        return {
            key: h.hist_obj for key, h in self.histograms.items() if h.metadata_hist
        }

    def get_histogram(self, name):
        return self.histograms[name]

    def fill_histograms(self, events, weights_manager, cuts_masks, custom_fields=None):
        '''
        We loop on the configured histograms only
        Doing so the catergory, sample, variation selections are handled correctly (by the constructor).

        Custom_fields is a dict of additional array. The expected lenght of the first dimension is the number of
        events. The categories mask will be applied.
        '''
        # print("===> Filling histograms")
        for category in self.available_categories:
            # print(f"\tCategory: {category}")
            # Getting the cut mask
            mask = cuts_masks.all(*self.categories_config[category])
            masked_events = events[mask]
            # Weights must be computed for each category and variation,
            # but it is not needed to recompute them for each histo.
            # The weights will be flattened if needed for each histo
            # map of weights in this category, with mask APPLIED
            if self.isMC:
                weights = {}
                for variation in self.available_variations_bycat[category]:
                    if variation == "nominal":
                        weights["nominal"] = weights_manager.get_weight(category)[mask]
                    else:
                        # Check if the variation is available in this category
                        weights[variation] = weights_manager.get_weight(
                            category, modifier=variation
                        )[mask]
                        # print(f"\t\t= Weights [{variation}] = {weights[variation]} ")

            for name, histo in self.histograms.items():
                # print(f"\t\tFilling histo {name}")
                if category not in histo.only_categories:
                    continue
                if not histo.autofill:
                    continue
                if histo.metadata_hist:
                    continue  # TODO dedicated function for metadata histograms
                # Get the filling axes and then manipulate the weights
                # for each variation to be compatible with the shape

                fill_categorical = {}
                fill_numeric = {}
                ndim = None
                has_none_mask = False
                all_axes_isnotnone = None
                has_data_structure = False
                data_structure = None
                for ax in histo.axes:
                    # print(f"\t\t\tFilling axes {ax}")
                    # Checkout the collection type
                    if ax.type in ["regular", "variable", "int"]:
                        if ax.coll == "events":
                            # These are event level information
                            data = masked_events[ax.field]
                        elif ax.coll == "metadata":
                            data = events.metadata[ax.field]
                        elif ax.coll == "custom":
                            # taking the data from the custom_fields argument
                            # IT MUST be a per-event number, so we expect an array to mask
                            data = custom_fields[ax.field][mask]
                        else:
                            if ax.coll not in events.fields:
                                raise ValueError(
                                    f"Collection {ax.coll} not found in events!"
                                )
                            # General collections
                            if ax.pos == None:
                                data = masked_events[ax.coll][ax.field]
                            elif ax.pos >= 0:
                                data = ak.pad_none(
                                    masked_events[ax.coll][ax.field], ax.pos + 1, axis=1
                                )[:, ax.pos]
                            else:
                                raise Exception(
                                    f"Invalid position {ax.pos} requested for collection {ax.coll}"
                                )

                        # Flattening
                        if ndim == None:
                            ndim = data.ndim
                        elif ndim != data.ndim:
                            raise Exception(
                                f"Incompatible shapes for Axis {ax} of hist {histo}"
                            )
                        # If we have multidim data we need to flatten it
                        # and to save the structure to reflatten the weights
                        # accordingly
                        if ndim > 1:
                            # Save the data structure for weights propagation
                            if not has_data_structure:
                                data_structure = ak.ones_like(data)
                                had_data_structure = True
                            # flatten the data in one dimension
                            data = ak.flatten(data)

                        # Filling the numerical axes
                        fill_numeric[ax.name] = data
                        # check isnotnone AFTER the flattening
                        if not has_none_mask:  # this is the first axis analyzed
                            all_axes_isnotnone = ~ak.is_none(data)
                            has_none_mask = True
                        else:
                            all_axes_isnotnone = all_axes_isnotnone & (
                                ~ak.is_none(data)
                            )
                        # The nontnone filter will be applied at the end
                        # to all the numerical axes

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

                # Now apply the isnone mask to all the numeric fileds
                for key, value in fill_numeric.items():
                    fill_numeric[key] = value[all_axes_isnotnone]

                # Ok, now we have all the numerical axes with
                # data that has been masked, flattened
                # removed the none value --> now we need weights for each variation
                if not histo.no_weights and self.isMC:
                    for variation in histo.hist_obj.axes["variation"]:
                        # print(f"\t\t\tFilling variation: {variation}")
                        # Check if this variation exists for this category
                        if variation not in weights:
                            # it means that the variation is in the axes only
                            # because it is requested for another category
                            # In this case we fill with the nominal variation
                            weight_varied = weights["nominal"]
                        else:
                            weight_varied = weights[variation]
                        # Check number of dimensione
                        if ndim > 1:
                            weight_varied = ak.flatten(data_structure * weight_varied)
                        # Then we apply the notnone mask
                        weight_varied = weight_varied[all_axes_isnotnone]
                        # Fill the histogram
                        try:
                            histo.hist_obj.fill(
                                cat=category,
                                variation=variation,
                                weight=weight_varied,
                                **{**fill_categorical, **fill_numeric},
                            )
                        except:
                            raise Exception(f"Cannot fill histogram: {name}, {histo}")

                elif (
                    histo.no_weights and self.isMC
                ):  # NO Weights modifier for the histogram
                    try:
                        histo.hist_obj.fill(
                            cat=category,
                            variation=variation,
                            **{**fill_categorical, **fill_numeric},
                        )
                    except:
                        raise Exception(f"Cannot fill histogram: {name}, {histo}")

                elif not self.isMC:
                    # Fill histograms for Data
                    try:
                        histo.hist_obj.fill(
                            cat=category,
                            **{**fill_categorical, **fill_numeric},
                        )
                    except:
                        raise Exception(f"Cannot fill histogram: {name}, {histo}")
                else:
                    raise Exception(
                        f"Cannot fill histogram: {name}, {histo}, not implemented combination of options"
                    )
