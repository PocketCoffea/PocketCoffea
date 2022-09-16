import hist
import awkward as ak
from collections import namedtuple
from typing import List, Tuple
from dataclasses import dataclass, field

@dataclass
class Axis():
    field: str # variable to plot
    label: str
    bins: int
    start: float = None
    stop: float = None
    coll: str = "events" # Collection or events or metadata or custom
    pos : int = None # index in the collection to plot. If None plot all the objects on the same histogram
    type: str = "regular"  #regular/variable/integer/intcat/strcat
    transform: str = None
    lim: Tuple[float] = (0,0)
    underflow: bool = True
    overflow: bool = True
    growth: bool = False

@dataclass
class HistConf():
    axes: List[Axis]
    storage: str = "weight"
    autofill: bool = True  # Handle the filling automatically
    variations: bool = True
    only_variations: List[str] = None 
    exclude_samples: List[str] = None
    only_samples: List[str] = None
    exclude_categories: List[str] = None
    only_categories: List[str] = None
    no_weights : bool = False     # Do not fill the weights
    metadata_hist : bool = False  # Non-event variables, for processing metadata

    def serialize(self):
        out= { **self.__dict__ }
        out["axes"] = [a.__dict__ for a in self.axes]
        return out

def get_hist_axis_from_config(ax: Axis):
    if ax.type == "regular":
        return hist.axis.Regular(
            name=ax.field,
            bins=ax.bins,
            start=ax.start,
            stop=ax.stop,
            label=ax.label,
            transform=ax.transform,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth)
    elif ax.type == "variable":
        return hist.axis.Variable(
            ax.bins, 
            name=ax.field,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth)
    elif ax.type == "int":
        return hist.axis.Integer(
            name=ax.field,
            start=ax.start,
            stop=ax.stop,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth)
    elif ax.type == "intcat":
        return hist.axis.IntCategory(
            ax.bins, 
            name=ax.field,
            label=ax.label,
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth)
    elif ax.type == "strcat":
        return hist.axis.StrCategory(
            ax.bins, 
            name=ax.field,
            label=ax.label,
            growth=ax.growth)
                    


class HistManager():

    def __init__(self, hist_config, sample, categories_config, variations_config, custom_axes=[]):
         self.histograms = {}
         self._hist_objs = {}
         self.categories_config = categories_config
         self.variations_config = variations_config
         self.available_categories = set(self.categories_config.keys())
         self.available_variations = ["nominal"]
         for cat, vars in self.variations_config["weights"].items():
             for var in vars:
                 self.available_variations += [f"{var}Up", f"{var}Down"]
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
             cats = [ ]
             for c in self.available_categories:
                 if hcfg.only_categories!=None:
                     if c in hcfg.only_categories:
                         cats.append(c)
                 elif hcfg.exclude_categories !=None:
                     if c not in hcfg.exclude_categories:
                         cats.append(c)
                 else:
                         cats.append(c)
             cat_ax = hist.axis.StrCategory(cats,name="cat", label="Category", growth=False)
             # Update the histConf to save the only category
             hcfg.only_categories = cats
             
             # Variation axes
             if hcfg.variations:
                 #Get all the variation
                 allvariat = []
                 for c in cats:
                     allvariat += self.variations_config["weights"][c]
                 allvariat = set(allvariat) 
                 if hcfg.only_variations !=None:
                     # filtering the variation list with the available ones
                     allvariat =  set(filter(lambda v: v in hcfg.only_variations, allvariat))
                    
                 hcfg.only_variations = ["nominal"] + \
                                               [var+"Up" for var in allvariat] + \
                                               [var+"Down" for var in allvariat]
             else:
                 hcfg.only_variations = ["nominal"]
             #Defining the variation axis
             var_ax = hist.axis.StrCategory(hcfg.only_variations, name="variation", 
                                            label="Variation", growth=False)
             # Axis in the configuration + custom axes
             fields_axes = []
             # the custom axis get included in the hcfg for future use
             hcfg.axes = custom_axes + hcfg.axes
             for ax in hcfg.axes:
                 fields_axes.append(get_hist_axis_from_config(ax))

             # Build the histogram object with the additional axes    
             histogram = hist.Hist(
                 *([cat_ax, var_ax] + fields_axes), storage=hcfg.storage,
                 name="Counts"
             )
             #Save the hist in the configuration and store the full config object
             self._hist_objs[name] = histogram
             self.histograms[name] = hcfg


    def get_histograms(self):
        # Exclude by default metadata histo
        return { key:h for key, h in self._hist_objs.items() if not self.histograms[key].metadata_hist}

    def get_metadata_histgrams():
        return { key:h for key, h in self._hist_objs.items() if self.histograms[key].metadata_hist}

    def get_histogram(self, name):
        return self.histograms[name], self._hist_objs[name]
    
    def fill_histograms(self, events, weights_manager, cuts_masks, custom_fields=None):
        '''
        We loop on the configured histograms only
        Doing so the catergory, sample, variation selections are handled correctly (by the constructor).

        Custom_fields is a dict of additional array. The expected lenght of the first dimension is the number of
        events. The categories mask will be applied.
        '''

        for category in self.available_categories:
            # Getting the cut mask
            mask = cuts_masks.all(*self.categories_config[category])

            # Weights must be computed for each category and variation,
            # but it is not needed to recompute them for each histo.
            # The weights will be flattened if needed for each histo
            # map of weights in this category, with mask APPLIED
            weights = {}
            for variation in self.available_variations:
                 if variation == "nominal":
                     weights["nominal"]  = weights_manager.get_weight(category)[mask]
                 else:
                     weights[variation] = weights_manager.get_weight(category, modifier=variation)[mask]

            for name, histo in self.histograms.items():
                if category not in histo.only_categories: continue
                if not histo.autofill: continue
                if histo.metadata_hist: continue  # TODO dedicated function for metadata histograms
                hobj = self._hist_objs[name]
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
                    # Checkout the collection type
                    if ax.type in ["regular", "variable", "int"]:
                        if ax.coll == "events":
                            # These are event level information
                            data = events[mask][ax.field]
                        elif ax.coll == "metadata":
                            data = events.metadata[ax.field]
                        elif ax.coll == "custom":
                            # taking the data from the custom_fields argument
                            # IT MUST be a per-event number, so we expect an array to mask
                            data = custom_fields[ax.field][mask]
                        else:
                            if ax.coll not in events.fields:
                                raise ValueError(f"Collection {coll} not found in events!")
                            # General collections
                            if ax.pos == None:
                                data = events[mask][ax.coll][ax.field]
                            elif ax.pos >=0:
                                data = ak.pad_none(events[mask][ax.coll][ax.field], ax.pos+1, axis=1)[:,ax.pos]
                            else:
                                raise Exception(f"Invalid position {ax.pos} requested for collection {ax.coll}")

                        # Flattening
                        if ndim == None:
                            ndim = data.ndim
                        elif ndim !=  data.ndim:
                            raise Exception(f"Incompatible shapes for Axis {ax} of hist {histo}")
                        # If we have multidim data we need to flatten it
                        # and to save the structure to reflatten the weights
                        # accordingly
                        if ndim > 1:
                            # Save the data structure for weights propagation
                            if not has_data_structure:
                                data_structure = ak.ones_like(data)
                                had_data_structure = True
                            #flatten the data in one dimension
                            data = ak.flatten(data)

                        # Filling the numerical axes
                        fill_numeric[ax.field] = data
                        # check isnotnone AFTER the flattening
                        if not has_none_mask:  # this is the first axis analyzed
                            all_axes_isnotnone = (~ak.is_none(data))
                            has_none_mask = True
                        else:
                            all_axes_isnotnone = all_axes_isnotnone & (~ak.is_none(data))
                        # The nontnone filter will be applied at the end
                        # to all the numerical axes

                    #### --> end of numeric axes
                    # Categorical axes (not appling the mask)
                    else:
                        if ax.coll == "metadata":
                            data = events.metadata[ax.field]
                            fill_categorical[ax.field] = data
                        elif ax.coll == "custom":
                            # taking the data from the custom_fields argument
                            data = custom_fields[ax.field]
                            fill_categorical[ax.field] = data
                        else:
                            raise NotImplementedError()

                # Now apply the isnone mask to all the numeric fileds
                for key, value in fill_numeric.items():
                    fill_numeric[key] = value[all_axes_isnotnone]

                # Ok, now we have all the numerical axes with
                # data that has been masked, flattened
                # removed the none value --> now we need weights for each variation
                if not histo.no_weights: 
                    for variation in hobj.axes["variation"]:
                        weight_varied = weights[variation]
                        # Check number of dimensione
                        if ndim > 1:
                            weight_varied = ak.flatten(data_structure*weight_varied)
                        # Then we apply the notnone mask
                        weight_varied = weight_varied[all_axes_isnotnone]
                        # Fill the histogram
                        hobj.fill(cat=category,
                                  variation=variation,
                                  weight=weight_varied,
                                  **{**fill_categorical,
                                     **fill_numeric},
                                  )
                else: # NO Weights modifier for the histogram
                    #if no_weights is requested only the nominal variation is filled with weight=1
                    if "weight" in histo.storage:
                        hobj.fill(cat=category,
                                  variation=variation,
                                  weight=np.ones(ak.sum(mask)), # weight==1 with lenght of the mask
                                  **{**fill_categorical,
                                     **fill_numeric},
                                  )
                    else:
                        hobj.fill(cat=category,
                                  variation=variation,
                                  **{**fill_categorical,
                                     **fill_numeric},
                                  )




        
