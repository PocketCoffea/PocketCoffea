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
    type: str = "regular"
    transform: str = None
    lim: Tuple[float] = (0,0)
    underflow: bool = True
    overflow: bool = True
    growth: bool = False

@dataclass
class HistConf():
    axes: List[Axis]
    storage: str = "weight"
    autofill: bool = True
    variations: bool = True
    only_variations: List[str] = None 
    exclude_samples: List[str] = None
    only_samples: List[str] = None
    exclude_categories: List[str] = None
    only_categories: List[str] = None

    def serialize(self):
        out= { **self.__dict__ }
        out["axes"] = [a.__dict__ for a in self.axes]
        return out

def _get_hist_axis(ax: Axis):
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
        return hist.axis.Variable(
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
         self.available_categories = list(self.categories_config.keys())
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
                 var_ax = hist.axis.StrCategory(hcfg.only_variations,
                                                name="variation", label="Variation", growth=False)
             else:
                 var_ax = hist.axis.StrCategory(["nominal"],name="variation", label="Variation", growth=False)
                 hcfg.only_variations = ["nominal"]

             # Axis in the configuration + custom axes
             fields_axes = []
             # the custom axis get included in the hcfg for future use
             hcfg.axes = custom_axes + hcfg.axes
             for ax in hcfg.axes:
                 fields_axes.append(_get_hist_axis(ax))

             # Build the histogram object with the additional axes    
             histogram = hist.Hist(
                 *([cat_ax, var_ax] + fields_axes), storage=hcfg.storage,
                 name="Counts"
             )
             #Save the hist in the configuration and store the full config object
             self._hist_objs[name] = histogram
             self.histograms[name] = hcfg


    def get_histograms(self):
        return { key:h for key, h in self._hist_objs.items()}

    def fill_histograms(self, events, weights, cuts_masks, custom_fields=None):
        '''
        We loop on the configured histograms only
        Doing so the catergory, sample, variation selections are handled correctly (by the constructor)
        '''
        for name, histo in self.histograms.items():
            # Skipping not autofill histograms
            if histo.autofill == False: continue
            hobj = self._hist_objs[name]
            #print(name, histo)
            #print("axes", hobj.axes)

            # Loop on the categories
            for category in hobj.axes[0]: #first axis is the category
                for variation in hobj.axes[1]: #second axis is alway the variation
                    # Using directly the axes of the histogram
                    # we are sure to use only the variations configured by the user
                    # If not variations are requested only the "nominal"
                    # variation is present.
                    # print(category, variation)
                    fill_categorical = {
                        "cat":category,
                        "variation": variation
                    }
                    fill_numeric = {}

                    # Getting the cut mask
                    mask = cuts_masks.all(*self.categories_config[category]) # Move this out
                    if variation == "nominal":
                        weight = weights.get_weight(category)[mask]
                    else:
                        weight = weights.get_weight(category, modifier=variation)[mask]

                    flattened = False
                    global_isnotnone = None

                    # Looping on Axis definition to get the collection and metadata
                    # all the axes have been included in the configuration object
                    for ax in histo.axes:
                        # Move all of this output of the variation
                        #print(ax)
                        # Checkout the collection type
                        if ax.type in ["regular", "variable", "int"]:
                            if ax.coll == "events":
                                # These are event level information
                                data = events[mask][ax.field]
                            elif ax.coll == "metadata":
                                data = events.metadata[ax.field]
                            elif ax.coll not in ["events", "metadata"]:
                                # General collections
                                if ax.pos == None:
                                    data = events[mask][ax.coll][ax.field]
                                elif ax.pos >=0:
                                    data = ak.pad_none(events[mask][ax.coll][ax.field], ax.pos, axis=1)
                                else:
                                    raise Exception(f"Invalid position {ax.pos} requested for collection {ax.coll}")
                            elif ax.coll == "custom":
                                # taking the data from the custom_fields argument
                                data = custom_fields[mask][ax.field]
                            else:
                                raise NotImplementedError()
                            #print(data)
                            # Flattening
                            if data.ndim > 1:
                                # it needs to be flattened
                                # and the weight must be brought down in the dimension
                                if flattened == False:
                                    weight = ak.flatten(ak.ones_like(data) * weight)
                                    flattened = True
                                #flatten the data in one dimension
                                data = ak.flatten(data)
                            # Filling the numerical axes
                            fill_numeric[ax.field] = data
                            #print(data)
                            # check isnotnone
                            if global_isnotnone==None:
                                global_isnotnone = ~ak.is_none(data)
                            else:
                                global_isnotnone &= ~ak.is_none(data)
                            # The nontnone filter will be applied at the end
                                                    

                        #### --> end of numeric axes
                        # Categorical axes (not appling the mask)
                        else:
                            if ax.coll == "metadata":
                                data = events.metadata[ax.field]
                                fill_categorical[ax.field] = data
                            elif ax.coll == "custom":
                                # taking the data from the custom_fields argument
                                data = custom_fields[ax.field]
                            else:
                                raise NotImplementedError()

                    # Now apply the isnone mask to all the numeric fileds
                    for key, value in fill_numeric.items():
                        value = value[global_isnotnone]
                    # Apply the isnone mask to weights
                    weight = weight[global_isnotnone]

                    # If the globalisnone mask is failing the number of
                    #objects will be different and everything will crash

                    # Finally fit the histogram
                    #print({**fill_categorical, **fill_numeric})
                    hobj.fill(**{**fill_categorical,
                                 **fill_numeric,},
                              weight=weight)
