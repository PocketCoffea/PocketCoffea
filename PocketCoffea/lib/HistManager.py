import hist
from collections import namedtuple
from typing import List, Tuple
from dataclasses import dataclass, field

@dataclass
class Axis():
    field: str # variable to plot
    bins: int
    start: float
    stop: float
    label: str
    coll: str = "events"  # Collection or events
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
    hist = None

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
    elif ax.type == "integer":
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
            overflow=ax.overflow,
            underflow=ax.underflow,
            growth=ax.growth)
                    


class HistManager():

    def __init__(self, config, sample, categories, variations_config, custom_axes=[]):
         self.histograms = {}        
         self.available_categories = categories
         # Prepare the variations Axes summing all the required variations
         # The variation config is organized as the weights one, by sample and by category
         self.available_variations = ["nominal"] + variations_config["weights"]["inclusive"] + \
                                     [ v for v in variations_config["weights"]["bycategory"].values()]
         # TODO Add the list of shape variations
         
         for name, hist_conf in config.items():
             # Check if the histogram is active for the current sample
             if hist_conf.only_samples != None:
                 if sample not in hist_conf.only_samples:
                     continue
             elif hist_conf.exclude_samples != None:
                 if sample in hist_conf.exclude_samples:
                     continue
             # Now we handle the selection of the categories
             cats = [ ]
             for c in self.available_categories:
                 if hist_conf.only_categories!=None:
                     if c in hist_conf.only_categories:
                         cats.append(c)
                 elif hist_conf.exclude_categories !=None:
                     if c not in hist_conf.exclude_categories:
                         cats.append(c)
                 else:
                         cats.append(c)
             cat_ax = hist.axis.StrCategory(cats,label="cat", growth=False)
             # Variation axes
             if hist_conf.variations:
                 if hist_conf.only_variations !=None:
                     # filtering the variation list with the available ones
                      var_ax = hist.axis.StrCategory(filter(lambda v: v in hist_conf.only_variations,
                                                    self.available_variations), label="variation", growth=False)
                 else:
                     var_ax = hist.axis.StrCategory(self.available_variations, label="variation", growth=False)
             else:
                 var_ax = hist.axis.StrCategory(["nominal"], label="variation", growth=False)
             # prepare the axes
             fields_axes = []
             for ax in hist_conf.axes:
                 fields_axes.append(_get_hist_axis(ax))

             # Build the histogram object with the additional axes    
             histogram = hist.Hist(
                 *(custom_axes + [cat_ax, var_ax] + fields_axes), storage=hist_conf.storage,
                 name="Counts"
             )
             #Save the hist in the configuration and store the full config object
             hist_conf.hist = histogram
             self.histograms[name] = hist_conf

    def fill_histograms(self, events, weights, ):
        pass
    
