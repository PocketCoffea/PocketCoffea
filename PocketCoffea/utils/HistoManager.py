import hist

class HistoManager():

    def __init__(self, config, additional_axes=[]):
         self.histograms = {}

         for name, hist_config in config.items():
             # prepare the axes
             axes = [ ]
             for ax in hist_config.axes:
                 if ax.type == "regular":
                     axes.append(hist.axis.Regular(
                         name=ax.field,
                         bins=ax.bins,
                         start=ax.start,
                         stop=ax.stop,
                         label=ax.label,
                         transform=ax.transform,
                         overflow=ax.overflow,
                         underflow=ax.underflow,
                         growth=ax.growth))
                 elif ax.type == "variable":
                     axes.append(hist.axis.Variable(
                         ax.bins, 
                         name=ax.field,
                         label=ax.label,
                         overflow=ax.overflow,
                         underflow=ax.underflow,
                     growth=ax.growth))
                 elif ax.type == "integer":
                     axes.append(hist.axis.Variable(
                         name=ax.field,
                         start=ax.start,
                         stop=ax.stop,
                         label=ax.label,
                         overflow=ax.overflow,
                         underflow=ax.underflow,
                         growth=ax.growth))
                 elif ax.type == "intcat":
                     axes.append(hist.axis.IntCategory(
                         ax.bins, 
                         name=ax.field,
                         label=ax.label,
                         overflow=ax.overflow,
                         underflow=ax.underflow,
                         growth=ax.growth))
                 elif ax.type == "strcat":
                     axes.append(hist.axis.StrCategory(
                         ax.bins, 
                         name=ax.field,
                         label=ax.label,
                         overflow=ax.overflow,
                         underflow=ax.underflow,
                         growth=ax.growth)
                                 )
             self.histograms[name] = hist.Hist(
                 *(additional_axes + axes), storage=hist_config.storage,
                 name="Counts"
                 # Add systematic axes
             )

            
