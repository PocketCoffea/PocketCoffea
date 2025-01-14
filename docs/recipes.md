# HOW-TOs for common tasks

:::{alert}
Page under construction! Come back for more common analysis steps recipes.
:::

## HLT trigger selection

## Define a new cut function


## Skimming events
WIP

## Subsamples
WIP


### Primary dataset cross-cleaning
WIP


## Define a custom weight


### Define a custom weights with custom variations

## Apply corrections
### MET-xy
From a purely physical point of view, the distribution of the $\phi$-component of the missing transverse momentum (a.k.a. MET) should be uniform due to rotational symmetry. However, for a variety of detector-related reasons, the distribution is not uniform in practice, but shows a sinus-like behaviour. To correct this behaviour, the x- and y-component of the MET can be altered in accordance to the recommendation of JME. In the PocketCoffea workflow, these corrections can be applied using the `met_xy_correction()` function:
```
from pocket_coffea.lib.jets import met_xy_correction
met_pt_corr, met_phi_corr = met_xy_correction(self.params, self.events, self._year, self._era)
```
Note, that this shift also alters the $p_\mathrm{T}$ component! Also, the corrections are only implemented for Run2 UL (thus far).
