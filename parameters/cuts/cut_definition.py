from dataclasses import dataclass
from collections.abc import Callable
import awkward as ak

@dataclass
class Cut:
    """Class for keeping track of an item in inventory."""
    name: str
    params: dict[str,...]
    function: Callable[[ak.Array, dict[str,...],... ], ak.Array]

    def get_mask(self, events, **kwargs):
        '''The function get called from the processor and the params are passed by default as the second argument.
        Additional parameters as the year, sample name or others can be included by the processor and are passed to the function. 
        '''
        return self.function(events, params=self.params, **kwargs )
