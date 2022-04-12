from dataclasses import dataclass
from collections.abc import Callable
import awkward as ak
import json

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

    def __hash__(self):
        '''The Cut is unique by its name, the  __name__ of the function and the set of parameters.'''
        return hash((self.name, json.dumps(self.params), self.function.__name__))

    def __str__(self):
        return f"Cut: {self.name}, f:{self.function.__name__}"

    def serialize(self):
        return {
            "name" : self.name,
            "params": self.params,
            "function": repr(self.function)
        }
