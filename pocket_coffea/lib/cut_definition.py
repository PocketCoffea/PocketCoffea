from dataclasses import dataclass, field
from collections.abc import Callable
import awkward as ak
import json
import inspect


@dataclass
class Cut:
    """Class for keeping track of a cut and its parameters."""

    name: str
    params: dict
    function: Callable
    collection: str = "events"
    _id: str = field(init=False, repr=True, hash=True, default=None)

    def get_mask(self, events, **kwargs):
        '''The function get called from the processor and the params are passed by default as the second argument.
        Additional parameters as the year, sample name or others can be included by the processor and are passed to the function.
        '''
        return self.function(events, params=self.params, **kwargs)

    def __hash__(self):
        '''The Cut is unique by its name, the  function, and the dict of parameters.'''
        return hash((self.name, json.dumps(self.params), self.function))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        return f"Cut: {self.name}, f:{self.function.__name__}"

    @property
    def id(self):
        '''The id property must be used inside the framework to
        identify the cut instead of the name.  It represents the cut
        in a human-readable way, but keeping into account also the
        hash value for uniquiness.
        '''
        if self._id == None:
            self._id = f"{self.name}__{hash(self)}"
        return self._id

    def serialize(self, src_code=False):
        out = {
            "name": self.name,
            "params": self.params,
            "collection": self.collection,
            "function": {
                "name": self.function.__name__,
                "module": self.function.__module__,
                "src_file": inspect.getsourcefile(self.function),
                "f_hash": hash(self.function),
            },
            "id": self.id,
        }
        if src_code:
            out["function"]["src_code"] = inspect.getsource(self.function)
        return out
