from collections.abc import Callable


@dataclass
class Cut:
    """Class for keeping track of an item in inventory."""
    name: str
    params: dict[str,...]
    function: Callable[[], ak.Array]
    
