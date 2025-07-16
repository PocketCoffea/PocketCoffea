"""Physical Processes as Dataclasses and Utilities"""

from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class Process:
    """Class to store information of a physical process
    
    :param name: Name of the process
    :param samples: Iterable of sample names associated with the process
    :param years: Iterable of years the process is relevant for, defaults to [None]
    :param is_signal: Whether the process is a signal process, defaults to False
    :param is_data: Whether the process is data, defaults to False
    :param has_rateParam: Whether the process has a rate parameter, defaults to True
    :param label: Label for the process, defaults to `name` if not specified

    Hints for the use with the Datacard class:
    - The years parameter is needed for MC processes
    - The years parameter has to be [None] for data processes
    """

    name: str
    samples: Iterable
    years: Iterable = field(default_factory=lambda: [None])
    is_signal: bool = False
    is_data: bool = False
    has_rateParam: bool = True
    label: str = None

    def __post_init__(self):
        if not self.label:
            self.label = self.name
        if not isinstance(self.samples, list):
            self.samples = list(self.samples)

class Processes(dict[str, Process]):
    """Class to store information of a list of processes"""

    def __init__(self, processes: list[Process]) -> None:
        """Store list of processes in a dictionary with custom functions.

        :param processes: List of processes
        :type processes: list[Process]
        """
        assert all(isinstance(process, Process) for process in processes), (
            "All elements must be of type Process"
        )
        super().__init__({process.name: process for process in processes})

    @property
    def signal_processes(self) -> list[str]:
        """List of Names of all Signal Processes."""
        return [f"{name}_{year}" for name, process in self.items() for year in process.years if process.is_signal]

    @property
    def background_processes(self) -> list[str]:
        """List of Names of all Background Processes."""
        return [f"{name}_{year}" for name, process in self.items() for year in process.years if not process.is_signal]

    @property
    def n_processes(self) -> int:
        """Number of Processes"""
        return len(self.signal_processes) + len(self.background_processes)
