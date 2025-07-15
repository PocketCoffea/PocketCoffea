"""Physical Processes as Dataclasses and Utilities"""

from typing import Iterable
from dataclasses import dataclass

@dataclass
class Process:
    """Class to store information of a physical process"""

    name: str
    samples: Iterable
    years: Iterable
    is_signal: bool
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
