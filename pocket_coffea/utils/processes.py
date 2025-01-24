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
    label: str = None

    def __post_init__(self):
        if not self.label:
            self.label = self.name
        if not isinstance(self.samples, list):
            self.samples = list(self.samples)

@dataclass
class Processes:
    """Class to store information of a list of processes"""

    processes: list[Process]

    @property
    def processes_names(self) -> list[str]:
        """List of Names of all Processes."""
        return [f"{process.name}_{year}" for process in self.processes for year in process.years]

    @property
    def signal_processes(self) -> list[str]:
        """List of Names of all Signal Processes."""
        return [f"{process.name}_{year}" for process in self.processes for year in process.years if process.is_signal]

    @property
    def background_processes(self) -> list[str]:
        """List of Names of all Background Processes."""
        return [f"{process.name}_{year}" for process in self.processes for year in process.years if not process.is_signal]

    @property
    def n_processes(self) -> int:
        """Number of Processes"""
        return len(self.processes_names)
