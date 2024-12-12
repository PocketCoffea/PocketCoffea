"""Physical Processes as Dataclasses and Utilities"""

from typing import Iterable
from dataclasses import dataclass

@dataclass
class Process:
    """Class to store information of a physical process"""

    name: str
    samples: Iterable
    is_signal: bool
    is_data: bool = False
    label: str = None

    def __post_init__(self):
        if not self.label:
            self.label = self.name

    def __post_init_post_parse__(self):
        '''Ensure samples is a list'''
        if not isinstance(self.samples, list):
            self.samples = list(self.samples)


@dataclass
class Processes:
    """Class to store information of a list of processes"""

    processes: list[Process]

    @property
    def processes_names(self) -> list[str]:
        """List of Names of all Processes."""
        return [process.name for process in self.processes]

    @property
    def signal_processes(self) -> list[str]:
        """List of Names of all Signal Processes."""
        return [process.name for process in self.processes if process.is_signal]

    @property
    def background_processes(self) -> list[str]:
        """List of Names of all Background Processes."""
        return [process.name for process in self.processes if not process.is_signal]

    @property
    def n_processes(self) -> int:
        """Number of Processes"""
        return len(self.processes)
