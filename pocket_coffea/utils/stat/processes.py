"""Physical Processes as Dataclasses and Utilities"""

import warnings
from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class Process:
    """Class to store information of a physical process

    :param name: Name of the process
    :param samples: Iterable of sample names associated with the process
    :param label: Label for the process, defaults to `name` if not specified
    :param is_data: Whether the process is data (needs to be set by subclasses)

    .. note::
        It is recommended to use the `MCProcess` or `DataProcess` subclasses directly.
        This base class is primarily for shared attributes and methods.

    """

    name: str
    samples: Iterable
    label: str = None
    is_data: bool = field(init=False)

    def __post_init__(self):
        if not hasattr(self, "is_data"):
            warnings.warn(
                f"'is_data' attribute not set for {self.name}. "
                "It is recommended to use MCProcess or DataProcess subclasses."
            )
        if not self.label:
            self.label = self.name
        if not isinstance(self.samples, list):
            self.samples = list(self.samples)


@dataclass(kw_only=True)
class MCProcess(Process):
    """Class to store information of a Monte Carlo process

    :param name: Name of the process
    :param samples: Iterable of sample names associated with the process
    :param years: Iterable of years the process is relevant for
    :param is_signal: Whether the process is a signal process
    :param has_rateParam: Whether the process has a rate parameter, defaults to False
    :param label: Label for the process, defaults to `name` if not specified

    Inherits from Process and sets is_data to False by default.
    """

    is_signal: bool
    years: Iterable
    has_rateParam: bool = False

    def __post_init__(self):
        self.is_data = False
        super().__post_init__()


@dataclass(kw_only=True)
class DataProcess(Process):
    """Class to store information of a Data process

    :param name: Name of the process
    :param samples: Iterable of sample names associated with the process
    :param years: Iterable of years the process is relevant for
    :param label: Label for the process, defaults to `name` if not specified

    Inherits from Process and sets is_data to True by default.
    """

    def __post_init__(self):
        self.is_data = True
        super().__post_init__()


class MCProcesses(dict[str, MCProcess]):
    """Custom dict to store information of multiple MC processes.

    :param processes: List of processes
    :type processes: list[Process]
    """

    def __init__(self, processes: list[MCProcess]) -> None:
        if not all(isinstance(process, MCProcess) for process in processes):
            raise TypeError(f"All elements of {processes} must be of type MCProcess")
        super().__init__({process.name: process for process in processes})

    @property
    def signal_processes(self) -> list[str]:
        """Names of all Signal MC Processes."""
        return [
            f"{name}_{year}"
            for name, process in self.items()
            for year in process.years
            if process.is_signal
        ]

    @property
    def background_processes(self) -> list[str]:
        """Names of all Background Processes."""
        return [
            f"{name}_{year}"
            for name, process in self.items()
            for year in process.years
            if not process.is_signal
        ]

    @property
    def n_processes(self) -> int:
        """Number of Processes"""
        return len(self.signal_processes) + len(self.background_processes)


class DataProcesses(dict[str, DataProcess]):
    """Custom dict to store information of multiple data processes.

    :param processes: List of processes
    :type processes: list[Process]
    """

    def __init__(self, processes: list[DataProcess]) -> None:
        if not all(isinstance(process, DataProcess) for process in processes):
            raise TypeError(f"All elements of {processes} must be of type DataProcess")
        super().__init__({process.name: process for process in processes})
