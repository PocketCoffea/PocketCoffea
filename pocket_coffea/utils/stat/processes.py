"""Physical Processes as Dataclasses and Utilities"""

from dataclasses import dataclass, field


@dataclass
class Process:
    """Class to store information of a physical process."""

    name: str
    is_signal: bool
    samples: list[str] = None
    label: str = None
    id: int = field(init=False, default=None)

    def __post_init__(self):
        if not self.label:
            self.label = self.name

        if self.samples is None:
            self.samples = [self.name]


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
        return [name for name, process in self.items() if process.is_signal]

    @property
    def background_processes(self) -> list[str]:
        """List of Names of all Background Processes."""
        return [name for name, process in self.items() if not process.is_signal]

    def get_all_samples(self) -> set:
        """Get all samples from all processes."""
        return {sample for process in self.values() for sample in process.samples}
