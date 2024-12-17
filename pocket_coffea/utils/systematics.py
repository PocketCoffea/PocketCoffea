"""Systematic Uncertainties and Utilities for Statistical Analysis"""

from dataclasses import dataclass


@dataclass
class SystematicUncertainty:
    """Store information about one systematic uncertainty"""

    name: str
    typ: str
    processes: list[str] | tuple[str] | dict[str, float]
    value: float | tuple[float] = None

    def __post_init__(self):
        if self.value:
            if isinstance(self.processes, dict):
                raise UserWarning(
                    """Specified a dictionary of processes, but also a value.\n
                    Either define a list of processes and a value that applies to all of
                    them, or define a dictionary of processes with values for each key
                    as an entry.
                    """
                )

            if not isinstance(self.value, (tuple, float)):
                raise ValueError(
                    f"Value must be a float or a tuple, got {type(self.value)}"
                )

            if isinstance(self.value, tuple) and len(self.value) > 2:
                raise ValueError(
                    f"Value must be a tuple with 2 elements, got {len(self.value)}"
                )

            self.processes = {process: self.value for process in self.processes}


@dataclass
class Systematics:
    """Store information of a list of systematic uncertainties"""

    systematics: list[SystematicUncertainty]

    @property
    def systematics_names(self) -> list[str]:
        """List of Names of all Systematics."""
        return [systematic.name for systematic in self.systematics]

    @property
    def n_systematics(self) -> int:
        """Number of Systematics"""
        return len(self.systematics)

    def list_type(self, syst_type: str) -> list[str]:
        """List of Names of Systematics of a specific type."""
        return [
            systematic.name
            for systematic in self.systematics
            if systematic.typ == syst_type
        ]

    def get_systematics_by_type(self, syst_type: str) -> list[SystematicUncertainty]:
        """List of Systematics of a specific type."""
        return [
            systematic for systematic in self.systematics if systematic.typ == syst_type
        ]
