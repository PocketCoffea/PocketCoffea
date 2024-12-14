"""Systematic Uncertainties and Utilities for Statistical Analysis"""

from dataclasses import dataclass
from pocket_coffea.utils.processes import Process

@dataclass
class SystematicUncertainty:
    """Store information about one systematic uncertainty

    :param name: Name of the systematic uncertainty
    :param typ: Type of the systematic uncertainty (e.g. 'shape', 'lnN')
    :param processes: List of processes affected by the systematic uncertainty
    :param value: Value of the systematic uncertainty for all processes
    :param datacard_name: Name of the systematic uncertainty in the datacard
    :param correlated: Whether the systematic uncertainty is correlated between processes
    """

    name: str
    typ: str
    processes: list[str] | tuple[str] | dict[str, float]
    value: float | tuple[float] = None
    datacard_name: str = None

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

        if self.datacard_name is None:
            self.datacard_name = self.name


@dataclass
class Systematics:
    """Store information of a list of systematic uncertainties"""

    systematics: list[SystematicUncertainty]

    @property
    def systematics_names(self) -> list[str]:
        """List of Names of all Systematics."""
        return [systematic.name for systematic in self.systematics]

    @property
    def variations_names(self) -> list[str]:
        """List of Names of Shape Variations."""
        return [
            f"{syst}{shift}"
            for syst in [s.name for s in self.get_systematics_by_type("shape")]
            for shift in ("Up", "Down")
        ]

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

    def get_systematics_by_process(self, process: Process) -> list[SystematicUncertainty]:
        """List of Systematics that affect a specific process."""
        return [
            systematic for systematic in self.systematics if process.name in systematic.processes
        ]
