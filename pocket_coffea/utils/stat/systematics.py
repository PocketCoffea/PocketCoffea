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
        if self.value is None and not isinstance(self.processes, dict):
            raise UserWarning(
                """Value is not defined and processes is not a dictionary.\n
                    Define a value that applies to all processes, or define a dictionary
                    of processes with values for each key as an entry.
                """
            )
        if self.value is not None:
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


class Systematics(dict[str, SystematicUncertainty]):
    """Store information of a list of systematic uncertainties"""

    def __init__(self, systematics: list[SystematicUncertainty]) -> None:
        """Store systematics in a dictionary with custom functions.

        :param systematics: List of systematic uncertainties
        :type systematics: list[SystematicUncertainty]
        """
        assert all(isinstance(syst, SystematicUncertainty) for syst in systematics), (
            "All elements must be of type SystematicUncertainty"
        )
        super().__init__({systematic.name: systematic for systematic in systematics})

    def list_type(self, syst_type: str) -> list[str]:
        """List of Names of Systematics of a specific type."""
        return [key for key in self if self[key].typ == syst_type]

    def get_systematics_by_type(self, syst_type: str) -> list[SystematicUncertainty]:
        """List of Systematics of a specific type."""
        return {name: syst for name, syst in self.items() if syst.typ == syst_type}
