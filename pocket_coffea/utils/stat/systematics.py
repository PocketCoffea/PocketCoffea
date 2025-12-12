"""Systematic Uncertainties and Utilities for Statistical Analysis"""

from dataclasses import dataclass
from collections import defaultdict
from copy import deepcopy

from pocket_coffea.utils.stat.processes import Process


@dataclass
class SystematicUncertainty:
    """
    Store information about one systematic uncertainty.

    :param name: Name of the systematic uncertainty.
    :param typ: Type of the systematic uncertainty (e.g. 'shape', 'lnN').
    :param processes: List or tuple of process names affected, or a dict mapping process names to values.
    :param years: List or tuple of years the uncertainty applies to.
    :param value: Value (float or tuple of floats) of the uncertainty for all processes, or None if using a dict for processes.
    :param datacard_name: Name of the systematic uncertainty in the datacard. Defaults to `name` if not specified.
    """

    name: str
    typ: str
    processes: list[str] | tuple[str] | dict[str, float]
    years: list[str] | tuple[str]
    value: float | tuple[float] = None
    datacard_name: str = None

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

        if self.datacard_name is None:
            self.datacard_name = self.name


class Systematics(dict[str, SystematicUncertainty]):
    """Store information of a list of systematic uncertainties"""

    def __init__(self, systematics: list[SystematicUncertainty]) -> None:
        """Store systematics in a dictionary with custom functions.

        :param systematics: List of systematic uncertainties
        :type systematics: list[SystematicUncertainty]
        """
        if not all(isinstance(syst, SystematicUncertainty) for syst in systematics):
            raise TypeError(
                f"All elements of {systematics} must be of type SystematicUncertainty"
            )
        systematics = self.merge_duplicates(systematics)
        super().__init__(
            {systematic.datacard_name: systematic for systematic in systematics}
        )

    def merge_duplicates(self, systematics: list[SystematicUncertainty]) -> list[SystematicUncertainty]:
        """Merge systematics with the same name together to have them as correlated systematics in the datacard."""
        grouped: dict[tuple[str,str], list[SystematicUncertainty]] = defaultdict(list)

        for syst in systematics:
            key = (syst.datacard_name, syst.name)
            grouped[key].append(syst)

        merged_systematics: list[SystematicUncertainty] = []

        for (_,_), systs in grouped.items():
            if len(systs) == 1:
                merged_systematics.append(systs[0])
                continue
            base = deepcopy(systs[0])
            for other in systs[1:]:
                if base.typ != other.typ:
                    raise ValueError(
                        f"Inconsystent type of systematics with same name '{base.name}': "
                        f"{base.typ} vs {other.typ}"
                    )
                if base.years != other.years:
                    raise ValueError(
                        f"Inconsistent years for systematic '{base.name}': "
                        f"{base.years} vs {other.years}"
                    )
                for proc, val in other.processes.items():
                    if proc in base.processes and base.processes[proc] != val:
                        raise ValueError(
                            f"Conflicting values for process '{proc}' in "
                            f"systematic '{base.name}': "
                            f"{base.processes[proc]} vs {val}"
                        )
                    base.processes[proc] = val
                merged_systematics.append(base)
        return merged_systematics

    @property
    def variations_names(self) -> list[str]:
        """List of Names of Shape Variations."""
        return [
            f"{syst}{shift}"
            for syst in [
                syst.datacard_name
                for name, syst in self.get_systematics_by_type("shape").items()
            ]
            for shift in ("Up", "Down")
        ]

    def n_systematics(self) -> int:
        """Number of Systematics"""
        return len(self.keys())

    def list_type(self, syst_type: str) -> list[str]:
        """List of Names of Systematics of a specific type."""
        return [key for key in self if self[key].typ == syst_type]

    def get_systematics_by_type(self, syst_type: str) -> dict[SystematicUncertainty]:
        """Dict of Systematics of a specific type by datacard_name."""
        return {name: syst for name, syst in self.items() if syst.typ == syst_type}

    def get_systematics_by_process(
        self, process: Process
    ) -> list[SystematicUncertainty]:
        """List of Systematics that affect a specific process."""
        return {
            name: syst for name, syst in self.items() if process.name in syst.processes
        }
