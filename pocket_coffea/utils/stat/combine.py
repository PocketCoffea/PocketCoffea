"""Datacard Class and Utilities for CMS Combine Tool"""

import os
from functools import cached_property

import hist
import uproot

from pocket_coffea.utils.stat.processes import Processes
from pocket_coffea.utils.stat.systematics import Systematics
from pocket_coffea.utils.stat.utils import (
    create_shape_histogram_dict,
    rearrange_histograms,
)


class Datacard:
    """Datacard containing processes, systematics and write utilities."""

    def __init__(
        self,
        histograms: dict[str, dict[str, hist.Hist]],
        processes: Processes,
        systematics: Systematics,
        subsamples_reversed_map: dict[str, str],
        year: str,
        category: str,
        bin_prefix: str = None,
    ) -> None:
        """Initialize the Datacard.

        :param histograms: Dict with histograms for each sample
        :type histograms: dict
        :param processes: processes
        :type processes: Processes
        :param systematics: systematic uncertainties
        :type systematics: Systematics
        :param year: year of data taking
        :type year: str
        :param category: Category in datacard
        :type category: str
        :param bin_prefix: prefix for the bin name, defaults to None
        :type bin_prefix: str, optional
        """

        self.histograms = histograms
        self.processes = processes
        self.systematics = systematics
        self.subsamples_reversed_map = subsamples_reversed_map
        self.year = year
        self.category = category
        self.bin_prefix = bin_prefix

        # assign IDs to processes
        id_signal = 0  # signal processes have 0 or negative IDs
        id_background = 1  # background processes have positive IDs
        for process in self.processes.values():
            if process.is_signal:
                process.id = id_signal
                id_signal -= 1
            else:
                process.id = id_background
                id_background += 1
        self.processes = Processes(
            sorted(self.processes.values(), key=lambda proc: proc.id)
        )

        # check histograms and rearrange them
        self._check_histograms()
        self.histogram = rearrange_histograms(
            histograms=self.histograms,
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
            year=self.year,
            category=self.category,
            subsamples_reversed_map=self.subsamples_reversed_map,
        )

        # helper attributes
        self.linesep = "\n"
        self.sectionsep = "-" * 80

    @property
    def shape_systematics_names(self):
        return ["nominal"] + [
            f"{syst}{shift}"
            for syst in self.systematics.list_type("shape")
            for shift in ("Up", "Down")
        ]

    @property
    def bin(self) -> str:
        """Name of the bin in the datacard"""
        if self.bin_prefix:
            return f"{self.bin_prefix}_{self.category}_{self.year}"
        return f"{self.category}_{self.year}"

    @property
    def observation(self):
        """Number of observed events in the datacard"""
        # TODO: implement observation
        return -1

    def rate(self, process: str, systematic="nominal") -> float:
        """Rate of a process in the datacard"""
        # TODO: fix histograms (e.g. negative bins)!

        return self.histogram[process, systematic, :].sum()["value"]

    @property
    def imax(self):
        """Number of bins in the datacard"""
        return 1

    @property
    def jmax(self):
        """Number of background processes"""
        return len(self.processes.background_processes)

    @property
    def kmax(self):
        """Number of nuisance parameters in the datacard"""
        return len(self.systematics)

    @cached_property
    def adjust_first_column(self):
        return (
            max(
                [len("process")]
                + [
                    len(f"{systematic.name} {systematic.typ}")
                    for systematic in self.systematics.values()
                ],
            )
            + 4
        )

    @cached_property
    def adjust_syst_colum(self):
        return max(len(systematic) for systematic in self.systematics)

    @cached_property
    def adjust_columns(self):
        return (
            max(
                *[len(process) for process in self.processes],
                len(self.bin),
            )
            + 4
        )

    def _check_histograms(self) -> None:
        """Check if histograms are available for all processes and systematics."""
        systs_are_missing = False
        missing_systematics = {}
        for process in self.processes.values():
            for sample in process.samples:
                if sample not in self.histograms:
                    raise ValueError(f"Missing histogram for sample {sample}")

                missing_systematics[sample] = {}
                for dataset in self.histograms[sample]:
                    missing_set = set(self.shape_systematics_names) - set(
                        self.histograms[sample][dataset].axes["variation"]
                    )
                    if missing_set:
                        systs_are_missing = True
                        missing_systematics[sample][dataset] = missing_set
        if systs_are_missing:
            raise ValueError(
                f"The following systematics are missing: {missing_systematics}"
            )

    def preamble(self) -> str:
        preamble = f"imax {self.imax} number of channels{self.linesep}"
        preamble += f"jmax {self.jmax} number of background processes{self.linesep}"
        preamble += f"kmax {self.kmax} number of nuisance parameters{self.linesep}"
        return preamble

    def shape_section(self, shapes_name: str) -> str:
        """shapes process channel file histogram [histogram_with_systematics]"""
        return f"shapes * {self.bin} {shapes_name} $PROCESS_nominal $PROCESS_$SYSTEMATIC{self.linesep}"  # noqa

    def observation_section(self) -> str:
        content = f"bin {self.bin}{self.linesep}"
        content += f"observation {self.observation}{self.linesep}"
        return content

    def expectation_section(self) -> str:
        content = "bin".ljust(self.adjust_first_column)
        content += f"{self.bin}".ljust(self.adjust_columns) * len(self.processes)
        content += self.linesep

        content += "process".ljust(self.adjust_first_column)
        # process names
        content += "".join(
            process.ljust(self.adjust_columns) for process in self.processes
        )
        content += self.linesep
        # process ids
        content += "process".ljust(self.adjust_first_column)
        content += "".join(
            f"{process.id}".ljust(self.adjust_columns)
            for process in self.processes.values()
        )
        content += self.linesep

        # rates
        content += "rate".ljust(self.adjust_first_column)
        content += "".join(
            f"{self.rate(process)}".ljust(self.adjust_columns)
            for process in self.processes
        )
        content += self.linesep
        return content

    def systematics_section(self) -> str:
        content = ""
        for name, systematic in self.systematics.items():
            line = name.ljust(self.adjust_syst_colum)
            line += f" {systematic.typ}"
            line = line.ljust(self.adjust_first_column)

            # processes
            for process in self.processes:
                if process in systematic.processes:
                    value = systematic.processes[process]
                    if isinstance(value, tuple):
                        line += f"{value[0]}/{value[1]}".ljust(self.adjust_columns)
                    else:
                        line += f"{value}".ljust(self.adjust_columns)
                else:
                    line += " -".ljust(self.adjust_columns)

            line += self.linesep
            content += line
        return content

    def content(self, shapes_filename: str) -> str:
        content = self.preamble()
        content += self.sectionsep + self.linesep

        content += self.shape_section(shapes_name=shapes_filename)
        content += self.sectionsep + self.linesep

        content += self.observation_section()
        content += self.sectionsep + self.linesep

        content += self.expectation_section()
        content += self.sectionsep + self.linesep

        content += self.systematics_section()
        content += self.sectionsep + self.linesep

        return content

    def dump(
        self,
        directory: os.PathLike,
        card_name: str = "datacard.txt",
        shapes_name: str = "shapes.root",
    ) -> None:
        """Dump datacard and shapes to a directory.

        :param directory: Directory to dump the datacard and shapes
        :type directory: os.PathLike
        :param card_name: name of the datacard file, defaults to "datacard.txt"
        :type card_name: str, optional
        :param shapes_filename: name of the shapes file, defaults to "shapes.root"
        :type shapes_filename: str, optional
        """

        card_file = os.path.join(directory, card_name)
        shapes_file = os.path.join(directory, shapes_name)

        os.makedirs(directory, exist_ok=True)

        with open(card_file, "w") as card:
            card.write(self.content(shapes_filename=shapes_name))

        shape_histograms = create_shape_histogram_dict(
            histogram=self.histogram,
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
        )
        with uproot.recreate(shapes_file) as root_file:
            for shape, histogram in shape_histograms.items():
                root_file[shape] = histogram
