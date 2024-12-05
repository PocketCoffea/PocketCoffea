"""Datacard Class and Utilities for CMS Combine Tool"""

import os
from functools import cached_property

import hist
import uproot

from pocket_coffea.utils.processes import Process, Processes
from pocket_coffea.utils.systematics import Systematics, SystematicUncertainty


def rearrange_histograms(
    histograms: dict,
    datasets_metadata: dict,
    processes: list[Process],
    shape_systematics: list[str],
    year: str,
    category: str,
) -> hist.Hist:
    """Rearrange histograms from pocket_coffea output format to match processes
    and systematics in one histogram.

    :param histograms: output of pocket_coffea processor
    :type histograms: dict
    :param processes: list of processes to consider
    :type processes: list[Process]
    :param shape_systematics: names of systematics to consider (need to include Up/Down)
    :type shape_systematics: list[str]
    :param year: year to consider
    :type year: str
    :param category: category to consider
    :type category: str
    :return: single histogram with processes and systematics
    :rtype: hist.Hist
    """
    sample = processes[0].samples[0]

    variable_axis = histograms[sample][f"{sample.split('__')[0]}_{year}"].axes[-1]

    processes_names = [process.name for process in processes]
    new_histogram = hist.Hist(
        hist.axis.StrCategory(processes_names, name="process"),
        hist.axis.StrCategory(shape_systematics, name="systematics"),
        variable_axis,
        storage=hist.storage.Weight(),
    )

    new_histogram_view = new_histogram.view()
    for process in processes:
        process_index = new_histogram.axes["process"].index(process.name)
        for sample in process.samples:
            datasets = datasets_metadata["by_datataking_period"][year][sample]
            for dataset in datasets:
                histogram = histograms[sample][dataset]
                for systematic in new_histogram.axes["systematics"]:
                    systematic_index = new_histogram.axes["systematics"].index(systematic)
                    new_histogram_view[process_index, systematic_index, :] += histogram[
                        category, systematic, :
                    ].view()
    return new_histogram


def create_shape_histogram_dict(
    histogram: hist.Hist, processes: list[Process], shape_systematics: list[str]
) -> dict[str, hist.Hist]:
    """Create a dictionary of histograms for each process and systematic.

    :param processes: List of processes
    :type processes: list[Process]
    :param shape_systematics: List of shape systematics (need to include Up/Down)
    :type shape_systematics: list[str]
    :param histogram: single histogram as returned by rearrange_histograms
    :type histogram: hist.Hist
    :return: dictionary of histograms, keys are process_systematic
    :rtype: dict[str, hist.Hist]
    """
    new_histograms = dict()
    for process in processes:
        for systematic in shape_systematics:
            # create new 1d histogram
            new_histogram = hist.Hist(
                histogram.axes[-1],
                storage=hist.storage.Weight(),
            )
            new_histogram_view = new_histogram.view()

            # add samples that correspond to a process
            new_histogram_view[:] = histogram[process.name, systematic, :].view()
            new_histograms[f"{process.name}_{systematic}"] = new_histogram

    return new_histograms


class Datacard(Processes, Systematics):
    """Datacard containing processes, systematics and write utilities."""

    def __init__(
        self,
        histograms: dict[str, dict[str, hist.Hist]],
        datasets_metadata: dict[str, dict[str, dict]],
        processes: list[Process],
        systematics: list[SystematicUncertainty],
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
        self.datasets_metadata = datasets_metadata
        self.processes = processes
        self.systematics = systematics
        self.year = year
        self.category = category
        self.bin_prefix = bin_prefix
        self.number_width = 10

        # assign IDs to processes
        id_signal = 0  # signal processes have 0 or negative IDs
        id_background = 1  # background processes have positive IDs
        for process in self.processes:
            if process.is_signal:
                process.id = id_signal
                id_signal -= 1
            else:
                process.id = id_background
                id_background += 1
        self.processes = sorted(self.processes, key=lambda proc: proc.id)

        # check histograms and rearrange them
        self._check_histograms()
        self.histogram = rearrange_histograms(
            histograms=self.histograms,
            datasets_metadata=self.datasets_metadata,
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
            year=self.year,
            category=self.category,
        )

        # helper attributes
        self.linesep = "\n"
        self.sectionsep = "-" * 80

    @property
    def shape_systematics_names(self):
        return ["nominal"] + [
            f"{syst}{shift}"
            for syst in self.list_type("shape")
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
        return len(self.background_processes)

    @property
    def kmax(self):
        """Number of nuisance parameters in the datacard"""
        return self.n_systematics

    @cached_property
    def adjust_first_column(self):
        return (
            max(
                [len("process")]
                + [
                    len(f"{systematic.name} {systematic.typ}")
                    for systematic in self.systematics
                ],
            )
            + 4
        )

    @cached_property
    def adjust_syst_colum(self):
        return max(len(systematic) for systematic in self.systematics_names)

    @cached_property
    def adjust_columns(self):
        return (
            max(
                *[len(process) for process in self.processes_names],
                len(self.bin),
            )
            + 4
        )

    def _check_histograms(self) -> None:
        """Check if histograms are available for all processes and systematics."""
        for process in self.processes:
            for sample in process.samples:
                if sample not in self.histograms:
                    raise ValueError(f"Missing histogram for sample {sample}")

                datasets = self.datasets_metadata["by_datataking_period"][self.year][sample]
                for dataset in datasets:
                    if dataset not in self.histograms[sample]:
                        raise ValueError(
                            f"Sample {sample} for year {self.year} ({dataset})"
                            f"not found in histograms {self.histograms[sample].keys()}"
                        )
                    missing_systematics = set(self.shape_systematics_names) - set(
                        self.histograms[sample][dataset].axes["variation"]
                    )
                    if missing_systematics:
                        raise ValueError(
                            f"Sample {sample} for year {self.year} is missing the following"
                            f"systematics: {missing_systematics}"
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
            f"{process.name}".ljust(self.adjust_columns) for process in self.processes
        )
        content += self.linesep
        # process ids
        content += "process".ljust(self.adjust_first_column)
        content += "".join(
            f"{process.id}".ljust(self.adjust_columns) for process in self.processes
        )
        content += self.linesep

        # rates
        content += "rate".ljust(self.adjust_first_column)
        content += "".join(
            f"{self.rate(process)}"[:self.number_width].ljust(self.adjust_columns)
            for process in self.processes_names
        )
        content += self.linesep
        return content

    def systematics_section(self) -> str:
        content = ""
        for systematic in self.systematics:
            line = systematic.name.ljust(self.adjust_syst_colum)
            line += f" {systematic.typ}"
            line = line.ljust(self.adjust_first_column)

            # processes
            for process in self.processes:
                if process.name in systematic.processes:
                    value = systematic.processes[process.name]
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
        shapes_filename: str = "shapes.root",
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
        shapes_file = os.path.join(directory, shapes_filename)

        os.makedirs(directory, exist_ok=True)

        with open(card_file, "w") as card:
            card.write(self.content(shapes_filename=shapes_filename))

        shape_histograms = create_shape_histogram_dict(
            histogram=self.histogram,
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
        )
        with uproot.recreate(shapes_file) as root_file:
            for shape, histogram in shape_histograms.items():
                root_file[shape] = histogram
