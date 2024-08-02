"""Classes and functions for creating datacards for Higgs Combine Tool."""

import os
from dataclasses import dataclass
from functools import cached_property

import hist
import uproot
from coffea.util import load


@dataclass
class SystematicUncertainty:
    """Class to store information of a systematic uncertainty"""

    name: str
    type: str
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
class Process:
    """Class to store information of a physical process"""

    name: str
    samples: list
    is_signal: bool
    label: str = None

    def __post_init__(self):
        if not self.label:
            self.label = self.name


@dataclass
class Processes:
    """Class to store information of a list of processes"""

    processes: list[Process]

    @property
    def processes_names(self) -> list[str]:
        return [process.name for process in self.processes]

    @property
    def signal_processes(self) -> list[str]:
        return [process.name for process in self.processes if process.is_signal]

    @property
    def background_processes(self) -> list[str]:
        return [process.name for process in self.processes if not process.is_signal]

    @property
    def n_processes(self) -> int:
        return len(self.processes)


@dataclass
class Systematics:
    """Class to store information of a list of systematic uncertainties"""

    systematics: list[SystematicUncertainty]

    @property
    def systematics_names(self) -> list[str]:
        return [systematics.name for systematics in self.systematics]

    @property
    def n_systematics(self) -> int:
        return len(self.systematics)

    def list_type(self, syst_type: str) -> list[str]:
        return [
            systematics.name
            for systematics in self.systematics
            if systematics.type == syst_type
        ]

    def get_type(self, syst_type: str) -> list[SystematicUncertainty]:
        return [
            systematics
            for systematics in self.systematics
            if systematics.type == syst_type
        ]


@dataclass
class StatModel(Processes, Systematics):
    """Class to setup a statistical model"""

    scale_lumi: float | int = None

    def __post_init__(self):
        """ "Assign IDs to the processes.
        needed in datacard
        """
        id_signal = 0  # signal have 0 or negative ids in datacard
        id_background = 1  # background have positive ids in datacard
        for process in self.processes:
            if process.is_signal:
                process.id = id_signal
                id_signal -= 1
            else:
                process.id = id_background
                id_background += 1
        # sort by ID
        self.processes = sorted(self.processes, key=lambda proc: proc.id)


def rearrange_histograms(
    histograms: dict,
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
            subsample = f"{sample.split('__')[0]}_{year}"
            histogram = histograms[sample][subsample]
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


def load_histograms(
    coffea_file: os.PathLike,
    variable: str,
    processes: list[Process],
    shape_systematics: list,
    year: str,
    category: str,
) -> dict:
    """Load histograms from pocket_coffea output file and rearrange them.
    histogram axis order: process, systematic, variable

    :param coffea_file: path to the coffea output file
    :type coffea_file: os.PathLike
    :param variable: name of the variable to consider
    :type variable: str
    :param processes: processes to consider
    :type processes: list[Process]
    :param shape_systematics: shape systematics to consider
    :type shape_systematics: list
    :param year: year to consider
    :type year: str
    :param category: category to consider
    :type category: str
    :return: dict of histograms, keys are process_systematic
    :rtype: dict
    """
    coffea_output = load(coffea_file)
    histograms = coffea_output["variables"][variable]

    histograms = rearrange_histograms(
        histograms, processes, shape_systematics, year, category
    )

    return create_shape_histogram_dict(histograms, processes, shape_systematics)


class Datacard:
    """https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/part2/settinguptheanalysis/"""

    def __init__(
        self,
        histograms: dict,
        statmodel: StatModel,
        year: str,
        category: str,
        bin_prefix: str = None,
    ) -> None:
        self.histograms = histograms
        self.statmodel = statmodel
        self.year = year
        self.category = category
        self.bin_prefix = bin_prefix

        self.linesep = "\n"
        self.sectionsep = "-" * 80

        self.check_histograms()
        self.histogram = self.rearrange_histograms()

    @property
    def variable_axis(self):
        sample = self.processes[0].samples[0]
        subsample = sample.split("__")[0]
        return self.histograms[sample][f"{subsample}_{self.year}"].axes[-1]

    def check_histograms(self):
        """Check if the histograms contain all processes and systematics"""
        for process in self.processes:
            for sample in process.samples:
                if sample not in self.histograms:
                    raise ValueError(
                        f"Sample {sample} specified in Statmodel "
                        "but not found in histograms"
                    )
                subsample = f"{sample.split('__')[0]}_{self.year}"
                if subsample not in self.histograms[sample]:
                    raise ValueError(
                        f"Sample {sample} for year {self.year} ({subsample})"
                        f"not found in histograms {self.histograms[sample].keys()}"
                    )
                missing_systematics = set(self.shape_systematics_names) - set(
                    self.histograms[sample][subsample].axes["variation"]
                )
                if missing_systematics:
                    raise ValueError(
                        f"Sample {sample} for year {self.year} is missing the following"
                        f"systematics: {missing_systematics}"
                    )

    def rearrange_histograms(self):
        """Rearrange the histograms in the desired format"""
        return rearrange_histograms(
            histograms=self.histograms,
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
            year=self.year,
            category=self.category,
        )

    @cached_property
    def adjust_first_column(self):
        return (
            max(
                [len("process")]
                + [
                    len(f"{systematic.name} {systematic.type}")
                    for systematic in self.statmodel.systematics
                ],
            )
            + 4
        )

    @cached_property
    def adjust_syst_colum(self):
        return max(len(systematic) for systematic in self.statmodel.systematics_names)

    @cached_property
    def adjust_columns(self):
        return (
            max(
                *[len(process) for process in self.processes_names],
                len(self.bin),
            )
            + 4
        )

    @property
    def available_processes(self):
        return list(self.histograms.keys())

    @property
    def processes(self):
        return self.statmodel.processes

    @property
    def processes_names(self):
        return self.statmodel.processes_names

    @property
    def shape_systematics_names(self):
        return ["nominal"] + [
            f"{syst}{shift}"
            for syst in self.statmodel.list_type("shape")
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
        return len(self.statmodel.background_processes)

    @property
    def kmax(self):
        """Number of nuisance parameters in the datacard"""
        return self.statmodel.n_systematics

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
            f"{self.rate(process)}".ljust(self.adjust_columns)
            for process in self.processes_names
        )
        content += self.linesep
        return content

    def systematics_section(self) -> str:
        content = ""
        for systematic in self.statmodel.systematics:
            line = systematic.name.ljust(self.adjust_syst_colum)
            line += f" {systematic.type}"
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

    def content(self, shapes_name: str) -> str:
        content = self.preamble()
        content += self.sectionsep + self.linesep

        content += self.shape_section(shapes_name=shapes_name)
        content += self.sectionsep + self.linesep

        content += self.observation_section()
        content += self.sectionsep + self.linesep

        content += self.expectation_section()
        content += self.sectionsep + self.linesep

        content += self.systematics_section()
        content += self.sectionsep + self.linesep

        return content

    def build(self) -> dict:
        """Build the shape uncertainties"""
        return create_shape_histogram_dict(
            processes=self.processes,
            shape_systematics=self.shape_systematics_names,
            histogram=self.histogram,
        )

    def dump(
        self,
        directory: os.PathLike,
        card_name: str = "datacard.txt",
        shapes_name: str = "shapes.root",
    ):
        card_file = os.path.join(directory, card_name)
        shapes_file = os.path.join(directory, shapes_name)

        with open(card_file, "w") as card:
            card.write(self.content(shapes_name=shapes_name))

        with uproot.recreate(shapes_file) as root_file:
            for shape, histogram in self.build().items():
                root_file[shape] = histogram

    def __repr__(self):
        return f"Datacard(statmodel={self.statmodel!r}, card=CONTENT"


def load_datacard(histograms: dict, statmodel: StatModel, year: str, category: str):
    """Load a datacard from histograms and a statmodel.

    :param histograms: dictionary of histograms corresponding to processes in statmodel
    :type histograms: dict
    :param statmodel: statistical model
    :type statmodel: StatModel
    :param year: year to consider (read from histograms)
    :type year: str
    :param category: category to consider (read from histograms)
    :type category: str
    :return: Datacard object
    :rtype: _type_
    """
    return Datacard(
        histograms=histograms, statmodel=statmodel, year=year, category=category
    )
