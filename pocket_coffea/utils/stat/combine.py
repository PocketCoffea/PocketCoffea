"""Datacard Class and Utilities for CMS Combine Tool"""

import os
from functools import cached_property

import hist
import numpy as np
import uproot

from pocket_coffea.utils.histogram import rebin_hist
from pocket_coffea.utils.stat.processes import DataProcesses, MCProcesses
from pocket_coffea.utils.stat.systematics import Systematics

# https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/part2/bin-wise-stats/
AUTO_MC_STATS = {
    "threshold": 0,
    "include_signal": 0,
    "hist_mode": 1,
}


class Datacard:
    """Datacard containing processes, systematics and write utilities.

    :param histograms: Dict with histograms for each sample
    :type histograms: dict[str, dict[str, hist.Hist]]
    :param datasets_metadata: Metadata for datasets
    :type datasets_metadata: dict[str, dict[str, dict]]
    :param cutflow: Cutflow information for datasets
    :type cutflow: dict[str, dict[str, float]]
    :param years: Years of data taking
    :type years: list[str]
    :param mc_processes: mc_processes
    :type mc_processes: MCProcesses
    :param systematics: systematic uncertainties
    :type systematics: Systematics
    :param category: Category in datacard
    :type category: str
    :param data_processes: Data processes, defaults to None
    :type data_processes: DataProcesses, optional
    :param mcstat: Whether to include MC statistics,
        you can also pass a dict with the options accepted by combine,
        defaults to True
    :type mcstat: bool | dict, optional
    :param bins_edges: Bin edges for rebinning histograms, defaults to None
    :type bins_edges: list[float], optional
    :param bin_prefix: prefix for the bin name, defaults to None
    :type bin_prefix: str, optional
    :param bin_suffix: suffix for the bin name, defaults to None
    :type bin_suffix: str, optional
    """

    def __init__(
        self,
        histograms: dict[str, dict[str, hist.Hist]],
        datasets_metadata: dict[str, dict[str, dict]],
        cutflow: dict[str, dict[str, float]],
        years: list[str],
        mc_processes: MCProcesses,
        systematics: Systematics,
        category: str,
        data_processes: DataProcesses = None,
        mcstat: bool | dict = True,
        bins_edges: list[float] = None,
        bin_prefix: str = None,
        bin_suffix: str = None,
    ) -> None:
        """Initialize the Datacard."""

        self.histograms = histograms
        self.datasets_metadata = datasets_metadata
        self.cutflow = cutflow
        self.mc_processes = mc_processes
        self.data_processes = data_processes
        self.systematics = systematics
        self.mcstat = mcstat
        self.bins_edges = bins_edges
        self.years = years
        self.category = category
        self.bin_prefix = bin_prefix
        self.bin_suffix = bin_suffix
        if self.bin_suffix is None:
            self.bin_suffix = "_".join(self.years)
        self.number_width = 10
        self.has_data = data_processes is not None

        # handle automatic MC statistics
        self._parse_mcstat_argument()

        if self.has_data and (len(self.data_processes) != 1):
            raise NotImplementedError("Only one data process is supported.")
        # If bin edges are passed, rebin histograms
        if self.bins_edges is not None:
            self.histograms = rebin_hist(
                bins_edges=self.bins_edges, histograms=self.histograms
            )

        # assign IDs to processes
        self.process_id = {}
        id_signal = 0  # signal processes have 0 or negative IDs
        id_background = 1  # background processes have positive IDs
        for process in self.mc_processes.values():
            for year in process.years:
                if process.is_signal:
                    self.process_id[f"{process.name}_{year}"] = id_signal
                    id_signal -= 1
                else:
                    self.process_id[f"{process.name}_{year}"] = id_background
                    id_background += 1
        # self.processes = sorted(self.processes, key=lambda proc: proc.id)

        # check histograms and rearrange them
        self._check_histograms()
        self.histogram = self.rearrange_histograms(is_data=False)

        if self.has_data:
            self.data_obs = self.rearrange_histograms(is_data=True)

        self._check_shapes()

        # helper attributes
        self.linesep = "\n"
        self.sectionsep = "-" * 80

    def _parse_mcstat_argument(self) -> None:
        """Parse the mcstat argument.

        Raises
        ------
        ValueError
            If there are unknown keys in the mcstat dict.
        TypeError
            If mcstat is not a bool or a dict.
        """
        if isinstance(self.mcstat, dict):
            # set values from the dict or use defaults
            self.threshold = self.mcstat.pop("threshold", AUTO_MC_STATS["threshold"])
            self.include_signal = self.mcstat.pop(
                "include_signal", AUTO_MC_STATS["include_signal"]
            )
            self.hist_mode = self.mcstat.pop("hist_mode", AUTO_MC_STATS["hist_mode"])

            # check if there are any unknown keys
            if self.mcstat:
                raise ValueError(
                    f"Unknown keys in mcstat: {', '.join(self.mcstat.keys())}. "
                    "Allowed keys are: " + ", ".join(AUTO_MC_STATS.keys())
                )

            # set self.mcstat to True for further use
            self.mcstat = True
        elif isinstance(self.mcstat, bool) and self.mcstat:
            self.threshold = AUTO_MC_STATS["threshold"]
            self.include_signal = AUTO_MC_STATS["include_signal"]
            self.hist_mode = AUTO_MC_STATS["hist_mode"]
        else:
            raise TypeError(
                "mcstat must be a bool or a dict with at least one of the following keys: "
                + ", ".join(AUTO_MC_STATS.keys())
            )

    @property
    def mcstat_config(self) -> dict:
        """Return the configuration for MC statistics."""
        if not self.mcstat:
            return {}
        return {
            "threshold": self.threshold,
            "include_signal": self.include_signal,
            "hist_mode": self.hist_mode,
        }

    @property
    def shape_variations(self) -> list[str]:
        return ["nominal"] + self.systematics.variations_names

    @property
    def bin(self) -> str:
        """Name of the bin in the datacard"""
        bin_name = self.category
        if self.bin_prefix:
            bin_name = f"{self.bin_prefix}_{bin_name}"
        if self.bin_suffix:
            bin_name = f"{bin_name}_{self.bin_suffix}"
        return bin_name

    @property
    def observation(self):
        """Number of observed events in the datacard"""
        if self.has_data:
            return self.data_obs.sum()["value"]
        else:
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
        """Number of background processes + number of signal processes - 1"""
        return (
            len(self.mc_processes.background_processes)
            + len(self.mc_processes.signal_processes)
            - 1
        )

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
                    len(f"{systematic.datacard_name} {systematic.typ}")
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
                *[len(process) for process in self.mc_processes],
                len(self.bin),
            )
            + 4
        )

    def is_empty_dataset(self, dataset: str) -> bool:
        """Check if dataset is empty"""
        return self.cutflow["presel"][dataset] == 0

    def get_datasets_by_sample(self, sample: str, year: str = None) -> list[str]:
        """
        Retrieve the list of dataset names for a given sample and optionally a specific year.

        :param sample: The sample name for which to retrieve datasets.
        :type sample: str
        :param year: The year (data-taking period) to filter datasets.
            If None (default), datasets from all years in self.years are returned.
        :type year: str, optional, default=None

        :return: List of dataset names corresponding to the sample (and year, if specified).
        :rtype: list[str]
        """
        if year is None:
            years = [
                year
                for year in self.datasets_metadata["by_datataking_period"].keys()
                if year in self.years
            ]
            return [
                d
                for year in years
                for d in self.datasets_metadata["by_datataking_period"][year][sample]
            ]
        else:
            return self.datasets_metadata["by_datataking_period"][year][sample]

    def _check_histograms(self) -> None:
        """Check if histograms are available for all processes and systematics."""
        available_variations = []
        for systematic in self.systematics.get_systematics_by_type("shape"):
            for shift in ("Up", "Down"):
                available_variations.append(f"{systematic}{shift}")

        for process in self.mc_processes.values():
            for sample in process.samples:
                if sample not in self.histograms:
                    raise ValueError(f"Missing histogram for sample {sample}")

                for year in process.years:
                    for dataset in self.get_datasets_by_sample(sample, year):
                        if dataset not in self.histograms[sample]:
                            if self.is_empty_dataset(dataset):
                                print(
                                    f"Sample {sample} for dataset {dataset} has 0 events in category `presel`. "
                                    f"Skipping this dataset."
                                )
                                continue
                            else:
                                raise ValueError(
                                    f"Sample {sample} for dataset {dataset} "
                                    f"not found in histograms {self.histograms[sample].keys()}"
                                )
                        if not process.is_data:
                            missing_systematics = set(available_variations) - set(
                                self.histograms[sample][dataset].axes["variation"]
                            )
                            if missing_systematics:
                                print(
                                    f"Sample {sample} for dataset {dataset} is missing the following "
                                    f"systematics: {missing_systematics}"
                                )

    def _check_shapes(self, threshold: float = 1.0, raise_error: bool = False) -> None:
        """Sanity checks for saved shapes.
        If each variation differs by more than 100% from the nominal, an error is raised."""
        # Loop over variations in self.histogram as saved after rearranging
        for process in self.mc_processes.values():
            for year in process.years:
                process_name_byyear = f"{process.name}_{year}"
                process_index = self.histogram.axes["process"].index(
                    process_name_byyear
                )
                variation_index_nominal = self.histogram.axes["variation"].index(
                    "nominal"
                )
                for variation in self.histogram.axes["variation"]:
                    if variation == "nominal":
                        continue
                    variation_index = self.histogram.axes["variation"].index(variation)
                    nominal = self.histogram[
                        process_index, variation_index_nominal, :
                    ].values()
                    var = self.histogram[process_index, variation_index, :].values()
                    diff_rel = np.where(nominal != 0, (var - nominal) / nominal, 0)
                    if any(diff_rel > threshold):
                        diff_avg = sum(diff_rel) / len(diff_rel)
                        error_message = (
                            f"Variation {variation} for process {process_name_byyear} "
                            f"differs by more than {threshold:.0%} from nominal in some bins of the {self.category} category. "
                            f"Average difference: {diff_avg:.0%}"
                        )
                        if raise_error:
                            raise ValueError(error_message)
                        else:
                            print(error_message)

    def rearrange_histograms(
        self,
        is_data: bool = False,
    ) -> hist.Hist:
        """Rearrange histograms from pocket_coffea output format to match processes
        and systematics in one histogram.

        :param is_data: Flag to indicate if the datacard is for data, defaults to False
        :type is_data: bool, optional
        :return: Rearranged histogram
        :rtype: hist.Hist
        """
        if is_data:
            processes = self.data_processes
        else:
            processes = self.mc_processes
        assert (
            (not is_data) & all(not process.is_data for process in processes.values())
        ) or (is_data & all(process.is_data for process in processes.values())), (
            "All processes must be either data or MC"
        )

        # Extract variable axis from the first dataset
        dataset = list(
            self.get_datasets_by_sample(list(processes.values())[0].samples[0])
        )[0]
        variable_axis = self.histograms[list(processes.values())[0].samples[0]][
            dataset
        ].axes[-1]

        if is_data:
            processes_names = processes.keys()
            new_histogram = hist.Hist(
                hist.axis.StrCategory(processes_names, name="process"),
                variable_axis,
                storage=hist.storage.Weight(),
            )
        else:
            processes_names = [
                f"{process_name}_{year}"
                for process_name, process in processes.items()
                for year in process.years
            ]
            new_histogram = hist.Hist(
                hist.axis.StrCategory(processes_names, name="process"),
                hist.axis.StrCategory(self.shape_variations, name="variation"),
                variable_axis,
                storage=hist.storage.Weight(),
            )
        new_histogram_view = new_histogram.view()

        for process in processes.values():
            for sample in process.samples:
                # data processes do not have attribute years
                years = process.years if not is_data else [None]
                for year in years:
                    if is_data:
                        assert year is None, "Data process should not have a year"
                        process_index = new_histogram.axes["process"].index(
                            process.name
                        )
                    else:
                        assert year is not None, "MC process should have a year"
                        process_index = new_histogram.axes["process"].index(
                            f"{process.name}_{year}"
                        )
                    for dataset in self.get_datasets_by_sample(sample, year):
                        if self.is_empty_dataset(dataset):
                            continue
                        histogram = self.histograms[sample][dataset]
                        if is_data:
                            new_histogram_view[process_index, :] += histogram[
                                self.category, :
                            ].view()
                        else:
                            # Save nominal variation
                            variation_index_nominal = new_histogram.axes[
                                "variation"
                            ].index("nominal")
                            new_histogram_view[
                                process_index, variation_index_nominal, :
                            ] += histogram[self.category, "nominal", :].view()
                            for (
                                syst_name,
                                systematic,
                            ) in self.systematics.get_systematics_by_type(
                                "shape"
                            ).items():
                                for shift in ("Up", "Down"):
                                    variation = f"{syst_name}{shift}"
                                    variation_index = new_histogram.axes[
                                        "variation"
                                    ].index(f"{systematic.datacard_name}{shift}")
                                    if variation in histogram.axes["variation"]:
                                        new_histogram_view[
                                            process_index, variation_index, :
                                        ] += histogram[
                                            self.category, variation, :
                                        ].view()
                                    else:
                                        print(
                                            f"Setting `{variation}` variation to nominal variation for sample {sample}."
                                        )
                                        new_histogram_view[
                                            process_index, variation_index, :
                                        ] += histogram[
                                            self.category, "nominal", :
                                        ].view()
        return new_histogram

    def create_shape_histogram_dict(
        self, is_data: bool = False
    ) -> dict[str, hist.Hist]:
        """Create a dictionary of histograms for each process and systematic.

        :param is_data: Flag to indicate if the datacard is for data, defaults to False
        :type is_data: bool, optional
        :return: dictionary of histograms, keys are process_systematic
        :rtype: dict[str, hist.Hist]
        """
        if is_data:
            histogram = self.data_obs
            processes = self.data_processes
        else:
            histogram = self.histogram
            processes = self.mc_processes
        new_histograms = dict()
        for process in processes.values():
            years = process.years if not is_data else [None]
            for year in years:
                if is_data:
                    process_name_byyear = process.name
                else:
                    process_name_byyear = f"{process.name}_{year}"
                if is_data:
                    # create new 1d histogram
                    new_histogram = hist.Hist(
                        histogram.axes[-1],
                        storage=hist.storage.Weight(),
                    )
                    new_histogram_view = new_histogram.view()
                    new_histogram_view[:] = histogram[process_name_byyear, :].view()
                    new_histograms[f"{process_name_byyear}_nominal"] = new_histogram
                else:
                    # Save nominal shape
                    new_histogram = hist.Hist(
                        histogram.axes[-1],
                        storage=hist.storage.Weight(),
                    )
                    new_histogram_view = new_histogram.view()
                    new_histogram_view[:] = histogram[
                        process_name_byyear, "nominal", :
                    ].view()
                    shape_name = f"{process_name_byyear}_nominal"
                    new_histograms[shape_name] = new_histogram
                    # Save shape variations
                    for systematic in self.systematics.get_systematics_by_type(
                        "shape"
                    ).values():
                        for shift in ("Up", "Down"):
                            variation = f"{systematic.datacard_name}{shift}"

                            # create new 1d histogram
                            new_histogram = hist.Hist(
                                histogram.axes[-1],
                                storage=hist.storage.Weight(),
                            )
                            new_histogram_view = new_histogram.view()

                            # add samples that correspond to a process
                            if (
                                process.name in systematic.processes
                                and year in systematic.years
                            ):
                                new_histogram_view[:] = histogram[
                                    process_name_byyear, variation, :
                                ].view()
                                shape_name = f"{process_name_byyear}_{systematic.datacard_name}{shift}"
                                new_histograms[shape_name] = new_histogram

        return new_histograms

    def preamble(self) -> str:
        preamble = f"imax {self.imax} number of channels{self.linesep}"
        preamble += f"jmax {self.jmax} number of processes minus 1{self.linesep}"
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
        content += (
            f"{self.bin}".ljust(self.adjust_columns) * self.mc_processes.n_processes
        )
        content += self.linesep

        content += "process".ljust(self.adjust_first_column)
        # process names
        content += "".join(
            f"{process.name}_{year}".ljust(self.adjust_columns)
            for process in self.mc_processes.values()
            for year in process.years
            if not process.is_data
        )
        content += self.linesep
        # process ids
        content += "process".ljust(self.adjust_first_column)
        content += "".join(
            f"{self.process_id[f'{process.name}_{year}']}".ljust(self.adjust_columns)
            for process in self.mc_processes.values()
            for year in process.years
            if not process.is_data
        )
        content += self.linesep

        # rates
        content += "rate".ljust(self.adjust_first_column)
        content += "".join(
            f"{self.rate(process)}"[: self.number_width].ljust(self.adjust_columns)
            for process in self.mc_processes.signal_processes
            + self.mc_processes.background_processes
        )
        content += self.linesep
        return content

    def systematics_section(self) -> str:
        content = ""
        for systematic in self.systematics.values():
            line = systematic.datacard_name.ljust(self.adjust_syst_colum)
            line += f" {systematic.typ}"
            line = line.ljust(self.adjust_first_column)

            # processes
            for process in self.mc_processes.values():
                for year in process.years:
                    if (
                        process.name in systematic.processes
                        and year in systematic.years
                    ):
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

    def rate_parameters_section(self) -> str:
        content = ""
        for process in self.mc_processes.values():
            for year in process.years:
                if not process.is_signal and process.has_rateParam:
                    line = f"SF_{process.name}".ljust(self.adjust_syst_colum)
                    line += "rateParam".ljust(self.adjust_columns)
                    line += f"* {process.name}_{year} 1 [0,5]".ljust(
                        self.adjust_columns
                    )
                    line += self.linesep
                    content += line
        return content

    def mcstat_section(self) -> str:
        content = ""
        content += f"{self.bin} autoMCStats {self.threshold} {self.include_signal} {self.hist_mode}"
        content += self.linesep
        return content

    def content(self, shapes_filename: str) -> str:
        """
        Generate the content of the datacard.

        :param shapes_filename: The filename of the root file containing the shape histograms.
        :type shapes_filename: str

        :returns: Content of the datacard as a string.
        :rtype: str
        """
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

        content += self.rate_parameters_section()
        content += self.sectionsep + self.linesep

        if self.mcstat:
            content += self.mcstat_section()
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

        shape_histograms = self.create_shape_histogram_dict(is_data=False)
        if self.has_data:
            shape_histograms_data = self.create_shape_histogram_dict(is_data=True)
        with uproot.recreate(shapes_file) as root_file:
            if self.has_data:
                for shape, histogram in shape_histograms_data.items():
                    root_file[shape] = histogram
            for shape, histogram in shape_histograms.items():
                root_file[shape] = histogram

    def __repr__(self) -> str:
        """Return a string representation of the Datacard."""

        s = "Datacard("
        s += f"histograms={repr(self.histograms)}, "
        s += f"datasets_metadata={repr(self.datasets_metadata)}, "
        s += f"cutflow={repr(self.cutflow)}, "
        s += f"years={repr(self.years)}, "
        s += f"mc_processes={repr(self.mc_processes)}, "
        s += f"systematics={repr(self.systematics)}, "
        s += f"category={repr(self.category)}, "
        s += f"data_processes={repr(self.data_processes)}, "

        if self.mcstat:
            s += f"mcstat={repr(self.mcstat_config)}, "
        else:
            s += "mcstat=False, "

        s += f"bins_edges={repr(self.bins_edges)}, "
        s += f"bin_prefix={repr(self.bin_prefix)}, "
        s += f"bin_suffix={repr(self.bin_suffix)}, "

        s += ")"

        return s

    def __str__(self) -> str:
        """Return a string representation of the Datacard."""
        process_names = (
            list(self.mc_processes.keys()) + list(self.data_processes.keys())
            if self.has_data
            else []
        )
        syst_names = list(self.systematics.keys())

        indent = " " * 2

        # Create simplified representation for histograms
        def simplify_hist_dict(hist_dict):
            """Convert histogram dict to simplified representation"""
            simplified = {}
            for sample, datasets in hist_dict.items():
                simplified[sample] = {}
                for dataset, histogram in datasets.items():
                    # Get the name of the last regular axis (variable axis)
                    axis_name = histogram.axes[-1].name
                    # Calculate total sum of all values
                    weighted_sum_flow = histogram.sum(flow=True)
                    weighted_sum = histogram.sum(flow=False)
                    weighted_sum_str = f"{weighted_sum} ({weighted_sum_flow} with flow)"
                    simplified[sample][dataset] = (
                        f"Hist(..., name={axis_name}, sum={weighted_sum_str})"
                    )
            return simplified

        histogram_str = simplify_hist_dict(self.histograms)

        return (
            "Datacard(\n"
            f"{indent}histograms={histogram_str},\n"
            f"{indent}category='{self.category}',\n"
            f"{indent}years={self.years},\n"
            f"{indent}processes={process_names},\n"
            f"{indent}systematics={syst_names[:3]}{'...' if len(syst_names) > 3 else ''},\n"
            f"{indent}has_data={self.has_data}\n"
            f"{indent}mcstat={self.mcstat_config if self.mcstat else 'mcstat=False'}\n"
            ")"
        )


def combine_datacards(
    datacards: dict[Datacard],
    directory: str,
    path: str = "combine_cards.sh",
    card_name: str = "datacard_combined.txt",
    workspace_name: str = "workspace.root",
    channel_masks: bool = False,
) -> None:
    """Write the bash script to combine datacards from different categories.

    :param datacards: Dictionary mapping output filenames to Datacard objects to combine.
    :type datacards: dict[Datacard]
    :param directory: Directory to save the bash script and combined datacard.
    :type directory: str
    :param path: Path (relative to directory) for the bash script file. Must end with .sh.
    :type path: str
    :param card_name: Name of the combined datacard file.
    :type card_name: str
    :param workspace_name: Name of the output workspace file.
    :type workspace_name: str
    :param channel_masks: Whether to add --channel-masks option to text2workspace.py.
    :type channel_masks: bool
    """
    assert path.endswith(".sh"), (
        "Output file must be a bash script and have .sh extension"
    )
    os.makedirs(directory, exist_ok=True)
    output_file = os.path.join(directory, path)

    print(f"Writing combination script to {output_file}")
    with open(output_file, "w") as file:
        file.write("#!/bin/bash\n")
        file.write("")
        args = " ".join(
            f"{card.bin}={filename}" for filename, card in datacards.items()
        )
        file.write(f"combineCards.py {args} > {card_name}\n")
        command = f"text2workspace.py {card_name} -o {workspace_name} \n"
        if channel_masks:
            command = command.replace(" \n", " --channel-masks \n")
        file.write(command)
