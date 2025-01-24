"""Datacard Class and Utilities for CMS Combine Tool"""

import os
from functools import cached_property

import hist
import uproot

from pocket_coffea.utils.processes import Process, Processes
from pocket_coffea.utils.systematics import Systematics, SystematicUncertainty


class Datacard(Processes, Systematics):
    """Datacard containing processes, systematics and write utilities."""

    def __init__(
        self,
        histograms: dict[str, dict[str, hist.Hist]],
        datasets_metadata: dict[str, dict[str, dict]],
        cutflow: dict[str, dict[str, float]],
        processes: list[Process],
        years: list[str],
        category: str,
        data_processes: list[Process] = None,
        systematics: list[SystematicUncertainty] = None,
        mcstat: bool = True,
        bin_prefix: str = None,
        bin_suffix: str = None,
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
        self.cutflow = cutflow
        self.processes = processes
        self.data_processes = data_processes
        self.systematics = systematics
        self.mcstat = mcstat
        self.years = years
        self.category = category
        self.bin_prefix = bin_prefix
        self.bin_suffix = bin_suffix
        if self.bin_suffix is None:
            self.bin_suffix = '_'.join(self.years)
        self.number_width = 10
        self.has_data = data_processes is not None
        if self.mcstat:
            self.threshold = 0
            self.include_signal = 0
            self.hist_mode = 1
        if self.has_data and (len(self.data_processes) != 1):
            raise NotImplementedError("Only one data process is supported.")

        # assign IDs to processes
        self.process_id = {}
        id_signal = 0  # signal processes have 0 or negative IDs
        id_background = 1  # background processes have positive IDs
        for process in self.processes:
            for year in process.years:
                if process.is_signal:
                    self.process_id[f"{process.name}_{year}"] = id_signal
                    id_signal -= 1
                else:
                    self.process_id[f"{process.name}_{year}"] = id_background
                    id_background += 1
        #self.processes = sorted(self.processes, key=lambda proc: proc.id)

        # check histograms and rearrange them
        self._check_histograms()
        self.histogram = self.rearrange_histograms(is_data=False)

        if self.has_data:
            self.data_obs = self.rearrange_histograms(is_data=True)

        # helper attributes
        self.linesep = "\n"
        self.sectionsep = "-" * 80

    @property
    def shape_variations(self) -> list[str]:
        return ["nominal"] + self.variations_names

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
        """Number of background processes"""
        return len(self.background_processes) + len(self.signal_processes) - 1

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
                    len(f"{systematic.datacard_name} {systematic.typ}")
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

    def is_empty_dataset(self, dataset: str) -> bool:
        """Check if dataset is empty"""
        return self.cutflow["presel"][dataset] == 0

    def get_datasets_by_sample(self, sample: str, year: str = None) -> list[str]:
        """Get datasets for a given sample."""
        if year is None:
            years = [year for year in self.datasets_metadata["by_datataking_period"].keys() if year in self.years]
            return [d for year in years for d in self.datasets_metadata["by_datataking_period"][year][sample]]
        else:
            return self.datasets_metadata["by_datataking_period"][year][sample]

    def _check_histograms(self) -> None:
        """Check if histograms are available for all processes and systematics."""
        for process in self.processes:
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
                            missing_systematics = set(self.shape_variations) - set(
                                self.histograms[sample][dataset].axes["variation"]
                            )
                            if missing_systematics:
                                print(
                                    f"Sample {sample} for dataset {dataset} is missing the following "
                                    f"systematics: {missing_systematics}"
                                )

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
            processes = self.processes
        assert ((not is_data) & all(not process.is_data for process in processes)) or (
            is_data & all(process.is_data for process in processes)
        ), "All processes must be either data or MC"

        # Extract variable axis from the first dataset
        dataset = list(self.get_datasets_by_sample(processes[0].samples[0]))[0]
        variable_axis = self.histograms[processes[0].samples[0]][dataset].axes[-1]

        if is_data:
            processes_names = [process.name for process in processes]
            new_histogram = hist.Hist(
                hist.axis.StrCategory(processes_names, name="process"),
                variable_axis,
                storage=hist.storage.Weight(),
            )
        else:
            processes_names = [f"{process.name}_{year}" for process in processes for year in process.years]
            new_histogram = hist.Hist(
                hist.axis.StrCategory(processes_names, name="process"),
                hist.axis.StrCategory(self.shape_variations, name="variations"),
                variable_axis,
                storage=hist.storage.Weight(),
            )
        new_histogram_view = new_histogram.view()

        for process in processes:
            for sample in process.samples:
                for year in process.years:
                    if is_data:
                        assert year is None, "Data process should not have a year"
                        process_index = new_histogram.axes["process"].index(process.name)
                    else:
                        process_index = new_histogram.axes["process"].index(f"{process.name}_{year}")
                    for dataset in self.get_datasets_by_sample(sample, year):
                        if self.is_empty_dataset(dataset): continue
                        histogram = self.histograms[sample][dataset]
                        if is_data:
                            new_histogram_view[process_index, :] += histogram[self.category, :].view()
                        else:
                            for systematic in new_histogram.axes["variations"]:
                                systematic_index = new_histogram.axes["variations"].index(systematic)
                                if systematic in histogram.axes["variation"]:
                                    new_histogram_view[process_index, systematic_index, :] += histogram[
                                        self.category, systematic, :
                                    ].view()
                                else:
                                    print(f"Setting `{systematic}` variation to nominal variation for sample {sample}.")
                                    new_histogram_view[process_index, systematic_index, :] += histogram[
                                        self.category, "nominal", :
                                    ].view()
        return new_histogram

    def create_shape_histogram_dict(
        self,
        is_data: bool = False
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
            processes = self.processes
        new_histograms = dict()
        for process in processes:
            for year in process.years:
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
                    new_histogram_view[:] = histogram[process_name_byyear, "nominal", :].view()
                    shape_name = f"{process_name_byyear}_nominal"
                    new_histograms[shape_name] = new_histogram
                    # Save shape variations
                    for systematic in self.get_systematics_by_type("shape"):
                        for shift in ("Up", "Down"):
                            variation = f"{systematic.name}{shift}"

                            # create new 1d histogram
                            new_histogram = hist.Hist(
                                histogram.axes[-1],
                                storage=hist.storage.Weight(),
                            )
                            new_histogram_view = new_histogram.view()

                            # add samples that correspond to a process
                            if process.name in systematic.processes and year in systematic.years:
                                new_histogram_view[:] = histogram[process_name_byyear, variation, :].view()
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
        content += f"{self.bin}".ljust(self.adjust_columns) * self.n_processes
        content += self.linesep

        content += "process".ljust(self.adjust_first_column)
        # process names
        content += "".join(
            f"{process.name}_{year}".ljust(self.adjust_columns) for process in self.processes for year in process.years if not process.is_data
        )
        content += self.linesep
        # process ids
        content += "process".ljust(self.adjust_first_column)
        content += "".join(
            f"{self.process_id[f'{process.name}_{year}']}".ljust(self.adjust_columns) for process in self.processes for year in process.years if not process.is_data
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
            line = systematic.datacard_name.ljust(self.adjust_syst_colum)
            line += f" {systematic.typ}"
            line = line.ljust(self.adjust_first_column)

            # processes
            for process in self.processes:
                for year in process.years:
                    if process.name in systematic.processes and year in systematic.years:
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
        for process in self.processes:
            for year in process.years:
                if not process.is_signal:
                    line = f"SF_{process.name}".ljust(self.adjust_syst_colum)
                    line += "rateParam".ljust(self.adjust_columns)
                    line += f"* {process.name}_{year} 1 [0,5]".ljust(self.adjust_columns)
                    line += self.linesep
                    content += line
        return content

    def mcstat_section(self) -> str:
        content = ""
        content += f"{self.bin} autoMCStats {self.threshold} {self.include_signal} {self.hist_mode}"
        content += self.linesep
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

        shape_histograms = self.create_shape_histogram_dict(is_data=False)
        if self.has_data:
            shape_histograms_data = self.create_shape_histogram_dict(is_data=True)
        with uproot.recreate(shapes_file) as root_file:
            if self.has_data:
                for shape, histogram in shape_histograms_data.items():
                    root_file[shape] = histogram
            for shape, histogram in shape_histograms.items():
                root_file[shape] = histogram

def combine_datacards(
    datacards: dict[Datacard],
    directory: str,
    path: str = "combine_cards.sh",
    card_name: str = "datacard_combined.txt",
    workspace_name : str = "workspace.root",
    channel_masks : list[str] = None,
    suffix: str = None,
    ) -> None:
    """Write the bash script to combine datacards from different categories.

    :param datacards: List of datacards to combine
    :type datacards: list[Datacard]
    :param output_dir: Directory to save the bash script
    :type output_dir: str
    :param output_name: Name of the bash script
    :type output_name: str
    """
    assert path.endswith(".sh"), "Output file must be a bash script and have .sh extension"
    os.makedirs(directory, exist_ok=True)
    output_file = os.path.join(directory, path)

    print(f"Writing combination script to {output_file}")
    with open(output_file, "w") as file:
        file.write("#!/bin/bash\n")
        file.write("")
        args = " ".join(
            f"{card.bin}={filename}"
            for filename, card in datacards.items()
        )
        file.write(
            f"combineCards.py {args} > {card_name}\n"
        )
        file.write(
            f"text2workspace.py {card_name} -o {workspace_name} --channel-masks \n"
        )

    # Save fit scripts
    freezeParameters = ["SF_diboson", "SF_singletop", "SF_tt_dilepton", "SF_ttv", "SF_vjets"]

    args = {
        "run_MultiDimFit.sh": [
            "-M MultiDimFit",
            f"-d {workspace_name}",
            "-n .snapshot_all_channels",
            f"--freezeParameters {','.join(freezeParameters)}",
            "--cminDefaultMinimizerStrategy 2",
            "--robustFit=1",
            "--saveWorkspace",
        ],
        "run_FitDiagnostics.sh": [
            "-M FitDiagnostics",
            f"-d {workspace_name}",
            "-n .snapshot_all_channels",
            f"--freezeParameters {','.join(freezeParameters)}",
            "--cminDefaultMinimizerStrategy 2",
            "--robustFit=1",
            "--saveWorkspace",
            "--saveShapes",
            "--saveWithUncertainties"
        ],
    }
    if channel_masks:
        for cat in channel_masks:
            args[f"run_MultiDimFit_mask_{cat}.sh"] = args["run_MultiDimFit.sh"] + [f"--setParameters mask_{cat}=1"]
            args[f"run_FitDiagnostics_mask_{cat}.sh"] = args["run_FitDiagnostics.sh"] + [f"--setParameters mask_{cat}=1"]
            args[f"run_MultiDimFit_mask_{cat}.sh"][2] = "-n .snapshot_CR_CR_ttlf"
            args[f"run_FitDiagnostics_mask_{cat}.sh"][2] = "-n .snapshot_CR_CR_ttlf"
    scripts = {path : f"combine {' '.join(l)}"+" \n" for path, l in args.items()}

    for script_name, command in scripts.items():
        script_name = script_name.replace(".sh", f"_{suffix}.sh") if suffix else script_name
        output_file = os.path.join(directory, script_name)
        print(f"Writing fit script to {output_file}")
        with open(output_file, "w") as file:
            file.write("#!/bin/bash\n")
            file.write(command)
