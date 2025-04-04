import os
import warnings
from functools import reduce
from operator import add
from pathlib import Path
from typing import Iterable

import hist
import uproot
from coffea.util import load

from pocket_coffea.utils.stat.processes import Processes


def save_histogram_to_root(
    hist_dict: dict[str, dict[str, hist.Hist]],
    year: str,
    category: str,
    output_file: os.PathLike,
) -> None:
    """Save histograms for one variable to a root file.

    :param hist_dict: Dictionary of histograms as returned by the processor for one variable
    :type hist_dict: dict[str, dict[str, hist.Hist]]
    :param year: Year to save
    :type year: str
    :param category: Category to save
    :type category: str
    :param output_file: Root file to save histograms
    :type output_file: os.PathLike

    The histograms for each sample are summed over all datasets.
    If the histogram has a variation axis, each variation is saved separately.
    """
    with uproot.recreate(output_file) as root_file:
        for subsample, sub_dict in hist_dict.items():
            subsample = subsample.removesuffix(f"_{year}")

            # sum all datasets for the category
            hist_sum = reduce(
                add, (histogram[category, ...] for histogram in sub_dict.values())
            )

            # save histograms to root file
            if "variation" not in hist_sum.axes.name:
                root_file[f"{subsample}"] = hist_sum
            else:
                # save each variation separately
                for variation in hist_sum.axes["variation"]:
                    root_file[f"{subsample}_{variation}"] = hist_sum[variation, ...]


def export_coffea_output_to_root(
    coffea_output: os.PathLike,
    output_dir: os.PathLike,
    variables: Iterable[str],
    categories: Iterable[str],
    years: Iterable[str],
) -> None:
    """Export pocket_coffea output to root files.

    :param coffea_output: Path to coffea output
    :type coffea_output: os.PathLike
    :param output_dir: Output directory for root files
    :type output_dir: os.PathLike
    :param variables: Names of Variables to export
    :type variables: Iterable[str]
    :param categories: Names of categories to export
    :type categories: Iterable[str]
    :param years: Years to export
    :type years: Iterable[str]
    :raises ValueError: Raise if variables are not present in the coffea output

    The output is saved in the output directory.
    Each year and category are saved as subdirectories,
    with each variable saved as a root file.
    """

    # load coffea output, get variables histograms
    histograms = load(coffea_output)["variables"]

    # make sure all variables are present in the coffea output
    # remove variables that are not present in the coffea output
    variables = set(variables)
    histogram_keys = set(histograms.keys())

    common_variables = variables & histogram_keys
    missing_variables = variables - histogram_keys
    if missing_variables:
        warnings.warn(
            f"Variables {missing_variables} are not present in the coffea output."
        )

    if not common_variables:
        raise ValueError(f"Variables {variables} found in the coffea output.")

    for variable in common_variables:
        hist_dict = histograms[variable]
        for year in years:
            for category in categories:
                output_file = Path(output_dir, year, category, f"{variable}.root")
                output_file.parent.mkdir(parents=True, exist_ok=True)

                save_histogram_to_root(hist_dict, year, category, output_file)


def rearrange_histograms(
    histograms: dict,
    processes: Processes,
    subsamples_reversed_map: dict[str, str],
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
    sample = list(processes.values())[0].samples[0]

    variable_axis = histograms[sample][
        f"{subsamples_reversed_map[sample]}_{year}"
    ].axes[-1]

    new_histogram = hist.Hist(
        hist.axis.StrCategory(list(processes.keys()), name="process"),
        hist.axis.StrCategory(shape_systematics, name="systematics"),
        variable_axis,
        storage=hist.storage.Weight(),
    )

    new_histogram_view = new_histogram.view()
    for name, process in processes.items():
        process_index = new_histogram.axes["process"].index(name)
        for sample in process.samples:
            # at this point we have a dict for each sample
            # keys are different datasets, possibly for different years
            histogram = histograms[sample]
            for dataset in histogram:
                # sum over datasets
                if not dataset.endswith(year):
                    continue
                for systematic in new_histogram.axes["systematics"]:
                    systematic_index = new_histogram.axes["systematics"].index(
                        systematic
                    )
                    new_histogram_view[process_index, systematic_index, :] += histogram[
                        dataset
                    ][category, systematic, :].view()
    return new_histogram


def create_shape_histogram_dict(
    histogram: hist.Hist, processes: Iterable[str], shape_systematics: Iterable[str]
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
            new_histogram_view[:] = histogram[process, systematic, :].view()
            new_histograms[f"{process}_{systematic}"] = new_histogram

    return new_histograms
