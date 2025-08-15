import os
import warnings
from functools import reduce
from operator import add
from pathlib import Path
from typing import Iterable

import hist
import uproot
from coffea.util import load


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
