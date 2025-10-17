"""utility functions for law tasks in pocket_coffea"""

import importlib
import json
import os
import re
import typing
import warnings
from collections import Counter
from types import ModuleType
from typing import Union

import coffea
import law
from coffea.nanoevents.schemas.base import BaseSchema
from coffea.processor import NanoAODSchema, accumulate
from coffea.processor import Runner as CoffeaRunner
from coffea.processor.executor import ExecutorBase
from omegaconf import OmegaConf

from pocket_coffea.executors import executors_base
from pocket_coffea.parameters import defaults as parameters_utils
from pocket_coffea.utils import utils as pocket_utils
from pocket_coffea.utils.configurator import Configurator

# Type aliases
FileName = Union[str, os.PathLike]


def filter_items_by_regex(
    regex: str, items: typing.Iterable[str], match: bool = True
) -> list:
    """
    Apply a regular expression on an iterable.

    :param regex: The regular expression to match.
    :type regex: str
    :param items: The iterable of items to match the regular expression on.
    :type items: typing.Iterable[str]
    :param match: Flag indicating whether to match the regular expression (True)
        or not (False). Default is True.
    :type match: bool, optional
    :return: The list of items that match (or do not match) the regular expression.
    :rtype: list[str]
    """
    if match:
        return [item for item in items if re.match(regex, item)]
    else:
        return [item for item in items if not re.match(regex, item)]


def extract_executor_and_site(executor: str) -> tuple:
    """Extract executor and site from executor string.

    :param executor: Name of the executor.
    :type executor: str
    :return: Tuple containing the name of the executor and cluster.
    :rtype: tuple
    """
    if "@" in executor:
        executor_name, site = executor.split("@")
    else:
        executor_name = executor
        site = None
    return executor_name, site


def read_datasets_definition(dataset_definition: FileName) -> dict:
    """Read the datasets definition file and return it as a dictionary.

    :param dataset_definition: The path to the dataset definition file.
    :type dataset_definition: str or os.PathLike
    :return: The datasets definition as a dictionary.
    :rtype: dict
    """
    if not os.path.isfile(dataset_definition):
        raise FileNotFoundError(
            f"Dataset definition file {dataset_definition} not found"
        )
    with open(dataset_definition) as f:
        return json.load(f)


def merge_datasets_definition(definition_files: list) -> dict[str, dict]:
    """
    Merge multiple dataset definition files into one.

    :param definition_files: List of dataset definition files.
    :type definition_files: list
    :return: The merged dataset definition.
    :rtype: dict

    .. warning::
        If duplicate keys are found in the datasets definition,
        a warning will be raised.

    """

    datasets = [read_datasets_definition(file) for file in definition_files]

    keys = [key for dataset in datasets for key in dataset]
    duplicate_keys = {key: count for key, count in Counter(keys).items() if count > 1}
    if duplicate_keys:
        raise Warning(f"Duplicate keys found in datasets definition: {duplicate_keys}")

    return {key: value for dataset in datasets for key, value in dataset.items()}


def create_datasets_paths(
    datasets: dict, split_by_year: bool = False, output_dir: FileName = None
) -> list:
    """
    Create a set of dataset paths based on the given datasets dictionary.
    Split datasets by year if the split_by_year flag is set.
    The input datasets definition dictionary has to be according to pocket_coffea.
    The set of file paths is returned.

    :param datasets: A dictionary containing dataset information.
    :type datasets: dict
    :param split_by_year: A flag indicating whether to split the datasets by year.
        Default is False.
    :type split_by_year: bool, optional
    :return: A list of dataset paths.
    :rtype: list
    """
    output_paths = set()
    for dataset in datasets.values():
        if output_dir is not None:
            filepath = os.path.join(os.path.abspath(output_dir), dataset["json_output"])
        else:
            filepath = os.path.abspath(dataset["json_output"])

        if split_by_year:
            years = {
                dataset_file["metadata"]["year"] for dataset_file in dataset["files"]
            }

            # append year if split by year is true
            for year in years:
                output_paths.update(
                    [
                        filepath.replace(".json", f"_{year}.json"),
                        filepath.replace(".json", f"_redirector_{year}.json"),
                    ]
                )
        else:
            output_paths.update(
                [filepath, filepath.replace(".json", "_redirector.json")]
            )
    return sorted(output_paths)


def modify_dataset_output_path(
    dataset_definition: Union[FileName, dict],
    dataset_configuration: dict,
    output_file: FileName = None,
) -> dict:
    """
    Modify the dataset definition file to include the full output path
    in the json output field.

    :param dataset_definition: The path to the dataset definition file or
        the dataset definition as a dictionary.
    :type dataset_definition: Union[FileName, dict]
    :param dataset_configuration: The configuration for the datasets from the configurator.
    :type dataset_configuration: dict
    :param output_file: The name of the output file. If provided, the modified
        dataset definition will be saved with this filename in the output directory.
        If not provided, the modified dataset definition will not be saved.
        Default is None.
    :type output_file: str or os.PathLike
    :return: The modified dataset definition as a dictionary.
    :rtype: dict
    """
    if any(isinstance(dataset_definition, inst) for inst in typing.get_args(FileName)):
        dataset_definition = read_datasets_definition(dataset_definition)

    # get json outputs from configuration
    jsons = dataset_configuration["jsons"]
    for dataset in dataset_definition.values():
        dataset_json = dataset["json_output"]
        # check for matching filenames
        for json_output in jsons:
            json_output = str(json_output)
            if json_output.endswith(dataset_json):
                dataset["json_output"] = json_output

    if output_file is not None:
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(dataset_definition, f, indent=4)

    return dataset_definition


def import_analysis_config(cfg: FileName) -> tuple[Configurator, ModuleType]:
    """Import the analysis configuration module and return the Configurator object.

    :param cfg: path to the config.py file
    :type cfg: FileName
    :raises AttributeError: if config.py has no attribute `cfg`
    :raises TypeError: if cfg is not of type Configurator (pocket_coffea)
    :return: Configurator object and the imported module
    :rtype: tuple[Configurator, ModuleType]
    """
    config_module = pocket_utils.path_import(cfg)

    try:
        config = config_module.cfg
    except AttributeError as e:
        raise AttributeError(
            "The provided configuration module does not contain a `cfg` attribute"
            "of type Configurator. "
            "Please check your configuration."
        ) from e

    if not isinstance(config, Configurator):
        raise TypeError(
            "`cfg` in config file is not of type Configurator."
            "Please check your configuration."
        )

    return config, config_module


def load_analysis_config(
    cfg: FileName, output_dir: FileName, save: bool = True
) -> tuple[Configurator, dict]:
    """
    Load the analysis config.

    :param cfg: Path to the config file.
    :type cfg: FileName
    :param output_dir: The output directory to save the configuration and parameters.
    :type output_dir: FileName
    :param save: Flag indicating whether to save the configuration and parameters.
        Default is True.
    :type save: bool, optional
    :raises AttributeError: If the config file does not have the attribute `cfg`.
    :raises TypeError: If `cfg` is not of type Configurator (pocket_coffea).
    :return: A tuple containing the Configurator and run_options (if defined in config).
    :rtype: tuple
    """
    config, config_module = import_analysis_config(cfg)
    config.load()

    if save:
        config.save_config(output_dir)

    run_options = getattr(config_module, "run_options", {})

    return config, run_options


def load_run_options(
    run_options: dict,
    executor: str,
    config: Configurator,
    test: bool = False,
    scaleout: int = None,
    limit_files: int = None,
    limit_chunks: int = None,
) -> tuple:
    """
    Load the run options for a given executor and scaleout value.
    Update the configuration based on the provided run options.

    :param run_options: A dictionary containing the run options.
    :type run_options: dict
    :param executor: The executor to use (and possible site, e.g. dask@lxplus).
    :type executor: str
    :param config: The Configurator object.
    :type config: Configurator
    :param test: Flag indicating whether to run in test mode. Defaults to False.
    :type test: bool, optional
    :param scaleout: The scaleout value. Defaults to None.
    :type scaleout: int, optional
    :param limit_files: The limit of files. Defaults to None.
    :type limit_files: int, optional
    :param limit_chunks: The limit of chunks. Defaults to None.
    :type limit_chunks: int, optional
    :return: A tuple containing the updated run options dictionary and
        the Configurator object.
    :rtype: tuple

    This function loads the run options for a given executor and scaleout value.
    It merges the default run options with the provided run options, and updates the
    scaleout value if provided. The run options are returned as a dictionary.
    """
    executor, site = extract_executor_and_site(executor)
    default_run_options = parameters_utils.get_default_run_options()
    general_run_options = default_run_options["general"]

    if site in default_run_options:
        general_run_options.update(default_run_options[site])

    if executor in default_run_options:
        general_run_options.update(default_run_options[executor])

    if f"{executor}@{site}" in default_run_options:
        general_run_options.update(default_run_options[f"{executor}@{site}"])

    run_options = parameters_utils.merge_parameters(general_run_options, run_options)

    if scaleout is not None:
        run_options["scaleout"] = scaleout

    if test:
        run_options["limit-files"] = limit_files if limit_files is not None else 1
        run_options["limit-chunks"] = limit_chunks if limit_chunks is not None else 1
        config.filter_dataset(run_options["limit-files"])

    return run_options, config


def get_executor(executor: str, run_options: dict, output_dir: FileName):
    """
    Get the executor factory based on the provided executor, run options, and output
    directory. Loads the module defined in pocket_coffea executors.

    :param executor: The name of the executor and possible site (e.g. dask@lxplus).
    :type executor: str
    :param run_options: The run options for the executor.
    :type run_options: dict
    :param output_dir: The output directory for the executor.
    :type output_dir: FileName
    :return: The executor factory.
    :rtype: executors_base.ExecutorFactoryABC
    :raises TypeError: If the executor factory is not of type
        executors_base.ExecutorFactoryABC.
    """
    executor, site = extract_executor_and_site(executor)
    if site is not None:
        executor_module = importlib.import_module(
            f"pocket_coffea.executors.executors_{site}"
        )
    else:
        executor_module = importlib.import_module(
            "pocket_coffea.executors.executors_base"
        )

    executor_factory = executor_module.get_executor_factory(
        executor, run_options=run_options, outputdir=output_dir
    )

    if not isinstance(executor_factory, executors_base.ExecutorFactoryABC):
        raise TypeError(
            f"Executor factory is not of type {executors_base.ExecutorFactoryABC}"
        )

    return executor_factory.get()


def process_datasets(
    coffea_executor: ExecutorBase,
    config: Configurator,
    run_options: dict,
    processor_instance,
    output_path: FileName = None,
    process_separately: bool = False,
    schema: BaseSchema = NanoAODSchema,
    file_format: str = "root",
):
    if process_separately:
        # raise NotImplementedError(
        #     "separate processing for each dataset is not yet implemented"
        # )
        outputs = []
        for sample, files in config.filesets.items():
            # If the number of available workers exceeds the maximum number of workers
            # for a given sample, the chunksize is reduced so that all the workers are
            # used to process the given sample
            n_events_tot = int(files["metadata"]["nevents"])
            n_workers_max = n_events_tot / run_options["chunksize"]

            if run_options["scaleout"] > n_workers_max:
                adapted_chunksize = n_events_tot // run_options["scaleout"]
            else:
                adapted_chunksize = run_options["chunksize"]

            fileset = {sample: files}

            run = CoffeaRunner(
                executor=coffea_executor,
                chunksize=adapted_chunksize,
                maxchunks=run_options["limit-chunks"],
                skipbadfiles=run_options["skip-bad-files"],
                schema=schema,
                format=file_format,
            )

            print(f"Working on sample: {sample}")
            output = run(
                fileset, treename="Events", processor_instance=processor_instance
            )
            outputs.append(output)

            if output_path is not None:
                head, tail = os.path.split(output_path)
                split_output = os.path.join(head, "separate_output")
                os.makedirs(split_output, exist_ok=True)
                coffea.util.save(output, os.path.join(split_output, f"{sample}_{tail}"))

        # accumulate separate files
        output = accumulate(outputs)

    else:
        fileset = config.filesets

        run = CoffeaRunner(
            executor=coffea_executor,
            chunksize=run_options["chunksize"],
            maxchunks=run_options["limit-chunks"],
            skipbadfiles=run_options["skip-bad-files"],
            schema=schema,
            format=file_format,
        )

        print(f"Working on samples: {list(fileset.keys())}")
        output = run(fileset, treename="Events", processor_instance=processor_instance)

    if output_path is not None:
        coffea.util.save(output, output_path)
    else:
        return output


def load_plotting_style(params_file: FileName, custom_plot_style: FileName = None):
    """
    Load the plotting style parameters from a configuration file.
    Merge them if custom_plot_style is provided

    :param params_file: The path to the configuration file containing
        the plotting style parameters.
    :param custom_plot_style: The path to a custom plotting style file. If provided,
        the parameters from this file will be merged with the default parameters.
    :return: The plotting style parameters.
    """
    parameters = OmegaConf.load(params_file)
    if os.path.isfile(custom_plot_style):
        # get the default parameters and overwrite them with the custom ones
        parameters = parameters_utils.get_defaults_and_compose(custom_plot_style)
    elif (custom_plot_style is not None) and (custom_plot_style != law.NO_STR):
        warnings.warn(
            f"custom plotting style file {custom_plot_style} not found."
            "Using default style",
            stacklevel=3,
        )

    OmegaConf.resolve(parameters)
    return parameters["plotting_style"]


def load_sample_names(sample_config: FileName, prefix: str = None):
    """
    Load sample names from a sample configuration file.

    :param sample_config: The path to the sample configuration file.
    :type sample_config: str or os.PathLike
    :param prefix: Optional prefix to filter sample names. Only sample names that start
        with the specified prefix will be included. Defaults to None.
    :type prefix: str, optional
    :return: List of sample names.
    :rtype: list[str]
    """
    with open(sample_config) as f:
        config = json.load(f)

    samples = config["datasets"]["samples"]
    if prefix is not None:
        samples = [sample for sample in samples if sample.startswith(prefix)]
    return samples


def exclude_samples_from_plotting(plotting_style: dict, exclude_samples: list):
    """
    Exclude specified samples from plotting.

    :param plotting_style: The plotting style configuration.
    :type plotting_style: dict
    :param exclude_samples: The list of sample names to exclude from plotting.
    :type exclude_samples: list[str]
    :return: The updated plotting style configuration.
    :rtype: dict

    :Example:

    >>> plotting_style = {"exclude_samples": []}
    >>> exclude_samples = ["sample1", "sample2"]
    >>> exclude_samples_from_plotting(plotting_style, exclude_samples)
    {'exclude_samples': ['sample1', 'sample2']}
    """
    OmegaConf.update(plotting_style, "exclude_samples", exclude_samples)
    if exclude_samples:
        print(f"excluding samples from plotting: {exclude_samples}")
    return plotting_style
