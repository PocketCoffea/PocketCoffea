"""law tasks for a HEP analysis with pocket_coffea"""

import glob
import os

import law
import luigi
import luigi.util

from pocket_coffea.law_tasks.configuration.general import baseconfig, datasetconfig
from pocket_coffea.law_tasks.tasks.base import BaseTask
from pocket_coffea.law_tasks.utils import (
    create_datasets_paths,
    import_analysis_config,
    merge_datasets_definition,
    modify_dataset_output_path,
)
from pocket_coffea.utils.dataset import build_datasets

# this is a nice idea but does not currently work because the datasets definition file
# needs to exist in the output of the CreateDatasets task


# class DatasetDefinitionExists(law.ExternalTask):
#     """Check existence of dataset definition file
#     External task, the datasets definition needs to be written by hand
#     """

#     definition_file = luigi.Parameter(description="Path to the dataset definition file")  #noqa

#     def output(self):
#         return law.LocalFileTarget(os.path.abspath(self.definition_file))


# @luigi.util.inherits(datasetconfig)
# class MergeDatasetsDefinition(law.Task):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # if wildcard in --datasets-definition parameter create a list
#         # if not list only has one entry
#         self.datasets_definition_list = glob.glob(self.dataset_definition)
#         if not self.datasets_definition_list:
#             warnings.warn(
#                 law.util.colored(
#                     "No datasets definition file found."
#                     "Check the path to the datasets definition file:"
#                     f"{self.dataset_definition}",
#                     color="light red",
#                 ),
#                 stacklevel=2,
#             )
#             self.datasets_definition_list = [self.dataset_definition]

#     def requires(self):
#         # check that all the dataset definition files exist
#         return [
#             DatasetDefinitionExists(definition_file=definition)
#             for definition in self.datasets_definition_list
#         ]

#     def output(self):
#         return law.LocalFileTarget(os.path.abspath("datasets/datasets_merged.json"))

#     def run(self):
#         # load the datasets definition files and merge them into one
#         # dump it into a new file
#         merged_datasets = merge_datasets_definition(
#             [dataset.abspath for dataset in self.input()]
#         )

#         # make sure, that the dataset_dir is full path in the datasets definition
#         # so that the build_datasets function saves it at the correct path
#         merged_datasets = modify_dataset_output_path(
#             dataset_definition=merged_datasets, output_dir=self.dataset_dir
#         )

#         self.output().dump(
#             merged_datasets,
#             indent=4,
#         )


# @luigi.util.requires(MergeDatasetsDefinition)
@luigi.util.inherits(baseconfig)
@luigi.util.inherits(datasetconfig)
class CreateDatasets(BaseTask):
    """Create dataset json files"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # if wildcard in --datasets-definition parameter create a list
        # if not list only has one entry
        self.datasets_definition_list = glob.glob(self.dataset_definition)
        if not self.datasets_definition_list:
            raise FileNotFoundError(
                law.util.colored(
                    "No datasets definition file found."
                    "Check the path to the datasets definition file: "
                    f"{self.dataset_definition}",
                    color="light red",
                ),
            )

        # load config and get dataset configuration
        self.config, _ = import_analysis_config(self.cfg)
        self.dataset_config = self.config.datasets_cfg

        # load the datasets definition files and merge them into one
        self.merged_datasets = merge_datasets_definition(
            [
                os.path.abspath(definition_file)
                for definition_file in self.datasets_definition_list
            ],
        )

        # make sure, that the dataset_dir is full path in the datasets definition
        # so that the build_datasets function saves it at the correct path
        self.merged_dataset_file = self.local_path("datasets_merged.json")
        # modify paths but do not save them yet, only in run method
        self.merged_datasets = modify_dataset_output_path(
            dataset_definition=self.merged_datasets,
            dataset_configuration=self.dataset_config,
        )

    def output(self):
        """json files for datasets"""
        return {
            "merged definition": law.LocalFileTarget(self.merged_dataset_file),
            "json files": [
                law.LocalFileTarget(dataset)
                for dataset in create_datasets_paths(
                    datasets=self.merged_datasets,
                    split_by_year=self.split_by_year,
                )
            ],
        }

    def run(self):
        # modify paths and save the definition in merged output file
        modify_dataset_output_path(
            dataset_definition=self.merged_datasets,
            dataset_configuration=self.dataset_config,
            output_file=self.merged_dataset_file,
        )

        # build the dataset json file
        build_datasets(
            cfg=self.merged_dataset_file,
            keys=self.keys,
            download=self.download,
            overwrite=self.overwrite,
            check=self.check,
            split_by_year=self.split_by_year,
            local_prefix=self.local_prefix,
            allowlist_sites=self.allowlist_sites,
            blocklist_sites=self.blocklist_sites,
            regex_sites=self.regex_sites,
            parallelize=self.parallelize,
        )
