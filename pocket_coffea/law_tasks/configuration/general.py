import os

import law
import luigi


class baseconfig(luigi.Config):
    """config class that holds general parameters"""

    cfg = luigi.Parameter(
        description="Config file with parameters specific to the current run",
        default=os.path.join(os.getcwd(), "config.py"),
    )
    # output_dir = luigi.Parameter(
    #     description="Output directory for the coffea processor and plots",
    #     default=os.path.join(os.getcwd(), "output"),
    # )


class datasetconfig(luigi.Config):
    """Paramters for dataset creation"""

    dataset_definition = luigi.Parameter(
        description="json file containing the datasets definitions (wildcard supported)",
        default=os.path.join(os.getcwd(), "datasets", "datasets_definitions.json"),
    )
    dataset_dir = luigi.Parameter(
        description="Output directory for the datasets json files,"
        "default: CWD/datasets",
        default=os.path.join(os.getcwd(), "datasets"),
    )
    keys = luigi.TupleParameter(
        description=(
            "Keys of the datasets to be created."
            "If None, the keys are read from the datasets definition file."
        ),
        default=(),
    )
    download = luigi.BoolParameter(
        description="Download datasets from DAS, default: False", default=False
    )
    overwrite = luigi.BoolParameter(
        description="Overwrite existing .json datasets, default: False", default=False
    )
    check = luigi.BoolParameter(
        description="Check existence of the datasets, default: False", default=False
    )
    split_by_year = luigi.BoolParameter(
        description="Split datasets by year, default: False", default=False
    )
    local_prefix = luigi.Parameter(
        description="Prefix of the local path where the datasets are stored",
        default="",
    )
    allowlist_sites = luigi.TupleParameter(
        description="List of sites to be whitelisted", default=()
    )
    blocklist_sites = luigi.TupleParameter(
        description="List of sites to be blacklisted", default=()
    )
    regex_sites = luigi.Parameter(
        description="Regex string to be used to filter the sites", default=""
    )
    parallelize = luigi.IntParameter(
        description=(
            "Number of parallel processes to be used to fetch the datasets, default: 4"
        ),
        default=4,
    )


class runnerconfig(luigi.Config):
    # parameters
    coffea_output = luigi.Parameter(
        description="Filename of the coffea output", default="output.coffea"
    )
    test = luigi.BoolParameter(
        description="Run with limit 1 interactively", default=False
    )
    limit_files = luigi.IntParameter(description="Limit number of files", default=None)
    limit_chunks = luigi.IntParameter(
        description="Limit number of chunks", default=None
    )
    executor = luigi.Parameter(
        description=(
            "Overwrite executor from config (to be used only with the --test options)"
        ),
        default="iterative",
    )
    scaleout = luigi.IntParameter(description="Overwrite scaleout config", default=10)
    process_separately = luigi.BoolParameter(
        description="Process each dataset separately", default=False
    )


class datacardconfig(luigi.Config):
    datacard_name = luigi.Parameter(
        default="datacard.txt", description="Name of the datacard file"
    )
    shapes_name = luigi.Parameter(
        default="shapes.root", description="Name of the shapes file"
    )
    datacard_dir = luigi.Parameter(
        default="datacards", description="Output folder for datacards"
    )
    transfer = luigi.BoolParameter(
        default=False, description="Transfer datacards to EOS"
    )
