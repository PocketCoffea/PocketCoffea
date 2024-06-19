import os

import law
import law.contrib
import luigi
from pocket_coffea.law_tasks.configuration.general import baseconfig, runnerconfig
from pocket_coffea.law_tasks.tasks.datasets import CreateDatasets
from pocket_coffea.law_tasks.utils import (
    get_executor,
    load_analysis_config,
    load_run_options,
    process_datasets,
)
from pocket_coffea.utils import build_jets_calibrator

# load contrib packages for law
law.contrib.load("coffea")


@luigi.util.inherits(baseconfig)
class JetCalibration(law.Task):
    def __init__(self, *args, **kwargs):
        # initialize task and all parameters
        super().__init__(*args, **kwargs)
        self.config, _ = load_analysis_config(
            self.cfg, output_dir=self.output_dir, save=False
        )
        self.factory_file = self.config.parameters.jets_calibration.factory_file
        self.jets_calibration = self.config.parameters.jets_calibration

    def output(self):
        # output file for jets calibration as defined in parameters
        return law.LocalFileTarget(self.factory_file)

    def run(self):
        build_jets_calibrator.build(self.jets_calibration)


@luigi.util.inherits(baseconfig, runnerconfig)
class Runner(law.Task):
    """Run the analysis with pocket_coffea
    requires CreateDatasets task
    """

    # init class attributes
    config = None
    processor_instance = None

    def requires(self):
        return {
            "datasets": CreateDatasets.req(self),
            "jets_calibration": JetCalibration.req(self),
        }

    @property
    def skip_output_removal(self):
        return not self.test

    def output(self):
        return {
            key: law.LocalFileTarget(
                os.path.join(os.path.abspath(self.output_dir), filename)
            )
            for key, filename in [
                ("coffea", self.coffea_output),
                ("parameters", "parameters_dump.yaml"),
            ]
        }

    def run(self):
        # create output folder if it does not exist
        self.output()["coffea"].parent.touch()

        # load analysis configuration
        self.config, run_options = load_analysis_config(
            self.cfg, output_dir=self.output_dir
        )
        run_options, self.config = load_run_options(
            run_options=run_options,
            executor=self.executor,
            config=self.config,
            test=self.test,
            scaleout=self.scaleout,
            limit_files=self.limit_files,
            limit_chunks=self.limit_chunks,
        )

        executor = get_executor(
            self.executor, run_options, self.output()["coffea"].parent
        )

        process_datasets(
            coffea_executor=executor,
            config=self.config,
            run_options=run_options,
            processor_instance=self.config.processor_instance,
            output_path=self.output()["coffea"].path,
            process_separately=self.process_separately,
        )
