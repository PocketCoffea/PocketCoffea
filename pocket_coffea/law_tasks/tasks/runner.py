import os

import law
import law.contrib
import luigi

from pocket_coffea.law_tasks.configuration.general import baseconfig, runnerconfig
from pocket_coffea.law_tasks.tasks.base import BaseTask
from pocket_coffea.law_tasks.tasks.datasets import CreateDatasets
from pocket_coffea.law_tasks.utils import (
    get_executor,
    import_analysis_config,
    load_analysis_config,
    load_run_options,
    process_datasets,
)
from pocket_coffea.utils import build_jets_calibrator

# load contrib packages for law
law.contrib.load("coffea")


@luigi.util.inherits(baseconfig)
class JetCalibration(BaseTask):
    # set version to None, Jet calibration is independend of analysis or version
    version = None
    # skip output removal if not interactively
    skip_output_removal = True

    def __init__(self, *args, **kwargs):
        # initialize task and all parameters
        super().__init__(*args, **kwargs)
        self.config, _ = import_analysis_config(self.cfg)
        self.factory_file = self.config.parameters.jets_calibration.factory_file
        self.jets_calibration = self.config.parameters.jets_calibration

    def output(self):
        # output file for jets calibration as defined in parameters
        return law.LocalFileTarget(os.path.abspath(self.factory_file))

    def run(self):
        build_jets_calibrator.build(self.jets_calibration)


@luigi.util.inherits(baseconfig, runnerconfig)
class Runner(BaseTask):
    """Run the analysis with pocket_coffea
    requires CreateDatasets task
    """

    # init class attributes
    config = None

    def requires(self):
        return {
            "datasets": CreateDatasets.req(self),
            "jets_calibration": JetCalibration.req(self),
        }

    def store_parts(self) -> tuple[str]:
        if self.test:
            return super().store_parts() + ("test",)
        return super().store_parts()

    @property
    def skip_output_removal(self):
        return not self.test

    def output(self):
        return {
            key: self.local_file_target(filename)
            for key, filename in [
                ("coffea", self.coffea_output),
                ("parameters", "parameters_dump.yaml"),
                ("config", "config.json"),
                ("configurator", "configurator.pkl"),
            ]
        }

    def run(self):
        # create output folder if it does not exist
        output = self.output()["coffea"]
        output.parent.touch()

        # load analysis configuration
        self.config, run_options = load_analysis_config(
            self.cfg, output_dir=self.local_path()
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

        executor = get_executor(self.executor, run_options, output.parent.path)

        process_datasets(
            coffea_executor=executor,
            config=self.config,
            run_options=run_options,
            processor_instance=self.config.processor_instance,
            output_path=output.path,
            process_separately=self.process_separately,
        )
