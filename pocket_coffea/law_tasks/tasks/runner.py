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


@luigi.util.inherits(baseconfig, runnerconfig)
class Runner(BaseTask):
    """Run the analysis with pocket_coffea
    requires CreateDatasets task
    """

    # init class attributes
    config = None

    def requires(self) -> dict[str, law.Task]:
        return CreateDatasets.req(self)

    def store_parts(self) -> tuple[str]:
        if self.test:
            return super().store_parts() + ("test",)
        return super().store_parts()

    @property
    def skip_output_removal(self) -> bool:
        return not self.test

    def output(self) -> dict[str, law.LocalFileTarget]:
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
