import law
import luigi
import luigi.util
from pocket_coffea.law_tasks.configuration.general import datacardconfig, transferconfig
from pocket_coffea.law_tasks.tasks.base import BaseTask
from pocket_coffea.law_tasks.tasks.runner import Runner
from pocket_coffea.utils import utils as pocket_utils
from pocket_coffea.utils.stat.combine import Datacard
import cloudpickle

law.contrib.load("wlcg", "coffea")


@luigi.util.inherits(datacardconfig, transferconfig)
class DatacardProducer(BaseTask):
    def requires(self) -> Runner:
        return Runner.req(self)

    def store_parts(self) -> tuple[str]:
        return super().store_parts() + (self.variable, self.year, self.category)

    def output(self) -> dict[str, law.LocalFileTarget]:
        out = {
            "datacard": self.local_file_target(self.datacard_name),
            "shapes": self.local_file_target(self.shapes_name),
        }
        if self.transfer:
            out.update(
                {
                    "datacard_eos": self.wlcg_file_target("datacard.txt"),
                    "shapes_eos": self.wlcg_file_target("shapes.root"),
                }
            )

    @law.decorator.safe_output
    def run(self):
        out = self.output()
        inp = self.input()

        # histograms from output
        coffea_input = inp["coffea"].load()
        histograms = coffea_input["variables"][self.variable]

        # load stat config
        stat_config = pocket_utils.path_import(self.stat_config)

        # load the configurator
        # needed for subsamples_reversed_map
        with open(inp["configurator"].abspath, "rb") as conf_pkl:
            configurator = cloudpickle.load(conf_pkl)

        datacard = Datacard(
            histograms=histograms,
            processes=stat_config.processes,
            systematics=stat_config.systematics,
            subsamples_reversed_map=configurator.subsamples_reversed_map,
            year=self.year,
            category=self.category,
        )

        datacard.dump(
            directory=out["datacard"].absdirname,
            card_name=out["datacard"].basename,
            shapes_name=out["shapes"].basename,
        )

        if self.transfer:
            out["datacard_eos"].copy_from_local(out["datacard"].path)
            out["shapes_eos"].copy_from_local(out["shapes"].path)
