import law
import law.contrib
import law.contrib.wlcg
import luigi
from pocket_coffea.law_tasks.configuration.general import datacardconfig
from pocket_coffea.law_tasks.tasks.runner import Runner

law.contrib.load("wlcg")


@luigi.util.inherits(datacardconfig)
class DatacardProducer(law.Task):
    def requires(self):
        return Runner.req(self)

    def output(self):
        out = {
            "datacard": law.LocalFileTarget(),
            "shapes": law.LocalFileTarget(),
        }
        if self.transfer:
            out.update(
                {
                    "datacard_eos": law.contrib.wlcg.WLCGFileTarget(),
                    "shapes_eos": law.contrib.wlcg.WLCGFileTarget(),
                }
            )

    def run(self):
        pass
