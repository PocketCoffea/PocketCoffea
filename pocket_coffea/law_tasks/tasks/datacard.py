import law
import law.contrib
import law.contrib.wlcg
import luigi
from pocket_coffea.law_tasks.configuration.general import datacardconfig, transferconfig
from pocket_coffea.law_tasks.tasks.base import BaseTask
from pocket_coffea.law_tasks.tasks.runner import Runner

law.contrib.load("wlcg")


@luigi.util.inherits(datacardconfig, transferconfig)
class DatacardProducer(BaseTask):
    def requires(self):
        return Runner.req(self)

    def output(self):
        out = {
            "datacard": self.local_file_target("datacard.txt"),
            "shapes": self.local_file_target("shapes.root"),
        }
        if self.transfer:
            out.update(
                {
                    "datacard_eos": self.wlcg_file_target("datacard.txt"),
                    "shapes_eos": self.wlcg_file_target("shapes.root"),
                }
            )
        return out

    def run(self):
        out = self.output()
        out["datacard"].dump("some datacard content")
        out["shapes"].touch()

        if self.transfer:
            out["datacard_eos"].copy_from_local(out["datacard"].path)
            out["shapes_eos"].copy_from_local(out["shapes"].path)
        # pass
