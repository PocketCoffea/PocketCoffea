import pytest
from pocket_coffea.utils.utils import load_config
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.parameters import defaults 
from pathlib import Path
from pocket_coffea.executors import executors_base as executors_lib
from coffea import processor
from coffea.processor import Runner
from coffea.util import load

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent



def test_new_weights(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_new_weights" )
    outputdir = tmp_path_factory.mktemp("test_new_weights")
    config = load_config("config.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 10000
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,                                                          outputdir=outputdir)

    executor = executor_factory.get()

    run = Runner(
        executor=executor,
        chunksize=run_options["chunksize"],
        maxchunks=run_options["limit-chunks"],
        schema=processor.NanoAODSchema,
        format="root"
    )
    output = run(config.filesets, treename="Events",
                 processor_instance=config.processor_instance)

    assert output is not None
    
    # Check the output

    
