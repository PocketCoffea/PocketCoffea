import pytest
from pocket_coffea.utils.utils import load_config
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.parameters import defaults 
from pathlib import Path
from pocket_coffea.executors import executors_base as executors_lib
from coffea import processor
from coffea.processor import Runner
from coffea.util import load, save
from utils import compare_outputs
import numpy as np

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent


reference_commit = "98fcca4c"

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
    save(output, outputdir / "output_all.coffea")
    
    assert output is not None
    
    # Check the output
    old_output = load(f"output_{reference_commit}/output_all.coffea")

    compare_outputs(output, old_output)


    
def test_custom_weights(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_custom_weights" )
    outputdir = tmp_path_factory.mktemp("test_custom_weights")
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
    save(output, outputdir / "output_all.coffea")
    
    assert output is not None

    # Check the output
    assert np.isclose(output["sumw"]["2jets_B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"]/output["sumw"]["2jets"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"], 2.0)
    
    H = output["variables"]["nJetGood"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]
    assert np.isclose(H[{"cat":"2jets_B", "variation":"nominal"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 2.0)

    # Checking custom weight variations
    assert np.isclose(H[{"cat":"2jets_C", "variation":"my_custom_weight_withvarUp"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum() ,3.0)
    assert np.isclose(H[{"cat":"2jets_C", "variation":"my_custom_weight_withvarDown"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 0.5)

    # Checking for multivariations by sample
    assert np.isclose(H[{"cat":"2jets_D", "variation":"my_custom_weight_multivar_statUp"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 3.0)
    assert np.isclose(H[{"cat":"2jets_D", "variation":"my_custom_weight_multivar_statDown"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 0.5)

    assert np.isclose(H[{"cat":"2jets_D", "variation":"my_custom_weight_multivar_systUp"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 4.0)
    assert np.isclose(H[{"cat":"2jets_D", "variation":"my_custom_weight_multivar_systDown"}].values().sum()/ H[{"cat":"2jets", "variation":"nominal"}].values().sum(), 0.25)
