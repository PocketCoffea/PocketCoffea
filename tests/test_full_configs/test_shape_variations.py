import pytest
import os
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
import awkward as ak
import hist
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent




def test_shape_variations(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 500
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
    h = output["variables"]['nJetGood']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']
    assert "JES_Total_AK4PFchsUp" in h.axes["variation"]
    assert "JES_Total_AK4PFchsDown" in h.axes["variation"]
    assert "JER_AK4PFchsUp" in h.axes["variation"]
    assert "JER_AK4PFchsDown" in h.axes["variation"]

    # Now let's check the values by category
    assert np.isclose(h[{ "variation": "JES_Total_AK4PFchsUp", "cat": "baseline"}].values(), h[{ "variation": "nominal", "cat": "baseline"}].values()).any()
    assert np.isclose(h[{ "variation": "JER_AK4PFchsDown", "cat": "baseline"}].values(), 0.).all()
    assert np.isclose(h[{ "variation": "JES_Total_AK4PFchsUp", "cat": "1btag"}].values(), h[{ "variation": "nominal", "cat": "1btag"}].values()).any()
    assert np.isclose(h[{ "variation": "JER_AK4PFchsDown", "cat": "1btag"}].values(), h[{ "variation": "nominal", "cat": "1btag"}].values()).any()
