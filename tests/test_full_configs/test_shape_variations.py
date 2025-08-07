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
from pocket_coffea.parameters import defaults
@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent


def test_shape_variations_JEC_run2(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_JEC_Run2.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 500
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,outputdir=outputdir)

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
    assert "AK4PFchs_JES_TotalUp" in h.axes["variation"]
    assert "AK4PFchs_JES_TotalDown" in h.axes["variation"]
    assert "AK4PFchs_JERUp" in h.axes["variation"]
    assert "AK4PFchs_JERDown" in h.axes["variation"]

    # Check that the output is different from the nominal
    H = output["variables"]['JetGood_pt']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']
    assert not np.isclose(H[{"cat":"baseline", "variation":"nominal"}].values().sum()/ H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalUp"}].values().sum(),  1.)



def test_shape_variations_JEC_run3(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_JEC_Run3.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 500
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,outputdir=outputdir)

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
    # h = output["variables"]['nJetGood']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']
    # assert "AK4PFchs_JES_TotalUp" in h.axes["variation"]
    # assert "AK4PFchs_JES_TotalDown" in h.axes["variation"]
    # assert "AK4PFchs_JERUp" in h.axes["variation"]
    # assert "AK4PFchs_JERDown" in h.axes["variation"]

    # # Check that the output is different from the nominal
    # H = output["variables"]['JetGood_pt']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']
    # assert not np.isclose(H[{"cat":"baseline", "variation":"nominal"}].values().sum()/ H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalUp"}].values().sum(),  1.)


def test_shape_variations_ele_SS_run3(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_eleSS_Run3.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 100
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,outputdir=outputdir)

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
    pt_orig = output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["ElectronGood_pt_original"]
    pt = output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["ElectronGood_pt"]
    assert np.all(pt_orig != pt)


def test_shape_variation_default_sequence(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_allvars.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 500
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,outputdir=outputdir)

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
    
    params = config.parameters
       # Check the output
    h = output["variables"]['nJetGood']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']

    for variation in params.jets_calibration.variations["2018"]["AK4PFchs"]:
        assert f"AK4PFchs_{variation}Up" in h.axes["variation"]
        assert f"AK4PFchs_{variation}Down" in h.axes["variation"]

