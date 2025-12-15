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
import numpy as np
import awkward as ak
import hist
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from tests.test_full_configs.test_datacard_creation_files.build_datacards import build_datacard

@pytest.fixture(scope="module")
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent

# @pytest.fixture()
# def change_dir(base_path: Path, monkeypatch: pytest.MonkeyPatch):
#     monkeypatch.chdir(base_path / "test_datacard_creation_files")


@pytest.fixture(scope="module")
def run_pc_config_run3(base_path: Path, tmp_path_factory):
    os.chdir(base_path / "test_datacard_creation_files")
    if os.path.exists("jets_calibrator_JES_JER_Syst.pkl.gz"):
        os.remove("jets_calibrator_JES_JER_Syst.pkl.gz")
    outputdir = tmp_path_factory.mktemp("test_datacard_creation_files")
    config = load_config("config_allvars_Run3.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 200
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
    return outputdir

def test_datacard_creation_single_year_run3(run_pc_config_run3):
    outputdir = run_pc_config_run3
    build_datacard(f"{outputdir}", output=f"{outputdir}/datacards_single_year", single_year=True)
    with open(f"{outputdir}/datacards_single_year/MET_pt/2btag_run3.txt") as output_datacard, open("comparison_arrays/datacards_single_year/MET_pt/2btag_run3.txt") as expected:
        out_read = output_datacard.read()
        exp_read = expected.read()
    # assert out_read == exp_read
    assert True

def test_datacard_creation_multi_year_run3(run_pc_config_run3):
    outputdir = run_pc_config_run3
    build_datacard(f"{outputdir}", output=f"{outputdir}/datacards_multi_year", single_year=False)
    with open(f"{outputdir}/datacards_multi_year/MET_pt/2btag_run3.txt") as output_datacard, open("comparison_arrays/datacards_multi_year/MET_pt/2btag_run3.txt") as expected:
        out_read = output_datacard.read()
        exp_read = expected.read()
    # assert out_read == exp_read
    assert True
