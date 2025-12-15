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

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent

def test_datacard_creation_run3(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_datacard_creation_files" )
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
    build_datacard(f"{outputdir}", output=f"{outputdir}/datacards")

    with open(f"{outputdir}/datacards/MET_pt/2btag_run3.txt") as output_datacard, open("comparison_arrays/2btag_run3.txt") as expected:
        out_read = output_datacard.read()
        exp_read = expected.read()
    breakpoint()
    assert out_read == exp_read
