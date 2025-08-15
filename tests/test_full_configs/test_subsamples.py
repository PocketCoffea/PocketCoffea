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
from utils import compare_outputs, compare_totalweight
import numpy as np
import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import hist

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent



def test_subsamples(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_categorization" )
    outputdir = tmp_path_factory.mktemp("test_categorization_subsamples")
    config = load_config("config_subsamples.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    assert config.samples == ['TTTo2L2Nu', 'DATA_SingleMuon']
    assert config.has_subsamples["TTTo2L2Nu"] == True
    assert config.has_subsamples["DATA_SingleMuon"] == True
    assert config.subsamples_list == ['DATA_SingleMuon__clean', 'TTTo2L2Nu__ele', 'TTTo2L2Nu__mu']
    assert config.subsamples_reversed_map == {'TTTo2L2Nu__ele': 'TTTo2L2Nu',
                                              'TTTo2L2Nu__mu': 'TTTo2L2Nu',
                                              'DATA_SingleMuon__clean': 'DATA_SingleMuon'}

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
    assert output["cutflow"] == {
        'initial': {'DATA_SingleMuon_2018_EraA': 500,
                    'TTTo2L2Nu_2018': 500},
        'skim': {'DATA_SingleMuon_2018_EraA': 434,
                 'TTTo2L2Nu_2018': 326},
        'presel': {'DATA_SingleMuon_2018_EraA': 434,
                   'TTTo2L2Nu_2018': 326},
        'baseline': {'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 434,
                                                   'DATA_SingleMuon__clean': 433},
                     'TTTo2L2Nu_2018': {'TTTo2L2Nu': 326,
                                        'TTTo2L2Nu__ele': 107,
                                        'TTTo2L2Nu__mu': 156}},
        '1btag': {'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 36,
                                                'DATA_SingleMuon__clean': 36},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 280,
                                     'TTTo2L2Nu__ele': 89,
                                     'TTTo2L2Nu__mu': 140}},
        '2btag': {'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 1,
                                                'DATA_SingleMuon__clean': 1},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 112,
                                     'TTTo2L2Nu__ele': 39,
                                     'TTTo2L2Nu__mu': 52}},
        
    }
    
    
    compare_totalweight(output, ["nJetGood"])

# ----------------------------------------------------------------------------------------
def test_subsamples_and_weights(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_subsamples" )
    outputdir = tmp_path_factory.mktemp("test_subsamples_and_weights")
    config = load_config("config_weights_and_subsamples.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    weights_dict = config.weights_config
    assert "sf_custom_A" in weights_dict["TTTo2L2Nu"]["inclusive"]
    assert "sf_custom_B" in weights_dict["TTTo2L2Nu"]["bycategory"]["1btag_B"]
    
    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 200
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

    # some checks
    assert output is not None
    sw = output["sumw"]
    assert np.isclose(sw["1btag_B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"] / sw["1btag"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"], 3.)
    assert np.isclose(sw["1btag_B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele"] / sw["1btag"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele"], 3.)

    # Checking variations
    h = output["variables"]["ElectronGood_eta"]["TTTo2L2Nu__ele"]["TTTo2L2Nu_2018"]
    # N.B: the variation is checked w.r.t the nominal of a category without the custom weight,
    # if not the factor include the nominal custom weight
    assert np.isclose(h[{"cat":"1btag_B", "variation":"sf_custom_BUp"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 5.)
    assert np.isclose(h[{"cat":"1btag_B", "variation":"sf_custom_BDown"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 0.7)
    assert np.isclose(h[{"cat":"1btag", "variation":"sf_custom_AUp"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 4./2.)
    assert np.isclose(h[{"cat":"1btag", "variation":"sf_custom_ADown"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 0.5/2.)
    h = output["variables"]["ElectronGood_eta"]["TTToSemiLeptonic"]["TTToSemiLeptonic_2016_PostVFP"]
    # N.B: the variation is checked w.r.t the nominal of a category without the custom weight,
    # if not the factor include the nominal custom weight
    assert np.isclose(h[{"cat":"1btag_B", "variation":"sf_custom_BUp"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 5.)
    assert np.isclose(h[{"cat":"1btag_B", "variation":"sf_custom_BDown"}].values().sum() / h[{"cat":"1btag", "variation":"nominal"}].values().sum(), 0.7)
# ----------------------------------------------------------------------------------------

def test_subsamples_and_weights_splitbysubsamples(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_subsamples" )
    outputdir = tmp_path_factory.mktemp("test_categorization_subsamples_and_weights_splitbysubsamples")
    config = load_config("config_weights_and_subsamples_splitbysubsamples.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    weights_dict = config.weights_config
    assert "sf_custom_C" in weights_dict["TTToSemiLeptonic"]["bycategory"]["B"]
    assert "sf_custom_C" in weights_dict["TTTo2L2Nu"]["by_subsample"]["TTTo2L2Nu__ele"]["inclusive"]
    
    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 200
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

     # some checks
    assert output is not None
    sw = output["sumw"]
    assert np.isclose(sw["B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"] / sw["A"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"], 1.)
    # Ele 2 has the same cut but not additional scale factor applied by suybsample
    assert np.isclose(sw["A"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele"] / sw["A"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele2"], 2.)
    assert np.isclose(sw["B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele"] / sw["B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu__ele2"], 6.)

    # check histograms
    h1 = output["variables"]["nJetGood"]["TTTo2L2Nu__ele"]["TTTo2L2Nu_2018"]
    h2 = output["variables"]["nJetGood"]["TTTo2L2Nu__ele2"]["TTTo2L2Nu_2018"]

    assert np.isclose(h1[{"cat":"A", "variation":"nominal"}].values().sum() / h2[{"cat":"A", "variation":"nominal"}].values().sum(), 2.)
    assert np.isclose(h1[{"cat":"B", "variation":"nominal"}].values().sum() / h2[{"cat":"B", "variation":"nominal"}].values().sum(), 6.)
    assert np.isclose(h1[{"cat":"B", "variation":"nominal"}].values().sum() / h1[{"cat":"A", "variation":"nominal"}].values().sum(), 3.)
    
    # Checking variations
    # N.B: the variation is checked w.r.t the nominal of a category without the custom weight,
    # if not the factor include the nominal custom weight
    assert np.isclose(h1[{"cat":"A", "variation":"sf_custom_CUp"}].values().sum() / h2[{"cat":"A", "variation":"nominal"}].values().sum(), 4.)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_CUp"}].values().sum() / h2[{"cat":"B", "variation":"nominal"}].values().sum(), 4.*3.)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_CUp"}].values().sum() / h1[{"cat":"B", "variation":"nominal"}].values().sum(), 2.) #4./2.

    assert np.isclose(h1[{"cat":"A", "variation":"sf_custom_CDown"}].values().sum() / h2[{"cat":"A", "variation":"nominal"}].values().sum(), 0.5)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_CDown"}].values().sum() / h2[{"cat":"B", "variation":"nominal"}].values().sum(), 1.5)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_CDown"}].values().sum() / h1[{"cat":"B", "variation":"nominal"}].values().sum(), 0.25) #0.5./2.
    
    assert np.isclose(h1[{"cat":"A", "variation":"sf_custom_DUp"}].values().sum() / h2[{"cat":"A", "variation":"nominal"}].values().sum(), 2.) #like nominal
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_DUp"}].values().sum() / h2[{"cat":"B", "variation":"nominal"}].values().sum(), 5.*2.)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_DUp"}].values().sum() / h1[{"cat":"B", "variation":"nominal"}].values().sum(), 5./3.) 

    assert np.isclose(h1[{"cat":"A", "variation":"sf_custom_DDown"}].values().sum() / h2[{"cat":"A", "variation":"nominal"}].values().sum(), 2.)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_DDown"}].values().sum() / h2[{"cat":"B", "variation":"nominal"}].values().sum(), 0.7*2.)
    assert np.isclose(h1[{"cat":"B", "variation":"sf_custom_DDown"}].values().sum() / h1[{"cat":"B", "variation":"nominal"}].values().sum(), 0.7/3.) 
    

def test_subsamples_wrong1(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_subsamples" )
    outputdir = tmp_path_factory.mktemp("test_categorization_subsamples_wrong1")
    config = load_config("config_weights_and_subsamples_wrong1.py", save_config=True, outputdir=outputdir)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 200
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
