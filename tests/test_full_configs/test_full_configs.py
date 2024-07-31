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



    
def test_cartesian_categorization(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_categorization" )
    outputdir = tmp_path_factory.mktemp("test_categorization_cartesian")
    config = load_config("config_cartesian_categories.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the categories configuration
    assert config.categories.categories == ['inclusive', '4jets_40pt', '1j_0b', '1j_1b', '1j_2b',
                                            '1j_3b', '2j_0b', '2j_1b', '2j_2b', '2j_3b', '3j_0b',
                                            '3j_1b', '3j_2b', '3j_3b']
    

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
    H = output["variables"]["nBJetGood"]['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert H[{ "cat": "3j_0b", "variation": "nominal"}].values()[1:].sum() == 0.


def test_subsamples(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_categorization" )
    outputdir = tmp_path_factory.mktemp("test_categorization_subsamples")
    config = load_config("config_subsamples.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    assert config.samples == ['TTTo2L2Nu', 'DATA_SingleMuon', 'DATA_SingleEle']
    assert config.has_subsamples["TTTo2L2Nu"] == True
    assert config.has_subsamples["DATA_SingleMuon"] == True
    assert config.has_subsamples["DATA_SingleEle"] == False
    assert config.subsamples_list == ['DATA_SingleMuon__clean', 'TTTo2L2Nu__ele', 'TTTo2L2Nu__mu']
    assert config.subsamples_reversed_map == {'TTTo2L2Nu__ele': 'TTTo2L2Nu',
                                              'TTTo2L2Nu__mu': 'TTTo2L2Nu',
                                              'DATA_SingleMuon__clean': 'DATA_SingleMuon',
                                              'DATA_SingleEle': 'DATA_SingleEle'}

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
    assert output["cutflow"] == {
        'initial': {'DATA_EGamma_2018_EraA': 10004,
                    'DATA_SingleMuon_2018_EraA': 10010,
                    'TTTo2L2Nu_2018': 10000},
        'skim': {'DATA_EGamma_2018_EraA': 6514,
                 'DATA_SingleMuon_2018_EraA': 8157,
                 'TTTo2L2Nu_2018': 6220},
        'presel': {'DATA_EGamma_2018_EraA': 6514,
                   'DATA_SingleMuon_2018_EraA': 8157,
                   'TTTo2L2Nu_2018': 6220},
        'baseline': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 6514},
                     'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 8157,
                                                   'DATA_SingleMuon__clean': 8149},
                     'TTTo2L2Nu_2018': {'TTTo2L2Nu': 6220,
                                        'TTTo2L2Nu__ele': 1900,
                                        'TTTo2L2Nu__mu': 2911}},
        '1btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 342},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 754,
                                                'DATA_SingleMuon__clean': 750},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 5174,
                                     'TTTo2L2Nu__ele': 1594,
                                     'TTTo2L2Nu__mu': 2414}},
        '2btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 30},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 103,
                                                'DATA_SingleMuon__clean': 100},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 2285,
                                     'TTTo2L2Nu__ele': 691,
                                     'TTTo2L2Nu__mu': 1074}}}
    
