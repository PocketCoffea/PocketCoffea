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
    H = output["variables"]["nBJetGood"]['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert H[{ "cat": "3j_0b", "variation": "nominal"}].values()[1:].sum() == 0.


## ------------------------------------------------------------------------------------

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
        'initial': {'DATA_EGamma_2018_EraA': 501,
                    'DATA_SingleMuon_2018_EraA': 501,
                    'TTTo2L2Nu_2018': 500},
        'skim': {'DATA_EGamma_2018_EraA': 333,
                 'DATA_SingleMuon_2018_EraA': 421,
                 'TTTo2L2Nu_2018': 326},
        'presel': {'DATA_EGamma_2018_EraA': 333,
                   'DATA_SingleMuon_2018_EraA': 421,
                   'TTTo2L2Nu_2018': 326},
        'baseline': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 333},
                     'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 421,
                                                   'DATA_SingleMuon__clean': 421},
                     'TTTo2L2Nu_2018': {'TTTo2L2Nu': 326,
                                        'TTTo2L2Nu__ele': 107,
                                        'TTTo2L2Nu__mu': 156}},
        '1btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 16},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 50,
                                                'DATA_SingleMuon__clean': 50},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 279,
                                     'TTTo2L2Nu__ele': 89,
                                     'TTTo2L2Nu__mu': 138}},
        '2btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 2},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 5,
                                                'DATA_SingleMuon__clean': 5},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 115,
                                     'TTTo2L2Nu__ele': 39,
                                     'TTTo2L2Nu__mu': 55}}}

    

# ----------------------------------------------------------------------------------------

def test_skimming(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_skimming" )
    outputdir = tmp_path_factory.mktemp("test_skimming")
    outputdir_skim = tmp_path_factory.mktemp("test_skimming_skim")
    config = load_config("config.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    assert config.save_skimmed_files != None
    config.save_skimmed_files = outputdir_skim.as_posix()
    # this means that the processing will stop at the skimming step

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 10
    config.filter_dataset(run_options["limit-files"])

    executor_factory = executors_lib.get_executor_factory("iterative",
                                                          run_options=run_options,
                                                          outputdir=outputdir)

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

    assert "skimmed_files" in output
    assert "nskimmed_events" in output
    for dataset, files in output["skimmed_files"].items():
        for file in files:
            assert Path(file).exists()
    for dataset, nevents in output["nskimmed_events"].items():
        if output["datasets_metadata"]["by_dataset"][dataset]["isMC"] == "True":
            assert output["sum_genweights"][dataset] > 0
        for nevent in nevents:
            assert nevent > 0
            
