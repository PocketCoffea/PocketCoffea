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


def compare_totalweight(output, variables):
    for variable in variables:        
        for category, datasets in output["sumw"].items():
            for dataset, samples in datasets.items():
                for sample, sumw in samples.items():
                    if dataset not in output["variables"][variable][sample]:
                        continue
                    print(f"Checking {variable} for {category} in {dataset} for {sample}")
                    print(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)
                    assert np.isclose(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)


reference_commit = "3b6cf6c"

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

    compare_outputs(output, old_output, exclude_variables=["JetGood_btagDeepFlavB", "JetGood_btagDeepFlavCvL", "JetGood_btagDeepFlavCvB"])
    compare_totalweight(output, ["nJetGood"])


    
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

    compare_totalweight(output, ["nJetGood"])


    
def test_custom_weights_on_data(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_custom_weights" )
    outputdir = tmp_path_factory.mktemp("test_custom_weights_on_data")
    config = load_config("config_weights_data.py", save_config=True, outputdir=outputdir)
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
    H = output["variables"]["nJetGood"]["DATA_SingleMuon"]["DATA_SingleMuon_2018_EraA"]
    assert np.isclose(H[{"cat":"2jets_B"}].values().sum()/ H[{"cat":"2jets_A"}].values().sum(), 2.0)

    
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

    compare_totalweight(output, ["nJetGood"])


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
        'initial': {'DATA_EGamma_2018_EraA': 500,
                    'DATA_SingleMuon_2018_EraA': 500,
                    'TTTo2L2Nu_2018': 500},
        'skim': {'DATA_EGamma_2018_EraA': 354,
                 'DATA_SingleMuon_2018_EraA': 434,
                 'TTTo2L2Nu_2018': 326},
        'presel': {'DATA_EGamma_2018_EraA': 354,
                   'DATA_SingleMuon_2018_EraA': 434,
                   'TTTo2L2Nu_2018': 326},
        'baseline': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 354},
                     'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 434,
                                                   'DATA_SingleMuon__clean': 433},
                     'TTTo2L2Nu_2018': {'TTTo2L2Nu': 326,
                                        'TTTo2L2Nu__ele': 107,
                                        'TTTo2L2Nu__mu': 156}},
        '1btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 24},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 36,
                                                'DATA_SingleMuon__clean': 36},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 279,
                                     'TTTo2L2Nu__ele': 89,
                                     'TTTo2L2Nu__mu': 138}},
        '2btag': {'DATA_EGamma_2018_EraA': {'DATA_SingleEle': 2},
                  'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 1,
                                                'DATA_SingleMuon__clean': 1},
                  'TTTo2L2Nu_2018': {'TTTo2L2Nu': 115,
                                     'TTTo2L2Nu__ele': 39,
                                     'TTTo2L2Nu__mu': 55}},
        
    }
    
    compare_totalweight(output, ["nJetGood"])

# ----------------------------------------------------------------------------------------
def test_subsamples_and_weights(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_categorization" )
    outputdir = tmp_path_factory.mktemp("test_categorization_subsamples_and_weights")
    config = load_config("config_weights_and_subsamples.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    # Check the subsamples config
    weights_dict = config.weights_config
    assert "sf_custom_A" in weights_dict["TTTo2L2Nu"]["inclusive"]
    assert "sf_custom_B" in weights_dict["TTTo2L2Nu"]["bycategory"]["1btag_B"]
    
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
    run_options["limit-chunks"] = 2
    run_options["chunksize"] = 25
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
    from pocket_coffea.utils.skim import save_skimed_dataset_definition
    save_skimed_dataset_definition(output, f"{outputdir}/skimmed_dataset_definition.json", check_initial_events=False)
    
    assert output is not None
    print(output)
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
            
    # Now checking the rescaled sumw
    # Open a file with NanoEventsFactory
    for dataset, files in output["skimmed_files"].items():
        if output["datasets_metadata"]["by_dataset"][dataset]["isMC"] == "True":
            tot_sumw = 0.
            for file in files:
                ev = NanoEventsFactory.from_root(file, schemaclass=NanoAODSchema).events()
                sumw = ak.sum(ev.genWeight * ev.skimRescaleGenWeight)
                tot_sumw += sumw
            assert np.isclose(tot_sumw, output["sum_genweights"][dataset])

    # NOw let's hadd the files and them rerun on them
    print("Running new processor on the skimmed files with different chunksize")
    config = load_config("config.py", do_load=False, outputdir=outputdir)
    # Change the dataset_cfg file
    config.datasets_cfg["jsons"] = [f"{outputdir}/skimmed_dataset_definition.json"]
    config.load_datasets()
    config.load()
    
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    # Testing a different chunksize to verify that total sum_genweight scales correctly
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
    output2 = run(config.filesets, treename="Events",
                 processor_instance=config.processor_instance)
    save(output2, outputdir / "output_all_hadd.coffea")

    # Now we compare the total sumgenweight of this output with the previous one
    assert np.isclose(output["sum_genweights"]["TTTo2L2Nu_2018"], output2["sum_genweights"]["TTTo2L2Nu_2018"])


## Very difficult to test in the CDCI, disabled
"""
def test_skimming_hadd(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
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
    run_options["limit-chunks"] = 2
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
    print(output)
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
            
    # Now checking the rescaled sumw
    # Open a file with NanoEventsFactory
    for dataset, files in output["skimmed_files"].items():
        if output["datasets_metadata"]["by_dataset"][dataset]["isMC"] == "True":
            tot_sumw = 0.
            for file in files:
                ev = NanoEventsFactory.from_root(file, schemaclass=NanoAODSchema).events()
                sumw = ak.sum(ev.genWeight * ev.skimRescaleGenWeight)
                tot_sumw += sumw
            assert np.isclose(tot_sumw, output["sum_genweights"][dataset])

    # NOw let's hadd the files and them rerun on them
    print("Hadding the skimmed files")
    outputdir_skim_hadd = tmp_path_factory.mktemp("test_skimming_skim_hadd")
    # We need to source the correct libraries to make root work
    os.system(f"source /cvmfs/cms.cern.ch/cmsset_default.sh && \
    cd {outputdir} && \
    scram p CMSSW_14_0_1 && cd CMSSW_14_0_1/src && cmsenv && \
    cd {base_path}/test_skimming && \
    pocket-coffea hadd-skimmed-files -fl {outputdir}/output_all.coffea -f 4 -o {outputdir_skim_hadd} && scram unsetenv -sh")
    # This created a local dataset file with the hadded files
    print("Running new processor on the hadded files")
    config = load_config("config_hadd.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 20
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
    output2 = run(config.filesets, treename="Events",
                 processor_instance=config.processor_instance)
    save(output2, outputdir / "output_all_hadd.coffea")

    # Now we compare the total sumgenweight of this output with the previous one
    assert np.isclose(output["sum_genweights"]["TTTo2L2Nu_2018"], output2["sum_genweights"]["TTTo2L2Nu_2018"])
"""
#-------------------------------------------------------------------

def test_columns_export(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_columns" )
    outputdir = tmp_path_factory.mktemp("test_columns")
    config = load_config("config.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)


    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 50
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
    cls = output["columns"]["TTTo2L2Nu__mu"]["TTTo2L2Nu_2018"]["4jets"]
    assert np.all(cls["JetGood_N"].value >= 4)

def test_columns_export_parquet(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_columns" )
    outputdir = tmp_path_factory.mktemp("test_columns")
    config = load_config("config_parquet.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)


    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 50
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

    # build the parquet dataset from output
    ak.to_parquet.dataset("./columns/TTTo2L2Nu_2018/ele/4jets")
    ak.to_parquet.dataset("./columns/DATA_EGamma_2018_EraA/2btag")
    # load the parquet dataset
    dataset = ak.from_parquet("./columns/TTTo2L2Nu_2018/ele/4jets")
    assert dataset is not None
    
    assert "JetGood_pt" in dataset.fields
    assert ak.all(ak.num(dataset.JetGood_pt, axis=1) >= 4)
 
