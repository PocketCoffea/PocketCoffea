
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
from tests.utils import compare_outputs, compare_totalweight
import numpy as np
import awkward as ak
import hist
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from pocket_coffea.lib.delayed_eval import DelayedEvalBranchManager

from scipy.stats import binom

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent


reference_commit = "3b6cf6c"


class DummyCategories:
    def __init__(self, masks):
        self._masks = masks
        self.is_multidim = False

    def get_masks(self):
        return self._masks

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
    run_options["ignore-grid-certificate"] = True
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
    assert np.isclose(output["sumw"]["2jets_B"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"]["nominal"]/output["sumw"]["2jets"]["TTTo2L2Nu_2018"]["TTTo2L2Nu"]["nominal"], 2.0)
    
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


def test_delayed_eval(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    """
    Test the delayed evaluation of branches.

    This test defines a parity branch that is only computed for events ending up in histograms.
    The branch is initialized to -1 for all events. It is then set to 0 for event events
    and 1 to odd events. This test defines a region based off of the parity, calculated
    at a different stage in the workflow. It then histograms the delayed parity branch
    for each region and confirms that only the appropriate histogram bins are non-zero.

    It also defines a systematic weight based on the parity branch. It then confirms that
    the branch has been correctly defined at this stage.

    In total, this test checks that the delayed branch is always calculated for passing
    events and that it is calculated in time to define weights and fill histograms.

    """
    monkeypatch.chdir(base_path / "test_delayed_eval" )
    outputdir = tmp_path_factory.mktemp("test_delayed_eval")
    config = load_config("config.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 500
    run_options["ignore-grid-certificate"] = True
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


    H = output["variables"]["Parity"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]
    for cat in ["2jets_even", "2jets_odd"]:
        for key in H.axes["variation"]:
            histValues = H[{"cat":cat, "variation":key}].values()
            print(cat, key, histValues)
            assert histValues[0] == 0 # Parity should not stay -1 if the events pass
            if("even" in cat):
                assert histValues[1] > 0 # Parity should be 0 if the events pass the even_event cut
                assert histValues[2] == 0
            else:
                assert histValues[1] == 0 # Parity should be 1 if the events pass the odd_event cut
                assert histValues[2] > 0
        nomHistValues = H[{"cat":cat, "variation":"nominal"}].values()
        parityVarHistValuesUp = H[{"cat":cat, "variation":"parity_weightUp"}].values()
        parityVarHistValuesDown = H[{"cat":cat, "variation":"parity_weightDown"}].values()
        print("Nominal", nomHistValues)
        print("Parity weight Up", parityVarHistValuesUp)
        print("Parity weight Down", parityVarHistValuesDown)
        for x in range(3):
            assert parityVarHistValuesUp[x] == nomHistValues[x]
            assert parityVarHistValuesDown[x] ==  nomHistValues[x]


def test_delayed_eval_computes_once_per_nominal_event():
    events = ak.Array({"event": np.arange(6, dtype=np.int64)})
    manager = DelayedEvalBranchManager()

    seen_subsets = []

    def parity_fn(ev_subset):
        seen_subsets.append(ak.to_list(ev_subset.event))
        return ev_subset.event % 2

    manager.register("parity", parity_fn, default_value=-1.0)
    manager.prepare_nominal_snapshot(events)

    nominal_like = events[[0, 1, 2, 4]]
    nominal_categories = DummyCategories(
        [("2jets_even", np.array([True, False, True, False], dtype=bool))]
    )
    manager.update_for_current_variation(nominal_like, nominal_categories)

    assert ak.to_list(nominal_like.parity) == [0.0, -1.0, 0.0, -1.0]
    assert seen_subsets == [[0, 2]]

    shifted = events[[3, 1, 2, 4]]
    shifted_categories = DummyCategories(
        [("2jets_odd", np.array([True, True, False, False], dtype=bool))]
    )
    manager.update_for_current_variation(shifted, shifted_categories)

    assert ak.to_list(shifted.parity) == [1.0, 1.0, 0.0, -1.0]
    assert seen_subsets == [[0, 2], [1, 3]]


def test_delayed_eval_does_not_require_event_field_for_masks():
    events = ak.Array({"value": np.array([10, 11, 12, 13], dtype=np.int64)})
    manager = DelayedEvalBranchManager()

    def copy_value(ev_subset):
        return ev_subset.value

    manager.register("copied_value", copy_value, default_value=-1.0)
    manager.prepare_nominal_snapshot(events)

    varied = events[[0, 2, 3]]
    categories = DummyCategories(
        [("selected", np.array([False, True, True], dtype=bool))]
    )
    manager.update_for_current_variation(varied, categories)

    assert ak.to_list(varied.copied_value) == [-1.0, 12.0, 13.0]


def test_delayed_eval_supports_dense_multidimensional_outputs():
    events = ak.Array({"seed": np.arange(5, dtype=np.float32)})
    manager = DelayedEvalBranchManager()

    seen_subsets = []

    def vjet_like_outputs(ev_subset):
        seed = ak.to_numpy(ev_subset.seed)
        seen_subsets.append(seed.tolist())
        up = np.stack([seed + idx for idx in range(11)], axis=1)
        down = np.stack([seed - idx for idx in range(11)], axis=1)
        return {
            "nominal_weight": seed + 0.5,
            "weight_up": up,
            "weight_down": down,
        }

    default_matrix = np.full(11, -1.0, dtype=np.float32)
    manager.register(
        ["nominal_weight", "weight_up", "weight_down"],
        vjet_like_outputs,
        default_value={
            "nominal_weight": -1.0,
            "weight_up": default_matrix,
            "weight_down": default_matrix,
        },
    )
    manager.prepare_nominal_snapshot(events)

    varied = events[[3, 1, 4]]
    categories = DummyCategories(
        [("selected", np.array([True, False, True], dtype=bool))]
    )
    manager.update_for_current_variation(varied, categories)

    assert seen_subsets == [[3.0, 4.0]]
    assert ak.to_list(varied.nominal_weight) == [3.5, -1.0, 4.5]
    assert ak.to_list(varied.weight_up) == [
        [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0],
        [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
        [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0],
    ]
    assert ak.to_list(varied.weight_down) == [
        [3.0, 2.0, 1.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0],
        [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
        [4.0, 3.0, 2.0, 1.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0, -6.0],
    ]
    

    
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
    cls = output["columns"]["TTTo2L2Nu__mu"]["TTTo2L2Nu_2018"]["4jets"]["nominal"]
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
    ak.to_parquet.dataset("./columns/TTTo2L2Nu_2018/mu/4jets/nominal")
    ak.to_parquet.dataset("./columns/DATA_SingleMuon_2018_EraA/2btag/nominal")
    # load the parquet dataset
    dataset = ak.from_parquet("./columns/TTTo2L2Nu_2018/mu/4jets/nominal")
    assert dataset is not None
    
    assert "JetGood_pt" in dataset.fields
    assert ak.all(ak.num(dataset.JetGood_pt, axis=1) >= 4)
 

def test_skimming_presel_any_variation(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_skimming" )
    outputdir = tmp_path_factory.mktemp("test_skimming_presel_any")
    outputdir_skim = tmp_path_factory.mktemp("skim_presel_any")
    config = load_config("config_presel_any.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    assert config.save_skimmed_files != None
    config.save_skimmed_files_folder = outputdir_skim.as_posix()
    config.save_skimmed_files = outputdir_skim.as_posix()

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 100
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
    
    assert output is not None
    assert "skimmed_files" in output
    assert "nskimmed_events" in output
    
    # Check that skimmed files were actually saved
    for dataset, files in output["skimmed_files"].items():
        for file in files:
            assert Path(file).exists()

    # Get dataset name, e.g. "TTTo2L2Nu_2018"
    dataset_name = list(output["skimmed_files"].keys())[0]

    # Verify that skim cutflow is populated 
    assert output["cutflow"]["skim"][dataset_name] > 0
    # Because skip_processing_after_skim is True, variables should be empty or nominal shouldn't be processed
    assert len(output["variables"]) == 0

    # Ensure the skim output holds the expected sum_genweights
    for dataset, files in output["skimmed_files"].items():
        if output["datasets_metadata"]["by_dataset"][dataset]["isMC"] == "True":
            tot_sumw = 0.
            for file in files:
                ev = NanoEventsFactory.from_root(file, schemaclass=NanoAODSchema).events()
                sumw = ak.sum(ev.genWeight * ev.skimRescaleGenWeight)
                tot_sumw += sumw
            assert np.isclose(tot_sumw, output["sum_genweights"][dataset])
