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
    h = output["variables"]['nJetGood']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert "AK4PFchs_JES_TotalUp" in h.axes["variation"]
    assert "AK4PFchs_JES_TotalDown" in h.axes["variation"]
    assert "AK4PFchs_JERUp" in h.axes["variation"]
    assert "AK4PFchs_JERDown" in h.axes["variation"]

    # Check that the output is different from the nominal
    H = output["variables"]['JetGood_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
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
    
    # Check that the MET is different from the reference as it is explicitely not c
    H = output["variables"]['MET_pt']['DATA_SingleEle']['DATA_EGamma_2023_EraD']
    ref_MET = np.load("comparison_arrays/MET_pt_DATA_SingleMuon__clean__DATA_SingleMuon_2018_EraA_baseline_nominal.npy")
    assert not np.allclose((H[{"cat":"baseline"}].values() - ref_MET), 0)


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
    config = load_config("config_allvars_JESall.py", save_config=True, outputdir=outputdir)
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
    
    # Check the output
    params = config.parameters
    h = output["variables"]['nJetGood']['TTTo2L2Nu__ele']['TTTo2L2Nu_2018']
    for variation in params.jets_calibration.variations["2018"]["AK4PFchs"]:
        assert f"AK4PFchs_{variation}Up" in h.axes["variation"]
        assert f"AK4PFchs_{variation}Down" in h.axes["variation"]



def test_shape_variation_default_sequence_comparison_with_legacy_run2(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_allvars_Run2.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 300
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
    h = output["variables"]['nJetGood']['TTTo2L2Nu']['TTTo2L2Nu_2018']

    for variation in params.jets_calibration.variations["2018"]["AK4PFchs"]:
        assert f"AK4PFchs_{variation}Up" in h.axes["variation"]
        assert f"AK4PFchs_{variation}Down" in h.axes["variation"]

    # Load the reference output
    ref_output = load("comparison_arrays/output_run2.coffea")
    jet_pt_MC = ref_output["columns"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]["baseline"]["Jet_pt"].value
    jet_pt = output["columns"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]["baseline"]["Jet_pt"].value
    assert np.allclose(jet_pt, jet_pt_MC), "Jet pt values do not match with the reference output"
    # Check MET in MC
    met_pt_MC = ref_output["columns"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]["baseline"]["MET_pt"].value
    met_pt = output["columns"]["TTTo2L2Nu"]["TTTo2L2Nu_2018"]["baseline"]["MET_pt"].value
    assert np.allclose(met_pt, met_pt_MC), "MET pt values do not match with the reference output"

    # Compare the histograms for JES and JER variations
    ref_H = ref_output["variables"]['JetGood_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    H = output["variables"]['JetGood_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert np.allclose(ref_H[{"cat":"baseline", "variation":"JES_Total_AK4PFchsUp"}].values(), H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalUp"}].values()),\
          "JES Total Up variation does not match with the reference output"
    assert np.allclose(ref_H[{"cat":"1btag", "variation":"JER_AK4PFchsUp"}].values(), H[{"cat":"1btag", "variation":"AK4PFchs_JERUp"}].values()),\
          "JER Up variation does not match with the reference output"
    
    ref_H = ref_output["variables"]['JetGood_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    H = output["variables"]['JetGood_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert np.allclose(ref_H[{"cat":"baseline", "variation":"JES_Total_AK4PFchsDown"}].values(), H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalDown"}].values()),\
          "JES Total Down variation does not match with the reference output"
    assert np.allclose(ref_H[{"cat":"1btag", "variation":"JER_AK4PFchsDown"}].values(), H[{"cat":"1btag", "variation":"AK4PFchs_JERDown"}].values()),\
          "JER Down variation does not match with the reference output"
    
    ref_H = ref_output["variables"]['MET_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    H = output["variables"]['MET_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert np.allclose(ref_H[{"cat":"baseline", "variation":"JES_Total_AK4PFchsUp"}].values() , H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalUp"}].values()),\
          "JES Total Up variation does not match with the reference output"
    assert np.allclose(ref_H[{"cat":"1btag", "variation":"JER_AK4PFchsUp"}].values(), H[{"cat":"1btag", "variation":"AK4PFchs_JERUp"}].values()),\
          "JER Up variation does not match with the reference output"

    ref_H = ref_output["variables"]['MET_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    H = output["variables"]['MET_pt']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert np.allclose(ref_H[{"cat":"baseline", "variation":"JES_Total_AK4PFchsDown"}].values() , H[{"cat":"baseline", "variation":"AK4PFchs_JES_TotalDown"}].values()),\
          "JES Total Down variation does not match with the reference output"
    assert np.allclose(ref_H[{"cat":"1btag", "variation":"JER_AK4PFchsDown"}].values(), H[{"cat":"1btag", "variation":"AK4PFchs_JERDown"}].values()),\
          "JER Down variation does not match with the reference output"
    

    # Check that the MET histograms only the jet_calibration variations
    H = output["variables"]['MET_pt_2']['TTTo2L2Nu']['TTTo2L2Nu_2018']
    assert H.axes["variation"].size == 3
    assert H.axes["variation"].value(0) == "AK4PFchs_JES_TotalDown"
    assert H.axes["variation"].value(1) == "AK4PFchs_JES_TotalUp"
    assert H.axes["variation"].value(2) == "nominal"




def test_shape_variation_default_sequence_comparison_with_legacy_run3(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_allvars_Run3.py", save_config=True, outputdir=outputdir)
    assert isinstance(config, Configurator)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 300
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
    # Load the reference output
    ref_output = load("comparison_arrays/output_run3.coffea")
    jet_pt_MC = ref_output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["Jet_pt"].value
    jet_pt = output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["Jet_pt"].value
    assert np.allclose(jet_pt, jet_pt_MC), "Jet pt values do not match with the reference output"
    # Check MET in MC
    met_pt_MC = ref_output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["PuppiMET_pt"].value
    met_pt = output["columns"]["DATA_SingleEle"]["DATA_EGamma_2023_EraD"]["baseline"]["PuppiMET_pt"].value
    assert np.allclose(met_pt, met_pt_MC), "MET pt values do not match with the reference output"



def test_shape_variation_JEC_run3_pt_regression(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory):
    monkeypatch.chdir(base_path / "test_shape_variations" )
    outputdir = tmp_path_factory.mktemp("test_shape_variations")
    config = load_config("config_JEC_ptregr.py", save_config=True, outputdir=outputdir)
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
    