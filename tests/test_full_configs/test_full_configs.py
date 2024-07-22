import pytest
from pocket_coffea.utils.utils import load_config
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.parameters import defaults 
from pathlib import Path
from pocket_coffea.executors import executors_base as executors_lib
from coffea import processor
from coffea.processor import Runner
from coffea.util import load, save
import numpy as np

@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent



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
    old_output = load("legacy_output/output_all.coffea")

    for cat, data in old_output["sumw"].items():
        assert cat in output["sumw"]
        for dataset, _data in data.items():
            assert dataset in output["sumw"][cat]
            for sample, sumw in _data.items():
                assert np.isclose(sumw, output["sumw"][cat][dataset][sample], rtol=1e-5)

    # Testing cutflow
    for cat, data in old_output["cutflow"].items():
        assert cat in output["cutflow"]
        for dataset, _data in data.items():
            assert dataset in output["cutflow"][cat]
            if cat in ["initial", "skim", "presel"]:
                assert np.allclose(_data, output["cutflow"][cat][dataset], rtol=1e-5)
                continue
            for sample, cutflow in _data.items():
                assert np.allclose(cutflow, output["cutflow"][cat][dataset][sample], rtol=1e-5)

    # Testing variables
    for variables, data in old_output["variables"].items():
        assert variables in output["variables"]
        for dataset, _data in data.items():
            assert dataset in output["variables"][variables]
            for sample, hist in _data.items():
                variations = list([hist.axes[1].value(i) for i in range(hist.axes[1].size)])
                cats = list([hist.axes[0].value(i) for i in range(hist.axes[0].size)])
                for variation in variations:
                    for cat in cats:
                        print(f"Checking {variables} {dataset} {sample} {variation}")
                        assert np.allclose(hist[{"variation":variation, "cat":cat}].values(),
                                   output["variables"][variables][dataset][sample][{"variation":variation, "cat":cat}].values(),
                                       rtol=1e-5)
