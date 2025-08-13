import pytest
import os
import numpy as np
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import hist

@pytest.fixture(scope="session")
def events():
    filename = "root://eoscms.cern.ch//eos/cms/store/mc/RunIISummer20UL18NanoAODv9/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/280000/01881676-C30A-2142-B3B2-7A8449DAF8EF.root"

    print(filename)
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=100).events()
    return events


def compare_outputs(output, old_output, exclude_variables=None):
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

    metadata = output["datasets_metadata"]["by_dataset"]
    # Testing variables
    for variables, data in old_output["variables"].items():
        if exclude_variables is not None and variables in exclude_variables:
            continue
        assert variables in output["variables"]    
        for sample, _data in data.items():
            assert sample in output["variables"][variables]
            for dataset, hist in _data.items():
                if metadata[dataset]["isMC"] == "True":
                    variations = list([hist.axes[1].value(i) for i in range(hist.axes[1].size)])
                    cats = list([hist.axes[0].value(i) for i in range(hist.axes[0].size)])
                    for variation in variations:
                        for cat in cats:
                            print(f"Checking {variables} {dataset} {sample} {variation}")
                            assert np.allclose(hist[{"variation":variation, "cat":cat}].values(),
                                   output["variables"][variables][sample][dataset][{"variation":variation, "cat":cat}].values(),
                                       rtol=1e-5)
                else:
                    assert np.allclose(hist.values(),
                                       output["variables"][variables][sample][dataset].values(),
                                       rtol=1e-5)
                            

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

