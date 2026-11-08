import pytest

from pocket_coffea.utils.configurator import Configurator


def _config(nevents, files):
    config = Configurator.__new__(Configurator)
    config.filesets = {
        "dataset": {"files": list(files), "metadata": {"nevents": str(nevents)}}
    }
    config.datasets = ["dataset"]
    return config


def test_filter_dataset_by_events_uses_ceiling_and_scales_metadata():
    config = _config(100, ["f0", "f1", "f2", "f3", "f4"])

    config.filter_dataset_by_events(21)

    assert config.filesets["dataset"]["files"] == ["f0", "f1"]
    assert config.filesets["dataset"]["metadata"]["nevents"] == "40"


def test_filter_dataset_by_events_keeps_all_files_when_target_is_larger():
    config = _config(100, ["f0", "f1"])

    config.filter_dataset_by_events(101)

    assert config.filesets["dataset"]["files"] == ["f0", "f1"]
    assert config.filesets["dataset"]["metadata"]["nevents"] == "100"


def test_filter_dataset_by_events_warns_and_keeps_invalid_dataset():
    config = _config("unknown", ["f0"])

    with pytest.warns(UserWarning, match="Could not estimate events per file"):
        config.filter_dataset_by_events(10)

    assert config.filesets["dataset"]["files"] == ["f0"]


def test_filter_dataset_by_events_resolves_dataset_sample_and_default_targets():
    config = Configurator.__new__(Configurator)
    config.filesets = {
        "TT_2023": {
            "files": ["tt0", "tt1", "tt2", "tt3"],
            "metadata": {"nevents": 400, "sample": "TT"},
        },
        "DY_2023": {
            "files": ["dy0", "dy1", "dy2", "dy3"],
            "metadata": {"nevents": 400, "sample": "DY"},
        },
        "W_2023": {
            "files": ["w0", "w1", "w2", "w3"],
            "metadata": {"nevents": 400, "sample": "W"},
        },
    }
    config.datasets = list(config.filesets)

    config.filter_dataset_by_events({"TT_2023": 201, "DY": 101, "default": 301})

    assert len(config.filesets["TT_2023"]["files"]) == 3
    assert len(config.filesets["DY_2023"]["files"]) == 2
    assert len(config.filesets["W_2023"]["files"]) == 4


def test_filter_dataset_by_events_rejects_missing_mapping_target():
    config = _config(100, ["f0"])
    with pytest.raises(ValueError, match="no entry"):
        config.filter_dataset_by_events({"other": 10})


def test_filter_dataset_by_events_rejects_nonpositive_scalar():
    config = _config(100, ["f0"])
    with pytest.raises(ValueError, match="must be positive"):
        config.filter_dataset_by_events(0)
