import json

from pocket_coffea.utils.benchmarking import (
    add_sample_processing_stats,
    load_sample_throughputs,
    write_sample_throughputs,
)


def test_sample_throughput_file_is_updated_per_config(tmp_path):
    path = tmp_path / "timeit"
    path.mkdir()
    (path / "stale.json").write_text(json.dumps({"stale": 3.0}))
    filesets = {
        "dataset": {"metadata": {"sample": "same"}},
        "dataset2": {"metadata": {"sample": "same"}},
    }
    output = {
        "cutflow": {"initial": {"dataset": 100, "dataset2": 50}},
        "processing_time": {"dataset": 2.0, "dataset2": 5.0},
    }
    stats = {}

    add_sample_processing_stats(stats, output, filesets)
    write_sample_throughputs(path, stats)

    assert json.loads((path / "dataset.json").read_text()) == {"dataset": 50.0}
    assert json.loads((path / "dataset2.json").read_text()) == {"dataset2": 10.0}
    assert load_sample_throughputs(path) == {"dataset": 50.0, "dataset2": 10.0, "stale": 3.0}
