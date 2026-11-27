"""Two datasets that share a file must keep their own metadata (coffea 0.7 bug)."""
import numpy as np
import uproot
from coffea import processor
from coffea.nanoevents import BaseSchema

from pocket_coffea.utils.metadata import resolve_chunk_metadata


class YearProcessor(processor.ProcessorABC):
    def __init__(self, fileset):
        self.fileset = fileset

    def process(self, events):
        meta = resolve_chunk_metadata(events.metadata, self.fileset)
        return {meta["dataset"]: {"raw": {events.metadata["year"]}, "fixed": {meta["year"]}}}

    def postprocess(self, accumulator):
        return accumulator


def test_shared_file_keeps_dataset_metadata(tmp_path):
    fname = str(tmp_path / "nano.root")
    with uproot.recreate(fname) as f:
        f["Events"] = {"x": np.arange(10, dtype=np.float32)}

    fileset = {
        f"TTbar_{y}": {"files": [fname], "metadata": {"year": y, "sample": "TTbar"}}
        for y in ("2024", "2025")
    }
    run = processor.Runner(
        executor=processor.IterativeExecutor(),
        schema=BaseSchema,
        metadata_cache={},
    )
    out = run(fileset, treename="Events", processor_instance=YearProcessor(fileset))

    assert out["TTbar_2024"]["fixed"] == {"2024"}
    assert out["TTbar_2025"]["fixed"] == {"2025"}
    # The coffea bug itself: both datasets see the same raw year.
    # Remove this assert if a future coffea fixes it.
    assert out["TTbar_2024"]["raw"] == out["TTbar_2025"]["raw"]
