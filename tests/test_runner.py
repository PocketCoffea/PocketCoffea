import os
from pathlib import Path
import pytest


@pytest.fixture
def base_path() -> Path:
    """Get the current folder of the test"""
    return Path(__file__).parent


def test_runner_local(base_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Test the runner script"""
    monkeypatch.chdir(base_path / "test_full_configs/test_categorization/")
    outputdir = tmp_path_factory.mktemp("test_runner")
    status = os.system(f"pocket-coffea run --cfg config_subsamples.py -o {outputdir} --test -lf 1 -lc 1 --chunksize 100")
    assert status == 0
    assert (outputdir / "output_all.coffea").exists()
