"""Offline unit tests for datacard rate formatting and duplicate-name detection."""
import pytest

from pocket_coffea.utils.stat.combine import format_rate
from pocket_coffea.utils.stat.systematics import (
    Systematics,
    SystematicUncertainty,
)
from pocket_coffea.utils.stat.processes import MCProcesses, MCProcess


def test_format_rate_preserves_exponent():
    # The old `f"{rate}"[:10]` produced "3.45678901" (exponent dropped, ~1e5 too large).
    small = 3.4567890123e-05
    out = format_rate(small)
    assert "e-05" in out
    assert out != "3.45678901"
    assert float(out) == pytest.approx(small, rel=1e-4)
    # Ordinary magnitudes stay compact and correct.
    assert float(format_rate(1234.5678)) == pytest.approx(1234.5678, rel=1e-4)
    assert float(format_rate(0.0)) == 0.0


def test_duplicate_systematic_datacard_name_raises():
    s1 = SystematicUncertainty(
        name="a", typ="lnN", processes=["p"], years=["2018"], value=1.02, datacard_name="dup"
    )
    s2 = SystematicUncertainty(
        name="b", typ="lnN", processes=["p"], years=["2018"], value=1.03, datacard_name="dup"
    )
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Systematics([s1, s2])
    # Distinct names build fine.
    s3 = SystematicUncertainty(
        name="c", typ="lnN", processes=["p"], years=["2018"], value=1.01, datacard_name="other"
    )
    assert set(Systematics([s1, s3]).keys()) == {"dup", "other"}


def test_duplicate_mc_process_name_raises():
    p1 = MCProcess(name="ttbb", samples=["s1"], is_signal=False, years=["2018"])
    p2 = MCProcess(name="ttbb", samples=["s2"], is_signal=True, years=["2018"])
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        MCProcesses([p1, p2])
    p3 = MCProcess(name="ttcc", samples=["s3"], is_signal=False, years=["2018"])
    assert set(MCProcesses([p1, p3]).keys()) == {"ttbb", "ttcc"}
