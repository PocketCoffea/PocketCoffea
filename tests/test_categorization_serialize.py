"""Offline unit tests for CartesianSelection serialize / error paths (no EOS needed).

The Configurator serializes the categories at every run start, so a
CartesianSelection built with only multicuts (no common categories) previously
crashed at config-save time because `self.common_cats` was a plain `{}`.
"""
import pytest

from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.categorization import (
    MultiCut,
    CartesianSelection,
    StandardSelection,
)


def _cut_fn(events, params, **kwargs):
    return None


def test_cartesian_serialize_without_common_cats():
    c_lo = Cut(name="binA_lo", params={"x": 0}, function=_cut_fn)
    c_hi = Cut(name="binA_hi", params={"x": 1}, function=_cut_fn)
    mc = MultiCut(name="binA", cuts=[c_lo, c_hi], cuts_names=["lo", "hi"])
    cs = CartesianSelection(multicuts=[mc])  # no common_cats

    s = cs.serialize()  # previously raised AttributeError on {}.serialize()
    assert s["type"] == "CartesianSelection"
    assert s["common_categories"] is None
    assert len(s["multicuts"]) == 1

    # template_mask previously referenced unset attributes -> AttributeError.
    assert cs.template_mask is None


def test_standardselection_rejects_non_cut_with_clear_message():
    with pytest.raises(Exception) as exc:
        StandardSelection({"cat": ["not_a_cut"]})
    # The message must name the offending object (the fix); the old code raised a
    # NameError on an undefined variable instead.
    assert "not_a_cut" in str(exc.value)
