"""Offline unit tests for the weights/histogram crash fixes (no EOS needed)."""
import types

import numpy as np
import pytest

from pocket_coffea.lib.hist_manager import get_hist_axis_from_config
from pocket_coffea.lib.weights.weights import WeightLambda, WeightData


def _axis(**kw):
    defaults = dict(
        name="myaxis", coll=None, field=None, label="lab", bins=[1, 2, 3],
        start=0, stop=3, transform=None, overflow=True, underflow=True, growth=False,
    )
    defaults.update(kw)
    return types.SimpleNamespace(**defaults)


def test_intcat_axis_does_not_pass_underflow():
    # hist.axis.IntCategory has no `underflow` kwarg -> previously raised TypeError.
    ax = get_hist_axis_from_config(_axis(type="intcat", bins=[0, 1, 2]))
    assert list(ax) == [0, 1, 2]
    assert ax.name == "myaxis"


def test_strcat_axis_still_builds():
    ax = get_hist_axis_from_config(_axis(type="strcat", bins=["a", "b"]))
    assert list(ax) == ["a", "b"]


def test_weightlambda_single_element_output_unwrapped():
    # A 1-element sequence return must yield WeightData(nominal=<array>), not the list.
    arr = np.array([1.0, 2.0, 3.0])
    W = WeightLambda.wrap_func(
        name="w_single",
        function=lambda params, metadata, events, size, shape_variations: [arr],
        has_variations=False,
    )
    out = W(params=None, metadata=None).compute(events=None, size=3, shape_variation="nominal")
    assert isinstance(out, WeightData)
    assert isinstance(out.nominal, np.ndarray)
    assert np.array_equal(out.nominal, arr)


def test_weightlambda_three_element_output_still_works():
    nom, up, down = np.array([1.0]), np.array([1.1]), np.array([0.9])
    W = WeightLambda.wrap_func(
        name="w_triple",
        function=lambda params, metadata, events, size, shape_variations: [nom, up, down],
        has_variations=True,
    )
    out = W(params=None, metadata=None).compute(events=None, size=1, shape_variation="nominal")
    assert isinstance(out, WeightData)
    assert np.array_equal(out.nominal, nom)
    assert np.array_equal(out.up, up)
    assert np.array_equal(out.down, down)
