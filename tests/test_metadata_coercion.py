"""Unit tests for pocket_coffea.utils.metadata.to_bool.

to_bool is the single coercion helper that keeps isMC/isSkim classification
identical across the processor (base.py), the Configurator and
utils.get_nano_version, which previously coerced the same metadata three
different (and mutually inconsistent) ways.
"""
import pytest

from pocket_coffea.utils.metadata import to_bool


@pytest.mark.parametrize("value", [True, "True", "true", "TRUE", " true ", "1", 1, "yes"])
def test_to_bool_truthy(value):
    assert to_bool(value) is True


@pytest.mark.parametrize("value", [False, "False", "false", "FALSE", " false ", "0", 0, "no", ""])
def test_to_bool_falsy(value):
    assert to_bool(value) is False


def test_to_bool_none_uses_default():
    assert to_bool(None) is False
    assert to_bool(None, default=True) is True


def test_to_bool_unknown_string_raises():
    # a typo must fail loudly instead of being silently read as data/MC
    with pytest.raises(ValueError):
        to_bool("Ture")


def test_to_bool_unknown_type_raises():
    with pytest.raises(TypeError):
        to_bool(1.5)


def test_to_bool_agrees_on_the_old_divergent_cases():
    # the exact spellings that used to be classified differently by the three
    # former coercions must now all agree
    for v in ["True", "true", True]:
        assert to_bool(v) is True
    # utils.get_nano_version used truthiness, so the *string* "False" was wrongly
    # truthy -> treated as MC; it must now be False
    assert to_bool("False") is False
