"""Dependency-free helpers for coercing values that arrive from
``events.metadata`` / the datasets-definition JSON.

Boolean flags such as ``isMC`` / ``isSkim`` may reach the framework either as
native Python bools or as strings (``"True"`` / ``"true"`` / ``"1"`` / ...),
depending on whether they came straight from the coffea metadata, from a JSON
dump, or from a user config. Historically the processor, the ``Configurator``
and ``utils.get_nano_version`` each coerced them differently, so the same
dataset could be classified as MC in one place and as data in another. Route
every such coercion through :func:`to_bool` so the classification is identical
everywhere.

This module must stay import-light (no pocket_coffea imports) so it can be used
from ``configurator.py``, ``workflows/base.py`` and ``utils/utils.py`` without
creating an import cycle.
"""

_TRUE_STRINGS = {"true", "1", "yes"}
_FALSE_STRINGS = {"false", "0", "no", ""}


def to_bool(value, default=False):
    """Coerce a metadata boolean flag to a Python ``bool``.

    Accepts native bools, integers (``0``/``1``) and the string spellings that
    the datasets JSON / coffea metadata may carry (case-insensitive, whitespace
    tolerant). Returns ``default`` when ``value`` is ``None`` (e.g. an absent
    key looked up with ``.get(key)``).

    Raises ``ValueError`` on an unrecognised string, so a typo like
    ``isMC="Ture"`` fails loudly instead of being silently read as data.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):  # 0 / 1 (plain bools already handled above)
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in _TRUE_STRINGS:
            return True
        if v in _FALSE_STRINGS:
            return False
        raise ValueError(
            f"Cannot interpret metadata boolean value {value!r}; "
            f"expected one of True/False/true/false/1/0."
        )
    raise TypeError(
        f"Cannot interpret metadata boolean of type {type(value).__name__}: {value!r}"
    )
