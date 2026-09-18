"""Run-3 weight definitions.

``sf_ele_trigger`` is already registered in ``weights.common.common``. Re-registering the
same weight *name* here (as this module used to) raises a duplicate-registration error at
import time, making the module unimportable. It is currently identical to the common one,
so it is re-exported rather than redefined. If a genuinely Run-3-specific trigger scale
factor is needed later, register it here under a distinct name.
"""
from .common import SF_ele_trigger  # noqa: F401
