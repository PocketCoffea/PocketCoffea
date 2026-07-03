"""Test that the ``group_tags`` argument of ``setup_cvmfs_resolver`` overrides
the version (tag) used by the ``${cvmfs:...}`` resolver.

Needs the CMS CAT metadata tree mounted at
``/cvmfs/cms-griddata.cern.ch/cat/metadata`` (available on the CI runner / inside
the pocketcoffea apptainer image). The test self-skips when it is not present.

We check a single, concrete file that is actually referenced by the default
parameters (``jet_scale_factors.yaml``): the BTV b-tagging SF for one Run 3
period. The concrete version tag to override to is discovered from the live tree
so the test does not break when the tag list changes.
"""

import os

import pytest
from omegaconf import OmegaConf

from pocket_coffea.parameters.defaults import setup_cvmfs_resolver

CVMFS_BASE = "/cvmfs/cms-griddata.cern.ch/cat/metadata"

# A specific file referenced by the default parameters.
GROUP = "BTV"
PERIOD = "Run3-22CDSep23-Summer22-NanoAODv12"
FILE = "btagging.json.gz"


def _available_version_tag():
    """A concrete (non-``latest``) tag that provides FILE for GROUP/PERIOD."""
    period_dir = os.path.join(CVMFS_BASE, GROUP, PERIOD)
    if not os.path.exists(os.path.join(period_dir, "latest", FILE)):
        return None
    for tag in sorted(os.listdir(period_dir)):
        if tag == "latest":
            continue
        if os.path.isfile(os.path.join(period_dir, tag, FILE)):
            return tag
    return None


VERSION_TAG = _available_version_tag()

requires_cvmfs = pytest.mark.skipif(
    VERSION_TAG is None,
    reason=f"{GROUP}/{PERIOD}/{FILE} not available under {CVMFS_BASE}",
)


def _resolve(expr: str) -> str:
    conf = OmegaConf.create({"x": expr})
    OmegaConf.resolve(conf)
    return conf.x


@requires_cvmfs
def test_group_tags_overrides_version_for_file():
    expr = f"${{cvmfs:{PERIOD},{GROUP},{FILE}}}"

    # No group_tags -> default "latest" version.
    setup_cvmfs_resolver(None)
    default_path = _resolve(expr)
    assert default_path == f"{CVMFS_BASE}/{GROUP}/{PERIOD}/latest/{FILE}"

    # group_tags -> the user-provided version replaces "latest".
    setup_cvmfs_resolver({GROUP: {PERIOD: VERSION_TAG}})
    overridden_path = _resolve(expr)
    assert overridden_path == f"{CVMFS_BASE}/{GROUP}/{PERIOD}/{VERSION_TAG}/{FILE}"

    assert overridden_path != default_path
    assert os.path.exists(overridden_path)
