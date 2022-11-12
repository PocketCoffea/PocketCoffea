# PocketCoffea

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![Conda-Forge][conda-badge]][conda-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/PocketCoffea/PocketCoffea/workflows/CI/badge.svg
[actions-link]:             https://github.com/PocketCoffea/PocketCoffea/actions
<!-- [conda-badge]:              https://img.shields.io/conda/vn/conda-forge/PocketCoffea -->
<!-- [conda-link]:               https://github.com/conda-forge/PocketCoffea-feedstock -->
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/PocketCoffea/PocketCoffea/discussions

<!-- [pypi-link]:                https://pypi.org/project/PocketCoffea/ -->
<!-- [pypi-platforms]:           https://img.shields.io/pypi/pyversions/PocketCoffea -->
<!-- [pypi-version]:             https://img.shields.io/pypi/v/PocketCoffea -->

[rtd-badge]:                https://readthedocs.org/projects/PocketCoffea/badge/?version=latest
[rtd-link]:                 https://PocketCoffea.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->
PocketCoffea is a slim analysis framework based on Coffea for CMS NanoAOD events.

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration file), and with python code where customization is needed (by defining new coffea processors).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done by having a `BaseProcess` class which defines a `workflow` of operation to go from Raw NanoAOD to histograms.
The user can customize the process from the confguration file or by redefining well-defined steps in the workflow.


Have a look at the documentaton https://pocketcoffea.readthedocs.io
