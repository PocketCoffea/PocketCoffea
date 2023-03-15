# PocketCoffea

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/PocketCoffea/PocketCoffea/workflows/CI/badge.svg
[actions-link]:             https://github.com/PocketCoffea/PocketCoffea/actions
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/PocketCoffea/PocketCoffea/discussions
[pypi-link]:                https://pypi.org/project/PocketCoffea/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/PocketCoffea
[pypi-version]:             https://img.shields.io/pypi/v/PocketCoffea
[rtd-badge]:                https://readthedocs.org/projects/PocketCoffea/badge/?version=latest
[rtd-link]:                 https://PocketCoffea.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->
PocketCoffea is a slim analysis framework based on [Coffea](https://github.com/CoffeaTeam/coffea/) for CMS NanoAOD events.

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration file), and with python code where customization is needed (by subclassing the base PocketCoffea processor).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done thans to a `BaseProcessor` class which defines a `workflow` of operations to go from Raw NanoAOD to histograms.
The user can customize the process from the confguration file or by redefining well-defined steps in the workflow.

## Documentation

All the documentaton is hosted at: https://pocketcoffea.readthedocs.io
