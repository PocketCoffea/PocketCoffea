```bash
    ____             __        __  ______      ________          
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ / 
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/  
```
<!--[![Actions Status][actions-badge]][actions-link] -->
[![Documentation Status][rtd-badge]][rtd-link]
[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]
[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/PocketCoffea/PocketCoffea/workflows/CI/badge.svg
[actions-link]:             https://github.com/PocketCoffea/PocketCoffea/actions
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/PocketCoffea/PocketCoffea/discussions
[pypi-link]:                https://pypi.org/project/pocket-coffea/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/pocket-coffea
[pypi-version]:             https://img.shields.io/pypi/v/pocket-coffea
[rtd-badge]:                https://readthedocs.org/projects/pocketcoffea/badge/?version=latest
[rtd-link]:                 https://pocketcoffea.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->
PocketCoffea is a slim configuration framework for CMS NanoAOD analysess based on [Coffea](https://github.com/CoffeaTeam/coffea/). 

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration files), and with python code where customization is needed (by subclassing the base PocketCoffea processor).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done thans to a `BaseProcessor` class which defines a `workflow` of operations to go from Raw NanoAOD to histograms.
The user can customize the process from the confguration file or by redefining well-defined steps in the workflow.

## Documentation

- All the documentaton material is hosted at: https://pocketcoffea.readthedocs.io

- Complete analyses example under preparation: 
  - Z->ee **[analysis example page](https://pocketcoffea.readthedocs.io/en/latest/analysis_example.html)** 
  
- [Join](https://mattermost.web.cern.ch/cms-exp/channels/pocketcoffea---qa) the Q&A CMS Mattermost channel for technical questions


