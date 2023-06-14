# PocketCoffea

## Introduction

PocketCoffea is a slim analysis framework based on Coffea for CMS NanoAOD events.

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration file), and with python code where customization is needed (by defining new coffea processors).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done thanks to a `BaseProcessor` class which defines a `workflow` of operations to go from Raw NanoAOD to histograms.
The user can customize the process from the confguration file or by redefining well-defined steps in the workflow.

```{toctree}
:titlesonly:
installation.md
running.md
concepts.md
configuration.md
parameters.md
datasets.md
analysis_example.md
performance.md
api.md
```

