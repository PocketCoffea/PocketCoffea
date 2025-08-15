# PocketCoffea

## Introduction

PocketCoffea is a slim analysis framework based on Coffea for CMS NanoAOD events.

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration file), and with python code where customization is needed (by defining new coffea processors).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done thanks to a `BaseProcessor` class which defines a `workflow` of operations to go from NanoAOD to histograms.
The user can customize the process from the configuration file or by redefining well-defined steps in the workflow.

The framework features a powerful **calibration system** that handles object corrections (JEC, electron energy scale, etc.) and systematic variations in a modular, configurable way. See the [Calibrators](./calibrators.md) documentation for details.

The framework handles cleanly the analysis's parameters and metadata, separating in a transparent way the definition of an
analysis phase space and metadata from CMS groups, and an analysis's run configuration. 

:::{tip}
- If you wish to jump into practice have a look at the [full analyses example](./analysis_example.md). 
- Tutorials for newcomers [here](https://github.com/PocketCoffea/Tutorials)
- Installation instructions available [here](./installation.md)
- If you want a broader introduction to the components of PocketCoffea have a look at [Concepts](./concepts.md).
- Looking for the configuration manual? [Configuration](./configuration.md)
- **New**: Learn about object calibrations and systematic variations: [Calibrators](./calibrators.md)
:::

```{toctree}
:titlesonly:
changelog.md
installation.md
concepts.md
running.md
analysis_example.md
configuration.md
calibrators.md
plots.md
statistical_analysis.md
parameters.md
datasets.md
recipes.md
law_tasks.md
performance.md
api.md
```

