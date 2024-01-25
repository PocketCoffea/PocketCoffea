# Changelog

This page keeps track of major and minor changes between versions. 
### PocketCoffea 0.7

- Improved and generalized executors configuration for runner.py

### PocketCoffea 1.0rc0
The main change is the possibility to completely split the analysis configuration files and parameters from the core of
the framework.

- Parameters configuration now in place with OmegaConf
- Docker and singularity images built with GitLab CD/CI and published on
  `/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general`
- Improved the handling of datasets and samples
- Added dataset metadata in the output file

