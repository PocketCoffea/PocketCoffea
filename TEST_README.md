# Test Scripts for --by-dataset Feature

This directory contains test scripts for the new `--by-dataset` feature in `make_plots.py`.

## Test Files

### test_by_dataset_unit.py
Unit tests for the core logic of the `split_samples_by_dataset()` method. These tests verify:
- Basic splitting of a single sample by dataset
- Splitting multiple samples by dataset
- That dataset names work with styling parameters (labels_mc, colors_mc, samples_groups, exclude_samples)
- Integration with the group_samples() method

**To run:**
```bash
python test_by_dataset_unit.py
```

This test runs quickly and doesn't require full PocketCoffea installation or CVMFS access.

### test_by_dataset_feature.py
Integration test that creates synthetic data and tests the complete plotting pipeline with various --by-dataset configurations. This test requires a full PocketCoffea environment with all dependencies installed.

**Note:** This test may fail in environments without CVMFS access due to default parameter loading requirements.

## Feature Description

The `--by-dataset` option allows users to split specific samples by their constituent datasets when plotting, rather than collapsing all datasets within a sample (the default behavior).

### Usage Examples

```bash
# Split QCD sample by dataset
python -m pocket_coffea.scripts.plot.make_plots --by-dataset QCD

# Split multiple samples by dataset
python -m pocket_coffea.scripts.plot.make_plots --by-dataset QCD --by-dataset TTToSemiLeptonic

# Use with other parameters
python -m pocket_coffea.scripts.plot.make_plots --by-dataset QCD --only-cat cat1 -v 2
```

### Styling Parameters

When using `--by-dataset`, the dataset names can be used in styling configurations:

```yaml
plotting_style:
  labels_mc:
    QCD_pt1: "QCD pT bin 1"
    QCD_pt2: "QCD pT bin 2"
    QCD_pt3: "QCD pT bin 3"
  
  colors_mc:
    QCD_pt1: "#1f70ba"
    QCD_pt2: "#2f80ca"
    QCD_pt3: "#4fa0ea"
  
  samples_groups:
    QCD_pt1-2: ["QCD_pt1", "QCD_pt2"]  # Group two datasets
  
  exclude_samples:
    - QCD_pt3  # Exclude a specific dataset
```

## Implementation Details

The feature is implemented in two main places:

1. **make_plots.py**: Adds the `--by-dataset` command-line argument
2. **plot_utils.py**: 
   - PlotManager passes the split_by_dataset_samples parameter to Shape objects
   - Shape class implements `split_samples_by_dataset()` method
   - This method runs before `group_samples()` to transform the data structure
   - Datasets become "samples" for all subsequent processing

The key insight is that after splitting, datasets are treated identically to samples, so all existing functionality (grouping, labeling, coloring, filtering) works automatically with dataset names.
