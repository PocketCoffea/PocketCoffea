# Cutflow plotting

This directory contains scripts and utilities for creating cutflow plots from PocketCoffea processor outputs.

## Overview

The scripts analyze the `cutflow` and `sumw` dictionaries from PocketCoffea `.coffea` files to create:
- **Cutflow plots**: Absolute number of events for each category (with ratio plots showing ratio to initial category)
- **Sum of weights plots**: Weighted number of events for each category

The scripts automatically:
- Aggregate datasets belonging to the same sample using the `datasets_metadata` information
- Handle subsamples correctly
- Create separate plots for each sample (with optional year separation)
- Support data and MC samples
- Use CMS style formatting with proper colors (`CMS_blue` for cutflow, `CMS_orange` for sum of weights)
- Apply smart y-axis limits and scientific notation formatting
- Generate both regular and ratio plots for cutflow data

## Files

### Core Utilities
- **`pocket_coffea/utils/cutflow_utils.py`**: Core plotting functions and utilities that can be imported
- **`pocket_coffea/scripts/plot/plot_cutflow.py`**: Standalone command-line script for cutflow plotting

## Basic Usage

### Command-Line Script (CLI Integration)

The cutflow plotting is integrated into the PocketCoffea CLI. After installing the package, you can use:

```bash
# Basic usage with the new CLI command
plot-cutflow -i output_all.coffea -o cutflow_plots

# All options available
plot-cutflow \
    --input-file output_all.coffea \
    --output-dir cutflow_plots \
    --exclude-categories initial skim \
    --only-samples TTToSemiLeptonic DYJetsToLL \
    --log-y \
    --figsize 12,8 \
    --format pdf \
    --summary-only
```

### Standalone Python Script

You can also run the script directly:

```bash
# Use the full-featured script
python pocket_coffea/scripts/plot/plot_cutflow.py -i output_all.coffea -o cutflow_plots

# All options
python pocket_coffea/scripts/plot/plot_cutflow.py \
    --input-file output_all.coffea \
    --output-dir cutflow_plots \
    --exclude-categories initial skim \
    --only-samples TTToSemiLeptonic DYJetsToLL \
    --log-y \
    --figsize 12,8 \
    --format pdf
```

## Using as a Python Module

```python
from coffea.util import load
from pocket_coffea.utils.cutflow_utils import plot_cutflow_from_output, print_cutflow_summary

# Load your coffea file
output = load('output_all.coffea')

# Print summary
print_cutflow_summary(output)

# Create plots with all enhanced features
saved_files = plot_cutflow_from_output(
    output, 
    output_dir='cutflow_plots',
    exclude_categories=['initial', 'skim'],
    only_samples=['TTToSemiLeptonic', 'DYJetsToLL'],
    figsize=(12, 8),
    log_y=True,
    format='pdf'
)

print(f"Created {len(saved_files['cutflow'])} cutflow plots (includes ratio versions)")
print(f"Created {len(saved_files['sumw'])} sum of weights plots")

# The cutflow plots automatically include both regular and ratio versions
# Ratio plots show the ratio of each category to the initial category
```

## Data Structure

The scripts expect PocketCoffea output with the following structure:

```python
output = {
    'cutflow': {
        'initial': {'dataset1': 1000, 'dataset2': 2000, ...},
        'skim': {'dataset1': 800, 'dataset2': 1600, ...},
        'presel': {'dataset1': 600, 'dataset2': 1200, ...},
        'category1': {
            'dataset1': {'sample1': 400, 'sample1__subsample': 100},
            'dataset2': {'sample2': 800},
            ...
        },
        ...
    },
    'sumw': {
        # Same structure as cutflow but with weighted events
        ...
    },
    'datasets_metadata': {
        'by_dataset': {
            'dataset1': {'sample': 'sample1', 'year': '2018', ...},
            'dataset2': {'sample': 'sample2', 'year': '2018', ...},
            ...
        }
    }
}
```

## Technical Details

### Color Scheme and Styling
The plotting utilities use the official CMS color palette:
- **Cutflow plots**: CMS_blue (#3f90da) 
- **Sum of weights plots**: CMS_orange (#ffa90e)

### Plot Features
- **Two-panel layout**: Cutflow plots automatically include ratio panels showing efficiency relative to initial category
- **Scientific notation**: Smart formatting for large numbers with LaTeX rendering
- **Year separation**: Automatic handling of multi-year datasets with separate plots per year
- **Font standardization**: Consistent 12pt font size for all labels and annotations
- **CMS labeling**: Proper CMS preliminary labels and luminosity information

### Data Processing
- **Sample aggregation**: Automatic grouping of datasets by sample name using `datasets_metadata`
- **Subsample handling**: Proper treatment of subsamples (e.g., `sample__subsample` naming)
- **Year extraction**: Automatic year detection from metadata for proper luminosity labeling
- **Error handling**: Robust processing with informative error messages and warnings

## Output

The scripts create:

### Individual Sample Plots
- `cutflow_<sample_name>_<year>.png`: Bar plot showing event counts for each category
- `cutflow_with_ratio_<sample_name>_<year>.png`: Two-panel plot with cutflow (top) and ratio to initial category (bottom)
- `sum_of_weights_<sample_name>_<year>.png`: Bar plot showing weighted event counts for each category


## Options

### Common Options
- `--exclude-categories`: Skip certain categories (e.g., 'initial', 'skim')
- `--only-samples`: Only create plots for specified samples
- `--log-y`: Use logarithmic y-axis scale
- `--figsize`: Figure size as 'width,height' (default: '10,6')
- `--format`: Output format (png, pdf, svg, etc.)
- `--summary-only`: Only print summary information without creating plots


## Examples

### Typical Workflow

1. **Check what's in your file:**
   ```bash
   plot-cutflow -i output_all.coffea --summary-only
   ```

2. **Create basic plots with CLI:**
   ```bash
   plot-cutflow -i output_all.coffea -o cutflow_plots
   ```

3. **Refine with specific samples and options:**
   ```bash
   plot-cutflow -i output_all.coffea -o cutflow_plots \
       --only-samples TTToSemiLeptonic DYJetsToLL DATA_SingleMuon \
       --exclude-categories initial skim \
       --log-y \
       --figsize 12,8 \
       --format pdf
   ```

### Advanced Usage Examples

```python
# Using the enhanced utilities for custom workflows
from pocket_coffea.utils.cutflow_utils import (
    plot_cutflow_from_output, 
    aggregate_by_sample, 
    plot_sample_cutflow
)

# Load and process data
output = load('output_all.coffea')

# Get aggregated data with year separation
cutflow_by_sample = aggregate_by_sample(
    output['cutflow'], 
    categories=['presel', 'category1', 'category2'],
    datasets_metadata=output['datasets_metadata']['by_dataset'],
    separate_years=True  # Creates separate entries for each year
)

# Create custom plots with ratio panels
for year, samples in cutflow_by_sample.items():
    for sample, data in samples.items():
        filepaths = plot_sample_cutflow(
            sample=sample,
            sample_data=data,
            year=year,
            categories=['presel', 'category1', 'category2'],
            plot_type='Cutflow',
            ylabel='Number of Events',
            output_dir='custom_plots',
            figsize=(12, 8),
            log_y=True,
            format='pdf',
            with_ratio=True  # Include ratio panel
        )
```


