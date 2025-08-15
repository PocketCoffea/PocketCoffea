#!/usr/bin/env python3

import os
import sys
import click
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from coffea.util import load
from pocket_coffea.utils.cutflow_utils import print_cutflow_summary

@click.command()
@click.option('-i', '--input-file', type=str, required=True, help='Input .coffea file')
@click.option('-o', '--output-dir', type=str, default='cutflow_plots', help='Output directory for plots')
@click.option('--exclude-categories', type=str, multiple=True, help='Categories to exclude from plots')
@click.option('--only-samples', type=str, multiple=True, help='Only plot these samples')
@click.option('--format', type=str, default='png', help='Output format (png, pdf, etc.)')
@click.option('--log-y', is_flag=True, help='Use logarithmic y-axis')
@click.option('--figsize', type=str, default='10,6', help='Figure size as "width,height"')
@click.option('--summary-only', is_flag=True, help='Only print summary, do not create plots')

def plot_cutflow(input_file, output_dir, exclude_categories, only_samples, format, log_y, figsize, summary_only):
    """
    Plot cutflow histograms from PocketCoffea output files.
    
    Creates separate plots for:
    - Cutflow (absolute number of events)
    - Sum of weights (weighted number of events)
    
    One plot per sample, with all categories shown as bars.
    """
    
    # Load the coffea file
    if not os.path.exists(input_file):
        print(f"ERROR: Input file {input_file} does not exist")
        sys.exit(1)
    
    print(f"Loading {input_file}...")
    output = load(input_file)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Parse figure size
    try:
        figwidth, figheight = map(float, figsize.split(','))
    except:
        print("ERROR: Invalid figsize format. Use 'width,height' (e.g., '10,6')")
        sys.exit(1)
    
    # Extract data structures
    cutflow = output.get('cutflow', {})
    sumw = output.get('sumw', {})
    datasets_metadata = output.get('datasets_metadata', {}).get('by_dataset', {})
    
    if not cutflow:
        print("ERROR: No cutflow data found in the input file")
        sys.exit(1)
    
    if not datasets_metadata:
        print("WARNING: No datasets_metadata found. Will use dataset names as sample names.")
    
    # Convert click tuples to lists for utility functions
    exclude_categories_list = list(exclude_categories) if exclude_categories else None
    only_samples_list = list(only_samples) if only_samples else None
    
    # Print summary using utility function if available
    print_cutflow_summary(output, exclude_categories_list, only_samples_list)
    
    # If summary only, return here
    if summary_only:
        return
    
    # Get all categories, excluding specified ones
    all_categories = list(cutflow.keys())
    categories = [cat for cat in all_categories if cat not in (exclude_categories_list or [])]
    
    print(f"\nFound categories: {categories}")
    if exclude_categories_list:
        print(f"Excluded categories: {exclude_categories_list}")
    
    # Function to aggregate datasets by sample
    def aggregate_by_sample(data_dict):
        """Aggregate counts from datasets belonging to the same sample"""
        sample_data = defaultdict(lambda: defaultdict(float))
        
        for category in categories:
            if category not in data_dict:
                continue
                
            for dataset, counts in data_dict[category].items():
                # Get sample name from metadata
                if datasets_metadata and dataset in datasets_metadata:
                    sample = datasets_metadata[dataset]['sample']
                else:
                    # Fallback: use dataset name as sample name
                    sample = dataset
                
                # Skip if only_samples is specified and this sample is not in the list
                if only_samples_list and sample not in only_samples_list:
                    continue
                
                if isinstance(counts, dict):
                    # This is for categories with subsamples
                    for subsample, count in counts.items():
                        sample_key = subsample  # Use the full subsample name
                        sample_data[sample_key][category] += count
                else:
                    # This is for simple categories (initial, skim, presel)
                    sample_data[sample][category] += counts
        
        return sample_data
    
    # Aggregate data by sample
    cutflow_by_sample = aggregate_by_sample(cutflow)
    sumw_by_sample = aggregate_by_sample(sumw) if sumw else {}
    
    print(f"Found samples: {list(cutflow_by_sample.keys())}")
    
    # Plot cutflow for each sample
    def make_plots(data_by_sample, plot_type, ylabel):
        """Create plots for cutflow or sumw data"""
        
        for sample, sample_data in data_by_sample.items():
            # Get categories and their counts for this sample
            sample_categories = []
            sample_counts = []
            
            for category in categories:
                if category in sample_data:
                    sample_categories.append(category)
                    sample_counts.append(sample_data[category])
            
            if not sample_categories:
                print(f"WARNING: No data found for sample {sample}")
                continue
            
            # Create plot
            fig, ax = plt.subplots(figsize=(figwidth, figheight))
            
            # Create bar plot
            x_pos = np.arange(len(sample_categories))
            bars = ax.bar(x_pos, sample_counts, alpha=0.7)
            
            # Customize plot
            ax.set_xlabel('Category')
            ax.set_ylabel(ylabel)
            ax.set_title(f'{plot_type} - {sample}')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(sample_categories, rotation=45, ha='right')
            
            if log_y:
                ax.set_yscale('log')
            
            # Add value labels on bars
            for bar, count in zip(bars, sample_counts):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{count:.0f}' if count >= 1 else f'{count:.2e}',
                       ha='center', va='bottom')
            
            # Tight layout and save
            plt.tight_layout()
            
            # Clean sample name for filename
            clean_sample = sample.replace('/', '_').replace('__', '_')
            filename = f"{plot_type.lower()}_{clean_sample}.{format}"
            filepath = os.path.join(output_dir, filename)
            
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"Saved: {filepath}")
    
    # Create cutflow plots
    if cutflow_by_sample:
        print("\nCreating cutflow plots...")
        make_plots(cutflow_by_sample, 'Cutflow', 'Number of Events')
    
    # Create sumw plots
    if sumw_by_sample:
        print("\nCreating sum of weights plots...")
        make_plots(sumw_by_sample, 'Sum_of_Weights', 'Weighted Number of Events')
    else:
        print("No sumw data found, skipping weighted plots")
    
    print(f"\nAll plots saved to: {output_dir}")
    
    # Print summary
    print("\nSummary:")
    print(f"Categories plotted: {categories}")
    print(f"Samples plotted: {list(cutflow_by_sample.keys())}")
    
    # Show category breakdown
    if cutflow_by_sample:
        print(f"\nCutflow data preview:")
        for sample in list(cutflow_by_sample.keys())[:3]:  # Show first 3 samples
            print(f"  {sample}:")
            for cat in categories[:5]:  # Show first 5 categories
                if cat in cutflow_by_sample[sample]:
                    count = cutflow_by_sample[sample][cat]
                    print(f"    {cat}: {count:.0f}")


if __name__ == "__main__":
    plot_cutflow()
