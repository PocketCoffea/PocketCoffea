"""
Cutflow plotting utilities for PocketCoffea output files.

This module provides functions to create cutflow plots from PocketCoffea processor output,
showing the number of events (cutflow) and sum of weights (sumw) for different categories
and samples.
"""

import os
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import mplhep as hep
from pocket_coffea.utils.plot_utils import COLOR_ALIASES

CMS_BLUE = COLOR_ALIASES['CMS_blue']
CMS_ORANGE = COLOR_ALIASES['CMS_orange']


def aggregate_by_sample(data_dict: Dict, categories: List[str], 
                       datasets_metadata: Dict, only_samples: Optional[List[str]] = None) -> Dict:
    """
    Aggregate counts from datasets belonging to the same sample.
    
    Parameters:
    -----------
    data_dict : dict
        Dictionary with structure {category: {dataset: counts}}
    categories : list
        List of categories to process
    datasets_metadata : dict
        Metadata mapping datasets to samples
    only_samples : list, optional
        If provided, only include these samples
        
    Returns:
    --------
    dict
        Aggregated data with structure {sample: {category: count}}
    """
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
            if only_samples and sample not in only_samples:
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


def plot_sample_cutflow(sample: str, sample_data: Dict, categories: List[str],
                       plot_type: str, ylabel: str, output_dir: str,
                       figsize: Tuple[float, float] = (10, 6), 
                       log_y: bool = False, format: str = 'png',
                       with_ratio: bool = False) -> List[str]:
    """
    Create cutflow plot(s) for a single sample.
    
    Parameters:
    -----------
    sample : str
        Sample name
    sample_data : dict
        Data for this sample {category: count}
    categories : list
        List of categories in order
    plot_type : str
        Type of plot for title (e.g., 'Cutflow', 'Sum_of_Weights')
    ylabel : str
        Y-axis label
    output_dir : str
        Output directory for the plot
    figsize : tuple
        Figure size (width, height)
    log_y : bool
        Use logarithmic y-axis
    format : str
        Output format (png, pdf, etc.)
    with_ratio : bool
        Create plots with ratio to initial category
        
    Returns:
    --------
    list
        List of paths to the saved plots
    """
    # Get categories and their counts for this sample
    sample_categories = []
    sample_counts = []
    
    for category in categories:
        if category in sample_data:
            sample_categories.append(category)
            sample_counts.append(sample_data[category])
    
    if not sample_categories:
        raise ValueError(f"No data found for sample {sample}")
    
    # Set plotting style
    plt.style.use(hep.style.CMS)
    
    # Get color based on plot type
    if plot_type.lower().startswith('cutflow'):
        color = CMS_BLUE
    else:  # Sum_of_Weights
        color = CMS_ORANGE
    
    saved_files = []
    
    # Helper function to create a single plot
    def create_plot(include_ratio=False):
        if include_ratio:
            # Create subplot with ratio
            fig, (ax_main, ax_ratio) = plt.subplots(2, 1, figsize=figsize, 
                                                   gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.05})
        else:
            fig, ax_main = plt.subplots(figsize=figsize)
            ax_ratio = None
        
        # Main plot
        x_pos = np.arange(len(sample_categories))
        bars = ax_main.bar(x_pos, sample_counts, alpha=0.7, color=color)
        
        # Customize main plot
        ax_main.set_ylabel(ylabel)
        ax_main.set_title(f'{plot_type} - {sample}')
        
        if not include_ratio:
            ax_main.set_xlabel('Category')
            ax_main.set_xticks(x_pos)
            ax_main.set_xticklabels(sample_categories, rotation=45, ha='right')
        else:
            # Remove x-axis labels for main plot when ratio is present
            ax_main.set_xticks(x_pos)
            ax_main.set_xticklabels([])
        
        if log_y:
            ax_main.set_yscale('log')
        
        # Add value labels on bars
        for bar, count in zip(bars, sample_counts):
            height = bar.get_height()
            ax_main.text(bar.get_x() + bar.get_width()/2., height,
                        f'{count:.0f}' if count >= 1 else f'{count:.2e}',
                        ha='center', va='bottom', fontsize=8)
        
        # CMS label
        hep.cms.label(ax=ax_main, loc=0)
        
        # Ratio plot
        if include_ratio and ax_ratio is not None:
            # Find initial category count for ratio calculation
            initial_count = None
            for i, cat in enumerate(sample_categories):
                if cat == 'initial':
                    initial_count = sample_counts[i]
                    break
            
            if initial_count is not None and initial_count > 0:
                ratios = [count / initial_count for count in sample_counts]
                
                # Create ratio bars
                ratio_bars = ax_ratio.bar(x_pos, ratios, alpha=0.7, color=color)
                
                # Customize ratio plot
                ax_ratio.set_xlabel('Category')
                ax_ratio.set_ylabel('Ratio to Initial')
                ax_ratio.set_xticks(x_pos)
                ax_ratio.set_xticklabels(sample_categories, rotation=45, ha='right')
                ax_ratio.axhline(y=1, color='black', linestyle='--', alpha=0.5)
                ax_ratio.set_ylim(0, max(1.1, max(ratios) * 1.1))
                
                # Add ratio labels
                for bar, ratio in zip(ratio_bars, ratios):
                    height = bar.get_height()
                    ax_ratio.text(bar.get_x() + bar.get_width()/2., height,
                                 f'{ratio:.3f}',
                                 ha='center', va='bottom', fontsize=8)
            else:
                # If no initial category found, show message
                ax_ratio.text(0.5, 0.5, 'No "initial" category found for ratio',
                             ha='center', va='center', transform=ax_ratio.transAxes)
                ax_ratio.set_xlabel('Category')
                ax_ratio.set_ylabel('Ratio to Initial')
        
        return fig
    
    # Create plot without ratio
    fig = create_plot(include_ratio=False)
    plt.tight_layout()
    
    # Save plot without ratio
    clean_sample = sample.replace('/', '_').replace('__', '_')
    filename = f"{plot_type.lower()}_{clean_sample}.{format}"
    filepath = os.path.join(output_dir, filename)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    saved_files.append(filepath)
    
    # Create plot with ratio (only for cutflow plots)
    if with_ratio and plot_type.lower().startswith('cutflow'):
        fig = create_plot(include_ratio=True)
        plt.tight_layout()
        
        # Save plot with ratio
        filename_ratio = f"{plot_type.lower()}_with_ratio_{clean_sample}.{format}"
        filepath_ratio = os.path.join(output_dir, filename_ratio)
        
        plt.savefig(filepath_ratio, dpi=300, bbox_inches='tight')
        plt.close()
        saved_files.append(filepath_ratio)
    
    return saved_files


def plot_cutflow_from_output(output: Dict, output_dir: str,
                           exclude_categories: Optional[List[str]] = None,
                           only_samples: Optional[List[str]] = None,
                           figsize: Tuple[float, float] = (10, 6),
                           log_y: bool = False, format: str = 'png') -> Dict[str, List[str]]:
    """
    Create cutflow plots from PocketCoffea processor output.
    
    Parameters:
    -----------
    output : dict
        PocketCoffea processor output dictionary
    output_dir : str
        Output directory for plots
    exclude_categories : list, optional
        Categories to exclude from plots
    only_samples : list, optional
        Only plot these samples
    figsize : tuple
        Figure size (width, height)
    log_y : bool
        Use logarithmic y-axis
    format : str
        Output format (png, pdf, etc.)
        
    Returns:
    --------
    dict
        Dictionary with 'cutflow' and 'sumw' keys, each containing list of saved file paths
    """
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract data structures
    cutflow = output.get('cutflow', {})
    sumw = output.get('sumw', {})
    datasets_metadata = output.get('datasets_metadata', {}).get('by_dataset', {})
    
    if not cutflow:
        raise ValueError("No cutflow data found in the input")
    
    # Get all categories, excluding specified ones
    exclude_categories = exclude_categories or []
    all_categories = list(cutflow.keys())
    categories = [cat for cat in all_categories if cat not in exclude_categories]
    
    if not categories:
        raise ValueError("No categories available after exclusions")
    
    # Aggregate data by sample
    cutflow_by_sample = aggregate_by_sample(cutflow, categories, datasets_metadata, only_samples)
    sumw_by_sample = aggregate_by_sample(sumw, categories, datasets_metadata, only_samples) if sumw else {}
    
    saved_files = {'cutflow': [], 'sumw': []}
    
    # Create cutflow plots
    if cutflow_by_sample:
        for sample, sample_data in cutflow_by_sample.items():
            try:
                filepaths = plot_sample_cutflow(
                    sample, sample_data, categories,
                    'Cutflow', 'Number of Events', output_dir,
                    figsize, log_y, format, with_ratio=True
                )
                saved_files['cutflow'].extend(filepaths)
            except ValueError as e:
                print(f"WARNING: {e}")
    
    # Create sumw plots
    if sumw_by_sample:
        for sample, sample_data in sumw_by_sample.items():
            try:
                filepaths = plot_sample_cutflow(
                    sample, sample_data, categories,
                    'Sum_of_Weights', 'Weighted Number of Events', output_dir,
                    figsize, log_y, format, with_ratio=False
                )
                saved_files['sumw'].extend(filepaths)
            except ValueError as e:
                print(f"WARNING: {e}")
    
    return saved_files


def print_cutflow_summary(output: Dict, exclude_categories: Optional[List[str]] = None,
                         only_samples: Optional[List[str]] = None) -> None:
    """
    Print a summary of the cutflow data.
    
    Parameters:
    -----------
    output : dict
        PocketCoffea processor output dictionary
    exclude_categories : list, optional
        Categories to exclude from summary
    only_samples : list, optional
        Only show these samples
    """
    cutflow = output.get('cutflow', {})
    sumw = output.get('sumw', {})
    datasets_metadata = output.get('datasets_metadata', {}).get('by_dataset', {})
    
    # Get categories
    exclude_categories = exclude_categories or []
    all_categories = list(cutflow.keys())
    categories = [cat for cat in all_categories if cat not in exclude_categories]
    
    # Aggregate data
    cutflow_by_sample = aggregate_by_sample(cutflow, categories, datasets_metadata, only_samples)
    sumw_by_sample = aggregate_by_sample(sumw, categories, datasets_metadata, only_samples) if sumw else {}
    
    print("Cutflow Summary:")
    print("===============")
    print(f"Categories: {categories}")
    print(f"Samples: {list(cutflow_by_sample.keys())}")
    
    print(f"\nCutflow (absolute number of events):")
    for sample in sorted(cutflow_by_sample.keys()):
        print(f"  {sample}:")
        for cat in categories:
            if cat in cutflow_by_sample[sample]:
                count = cutflow_by_sample[sample][cat]
                print(f"    {cat:>12}: {count:>12.0f}")
    
    if sumw_by_sample:
        print(f"\nSum of weights (weighted number of events):")
        for sample in sorted(sumw_by_sample.keys()):
            print(f"  {sample}:")
            for cat in categories:
                if cat in sumw_by_sample[sample]:
                    count = sumw_by_sample[sample][cat]
                    print(f"    {cat:>12}: {count:>12.2f}")
