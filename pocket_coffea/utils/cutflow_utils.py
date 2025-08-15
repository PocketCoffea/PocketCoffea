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
from pocket_coffea.parameters.defaults import get_default_parameters

CMS_BLUE = COLOR_ALIASES['CMS_blue']
CMS_ORANGE = COLOR_ALIASES['CMS_orange']


def get_luminosity_text(year: str) -> str:
    """
    Get luminosity text for a given year from plotting_style defaults.
    Similar to what's done in plot_utils.py for the PlotManager class.
    
    Parameters:
    -----------
    year : str
        The year string (e.g., '2016_PreVFP', '2017', '2018', etc.)
        
    Returns:
    --------
    str
        Formatted luminosity text (e.g., "41.5 fb^{-1}")
    """
    # Get plotting style defaults like in plot_utils.py line 28
    plotting_style_defaults = get_default_parameters()["plotting_style"]
    
    # Extract luminosity values from plot_upper_label
    plot_upper_label = plotting_style_defaults.get('plot_upper_label', {})
    by_year = plot_upper_label.get('by_year', {})
    
    if year in by_year:
        # The format is like "${pico_to_femto:${lumi.picobarns.2017.tot}}"
        # We need to extract the actual luminosity value
        return f"$\mathcal{{L}}$ = {by_year[year]:.2f}/fb"
    elif year == 'all':
        return "13.6 TeV"
    else:
        raise ValueError(f"Year '{year}' not found in plotting style defaults.")

def aggregate_by_sample(data_dict: Dict, categories: List[str], 
                       datasets_metadata: Dict, only_samples: Optional[List[str]] = None,
                       separate_years: bool = False) -> Dict:
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
    separate_years : bool
        If True, create separate entries for each year (sample_year)
        If False, aggregate all years together (sample)
        
    Returns:
    --------
    dict
        Aggregated data with structure {sample[_year]: {category: count}}
    """
    sample_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    
    for category in categories:
        if category not in data_dict:
            continue
            
        for dataset, counts in data_dict[category].items():
            # Get sample name and year from metadata
            if datasets_metadata and dataset in datasets_metadata:
                sample = datasets_metadata[dataset]['sample']
                year = datasets_metadata[dataset].get('year', '')
            else:
                # Fallback: use dataset name as sample name
                sample = dataset
                year = ''
            
            # Skip if only_samples is specified and this sample is not in the list
            if only_samples and sample not in only_samples:
                continue
            
            # Create sample key
            if separate_years:
                era = year
            else:
                era = 'all'
            if isinstance(counts, dict):
                # This is for categories with subsamples
                for subsample, count in counts.items():
                    sample_data[era][subsample][category] += count
            else:
                # This is for simple categories (initial, skim, presel)
                sample_data[era][sample][category] += counts

    return sample_data


def plot_sample_cutflow(sample: str, sample_data: Dict, year: str, categories: List[str],
                       plot_type: str, ylabel: str, output_dir: str,
                       figsize: Tuple[float, float] = (10, 6), 
                       log_y: bool = False, format: str = 'png',
                       with_ratio: bool = False, datasets_metadata: Optional[Dict] = None) -> List[str]:
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
    datasets_metadata : dict, optional
        Metadata dictionary to determine if sample is MC or data
        
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
    
    # Determine if sample is MC or data for CMS label
    is_mc = True  # Default to MC
    if datasets_metadata:
        # Look for any dataset that matches this sample
        for dataset_name, metadata in datasets_metadata.items():
            if (metadata.get('sample') == sample or 
                dataset_name.startswith(sample) or 
                sample in dataset_name):
                is_mc = metadata.get('isMC', 'True') == 'True'
                break
        # Additional check: if sample name contains 'DATA' it's likely data
        if 'DATA' in sample.upper():
            is_mc = False
    
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
                                                   gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.2})
        else:
            fig, ax_main = plt.subplots(figsize=figsize)
            ax_ratio = None
        
        # Main plot
        x_pos = np.arange(len(sample_categories))
        bars = ax_main.bar(x_pos, sample_counts, alpha=0.7, color=color)
        
        # Customize main plot with reduced fontsize (30% reduction from default)
        title_fontsize = 16
        label_fontsize = 12
        tick_fontsize = 12
        
        ax_main.set_ylabel(ylabel, fontsize=label_fontsize)
        ax_main.set_title(f'{sample}', fontsize=title_fontsize)
        
        # Remove minor ticks
        ax_main.minorticks_off()
        
        if not include_ratio:
            ax_main.set_xlabel('Category', fontsize=label_fontsize)
            ax_main.set_xticks(x_pos)
            ax_main.set_xticklabels(sample_categories, rotation=45, ha='right', fontsize=tick_fontsize)
        else:
            # Remove x-axis labels for main plot when ratio is present
            ax_main.set_xticks(x_pos)
            ax_main.set_xticklabels([])
        
        # Set y-tick label fontsize
        ax_main.tick_params(axis='y', labelsize=tick_fontsize)
        
        if log_y:
            ax_main.set_yscale('log')
        
        # Add value labels on bars
        for bar, count in zip(bars, sample_counts):
            height = bar.get_height()
            ax_main.text(bar.get_x() + bar.get_width()/2., height,
                        f'{count:.0f}' if count >= 1 else f'{count:.2e}',
                        ha='center', va='bottom', fontsize=8)
        
        # CMS label - use "Simulation" for MC, "Preliminary" for data
        if is_mc:
            hep.cms.text("Simulation Preliminary", ax=ax_main, fontsize=title_fontsize)
        else:
            hep.cms.text("Preliminary", ax=ax_main, fontsize=title_fontsize)
        
        # Add luminosity text in top right if year is available
        if year:
            lumi_text = get_luminosity_text(year)
            hep.cms.lumitext(text=lumi_text, ax=ax_main, fontsize=title_fontsize)
        
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
                
                # Customize ratio plot with reduced fontsize and remove minor ticks
                ax_ratio.set_xlabel('Category', fontsize=label_fontsize)
                ax_ratio.set_ylabel('Ratio to Initial', fontsize=label_fontsize)
                ax_ratio.set_xticks(x_pos)
                ax_ratio.set_xticklabels(sample_categories, rotation=45, ha='right', fontsize=tick_fontsize)
                ax_ratio.tick_params(axis='y', labelsize=tick_fontsize)
                ax_ratio.minorticks_off()
                ax_ratio.axhline(y=1, color='black', linestyle='--', alpha=0.5)
                ax_ratio.set_ylim(0, max(1.2, max(ratios) * 1.2))
                
                # Add ratio labels
                for bar, ratio in zip(ratio_bars, ratios):
                    height = bar.get_height()
                    ax_ratio.text(bar.get_x() + bar.get_width()/2., height,
                                 f'{ratio:.3f}',
                                 ha='center', va='bottom', fontsize=8)
            else:
                # If no initial category found, show message
                ax_ratio.text(0.5, 0.5, 'No "initial" category found for ratio',
                             ha='center', va='center', transform=ax_ratio.transAxes, fontsize=label_fontsize)
                ax_ratio.set_xlabel('Category', fontsize=label_fontsize)
                ax_ratio.set_ylabel('Ratio to Initial', fontsize=label_fontsize)
                ax_ratio.minorticks_off()
        
        return fig
    
    # Create plot without ratio
    fig = create_plot(include_ratio=False)
    plt.tight_layout()
    
    # Save plot without ratio
    clean_sample = sample.replace('/', '_').replace('__', '_')
    filename = f"{plot_type.lower()}_{clean_sample}_{year}.{format}"
    filepath = os.path.join(output_dir, filename)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    saved_files.append(filepath)
    
    # Create plot with ratio (only for cutflow plots)
    if with_ratio and plot_type.lower().startswith('cutflow'):
        fig = create_plot(include_ratio=True)
        plt.tight_layout()
        
        # Save plot with ratio
        filename_ratio = f"{plot_type.lower()}_with_ratio_{clean_sample}_{year}.{format}"
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
    
    # Aggregate data by sample - both combined and year-separated
    cutflow_by_sample_combined = aggregate_by_sample(cutflow, categories, datasets_metadata, only_samples, separate_years=False)
    cutflow_by_sample_separated = aggregate_by_sample(cutflow, categories, datasets_metadata, only_samples, separate_years=True)
    
    sumw_by_sample_combined = {}
    sumw_by_sample_separated = {}
    if sumw:
        sumw_by_sample_combined = aggregate_by_sample(sumw, categories, datasets_metadata, only_samples, separate_years=False)
        sumw_by_sample_separated = aggregate_by_sample(sumw, categories, datasets_metadata, only_samples, separate_years=True)
    
    saved_files = {'cutflow': [], 'sumw': []}
    
    # Create cutflow plots - year-separated
    if cutflow_by_sample_separated:
        for year, cutflow_by_sample in cutflow_by_sample_separated.items():
            for sample, sample_data in cutflow_by_sample.items():
                try:
                    filepaths = plot_sample_cutflow(
                        sample, sample_data, year, categories,
                        'Cutflow', 'Number of Events', output_dir,
                        figsize, log_y, format, with_ratio=True, datasets_metadata=datasets_metadata
                    )
                    saved_files['cutflow'].extend(filepaths)
                except ValueError as e:
                    print(f"WARNING: {e}")
    
    # Create cutflow plots - combined (with "_all" suffix)
    if cutflow_by_sample_combined:
        for sample, sample_data in cutflow_by_sample_combined["all"].items():
            try:
                filepaths = plot_sample_cutflow(
                    sample, sample_data, "all", categories,
                    'Cutflow', 'Number of Events', output_dir,
                    figsize, log_y, format, with_ratio=True, datasets_metadata=datasets_metadata
                )
                saved_files['cutflow'].extend(filepaths)
            except ValueError as e:
                print(f"WARNING: {e}")
    
    # Create sumw plots - year-separated
    if sumw_by_sample_separated:
        for year, sumw_by_sample in sumw_by_sample_separated.items():
            for sample, sample_data in sumw_by_sample.items():
                try:
                    filepaths = plot_sample_cutflow(
                        sample, sample_data, year, categories,
                        'Sum_of_Weights', 'Weighted Number of Events', output_dir,
                        figsize, log_y, format, with_ratio=False, datasets_metadata=datasets_metadata
                    )
                    saved_files['sumw'].extend(filepaths)
                except ValueError as e:
                    print(f"WARNING: {e}")
    
    # Create sumw plots - combined (with "_all" suffix)
    if sumw_by_sample_combined:
        for sample, sample_data in sumw_by_sample_combined["all"].items():
            try:
                filepaths = plot_sample_cutflow(
                    sample, sample_data, "all", categories,
                    'Sum_of_Weights', 'Weighted Number of Events', output_dir,
                    figsize, log_y, format, with_ratio=False, datasets_metadata=datasets_metadata
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
    cutflow_by_sample = aggregate_by_sample(cutflow, categories, datasets_metadata, only_samples, separate_years=False)
    sumw_by_sample = aggregate_by_sample(sumw, categories, datasets_metadata, only_samples, separate_years=False) if sumw else {}
    
    print("Cutflow Summary:")
    print("===============")
    print(f"Categories: {categories}")
    print(f"Samples: {list(cutflow_by_sample['all'].keys())}")

    print(f"\nCutflow (absolute number of events):")
    for sample in sorted(cutflow_by_sample["all"].keys()):
        print(f"  {sample}:")
        for cat in categories:
            if cat in cutflow_by_sample["all"][sample]:
                count = cutflow_by_sample["all"][sample][cat]
                print(f"    {cat:>12}: {count:>12.0f}")
    
    if sumw_by_sample:
        print(f"\nSum of weights (weighted number of events):")
        for sample in sorted(sumw_by_sample["all"].keys()):
            print(f"  {sample}:")
            for cat in categories:
                if cat in sumw_by_sample["all"][sample]:
                    count = sumw_by_sample["all"][sample][cat]
                    print(f"    {cat:>12}: {count:>12.2f}")
