#!/usr/bin/env python3

import sys
import click
from coffea.util import load
from pocket_coffea.utils.cutflow_utils import plot_cutflow_from_output, print_cutflow_summary

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
    - Cutflow (absolute number of events) with and without ratio plots
    - Sum of weights (weighted number of events)
    
    Uses CMS styling and creates one plot per sample, with all categories shown as bars.
    For cutflow plots, creates both versions: with and without ratio to initial category.
    """
    
    # Parse figure size
    try:
        figwidth, figheight = map(float, figsize.split(','))
    except:
        print("ERROR: Invalid figsize format. Use 'width,height' (e.g., '10,6')")
        sys.exit(1)
    
    # Load the coffea file
    print(f"Loading {input_file}...")
    try:
        output = load(input_file)
    except Exception as e:
        print(f"ERROR: Could not load {input_file}: {e}")
        sys.exit(1)
    
    # Convert click tuples to lists
    exclude_categories_list = list(exclude_categories) if exclude_categories else None
    only_samples_list = list(only_samples) if only_samples else None
    
    # Print summary
    print("\nCutflow Summary:")
    print("=" * 50)
    try:
        print_cutflow_summary(output, exclude_categories_list, only_samples_list)
    except Exception as e:
        print(f"Error in summary: {e}")
        sys.exit(1)
    
    if summary_only:
        return
    
    # Create plots using the enhanced utility function
    print(f"\nCreating plots...")
    print("=" * 50)
    try:
        saved_files = plot_cutflow_from_output(
            output, 
            output_dir, 
            exclude_categories_list, 
            only_samples_list,
            (figwidth, figheight), 
            log_y, 
            format
        )
        
        print(f"\n✓ Plotting completed!")
        print(f"Output directory: {output_dir}")
        print(f"Cutflow plots: {len(saved_files['cutflow'])}")
        print(f"Sum of weights plots: {len(saved_files['sumw'])}")
        
        if saved_files['cutflow']:
            print("\nCutflow plots (includes both regular and ratio versions):")
            for f in saved_files['cutflow']:
                print(f"  - {f}")
        
        if saved_files['sumw']:
            print("\nSum of weights plots:")
            for f in saved_files['sumw']:
                print(f"  - {f}")
        else:
            print("\nNo sum of weights data found")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    plot_cutflow()
