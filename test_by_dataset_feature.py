#!/usr/bin/env python3
"""
Test script for the --by-dataset feature in make_plots.py

This script creates synthetic test data and tests the new --by-dataset functionality
with various parameter combinations: labels_mc, samples_groups, colors_mc, and exclude_samples.
"""

import os
import sys
import tempfile
import shutil
import numpy as np
import hist
from coffea.util import save
from omegaconf import OmegaConf

def create_test_data(output_dir):
    """
    Create synthetic test data with multiple samples, each containing multiple datasets.
    Structure:
    - QCD sample with 3 datasets: QCD_pt1, QCD_pt2, QCD_pt3
    - TTToSemiLeptonic sample with 2 datasets: TTToSemiLeptonic_part1, TTToSemiLeptonic_part2
    - WJets sample with 2 datasets: WJets_part1, WJets_part2
    - Data sample with 1 dataset: Data_2018
    """
    
    # Create histogram structure
    h_template = (
        hist.Hist.new
        .StrCat(["cat1", "cat2"], name="cat")
        .StrCat(["nominal", "syst_up", "syst_down"], name="variation")
        .Reg(50, 0, 500, name="observable", label="Observable [GeV]")
        .Weight()
    )
    
    variables = {
        "pt": h_template,
        "mass": hist.Hist.new
        .StrCat(["cat1", "cat2"], name="cat")
        .StrCat(["nominal", "syst_up", "syst_down"], name="variation")
        .Reg(30, 0, 300, name="mass", label="Mass [GeV]")
        .Weight()
    }
    
    # Define samples and their datasets
    samples_config = {
        "QCD": ["QCD_pt1", "QCD_pt2", "QCD_pt3"],
        "TTToSemiLeptonic": ["TTToSemiLeptonic_part1", "TTToSemiLeptonic_part2"],
        "WJets": ["WJets_part1", "WJets_part2"],
        "Data": ["Data_2018"]
    }
    
    # Create datasets metadata
    datasets_metadata = {
        "by_datataking_period": {
            "2018": samples_config
        },
        "by_dataset": {}
    }
    
    # Create histograms for each variable, sample, and dataset
    hist_objs = {}
    
    for var_name, h_template in variables.items():
        hist_objs[var_name] = {}
        
        for sample, datasets in samples_config.items():
            hist_objs[var_name][sample] = {}
            
            is_data = (sample == "Data")
            
            for i, dataset in enumerate(datasets):
                # Create a new histogram for each dataset
                h = h_template.copy()
                
                # Fill with synthetic data (different distributions for different samples)
                np.random.seed(hash(dataset) % (2**32))  # Reproducible random data
                
                for cat in ["cat1", "cat2"]:
                    for var in ["nominal", "syst_up", "syst_down"]:
                        n_events = 10000 + i * 1000  # Different amounts per dataset
                        
                        if sample == "QCD":
                            # Exponential distribution for QCD
                            mean = 50 + i * 20
                            values = np.random.exponential(mean, n_events)
                        elif sample == "TTToSemiLeptonic":
                            # Gaussian distribution for ttbar
                            mean = 150 + i * 10
                            values = np.random.normal(mean, 30, n_events)
                        elif sample == "WJets":
                            # Another Gaussian for W+jets
                            mean = 80 + i * 10
                            values = np.random.normal(mean, 20, n_events)
                        else:  # Data
                            # Mix of distributions
                            values = np.concatenate([
                                np.random.exponential(50, 3000),
                                np.random.normal(150, 30, 5000),
                                np.random.normal(80, 20, 2000)
                            ])
                        
                        # Apply systematic shift
                        if var == "syst_up":
                            values = values * 1.05
                        elif var == "syst_down":
                            values = values * 0.95
                        
                        # Clip to histogram range
                        axis_name = "observable" if var_name == "pt" else "mass"
                        axis = h.axes[axis_name]
                        values = np.clip(values, axis.edges[0], axis.edges[-1])
                        
                        # Fill histogram
                        weights = np.ones_like(values)
                        h.fill(cat=cat, variation=var, **{axis_name: values}, weight=weights)
                
                hist_objs[var_name][sample][dataset] = h
                
                # Add dataset metadata
                datasets_metadata["by_dataset"][dataset] = {
                    "sample": sample,
                    "isMC": "False" if is_data else "True",
                    "year": "2018"
                }
    
    # Create accumulator structure
    accumulator = {
        "variables": hist_objs,
        "datasets_metadata": datasets_metadata
    }
    
    # Save to file
    output_file = os.path.join(output_dir, "output_merged.coffea")
    save(accumulator, output_file)
    
    print(f"Created test data at: {output_file}")
    return output_file


def create_plotting_config(output_dir):
    """
    Create a comprehensive plotting configuration file with all the parameters
    that should work with the --by-dataset feature.
    """
    
    config = {
        "plotting_style": {
            "collapse_datasets": True,
            "fontsize": 22,
            "fontsize_legend": 18,
            "experiment_label_loc": 2,
            
            # Test labels_mc - should work for dataset names when using --by-dataset
            "labels_mc": {
                "QCD": "QCD (all parts)",
                "QCD_pt1": "QCD pT bin 1",
                "QCD_pt2": "QCD pT bin 2",
                "QCD_pt3": "QCD pT bin 3",
                "TTToSemiLeptonic": "t#bar{t} (semileptonic)",
                "TTToSemiLeptonic_part1": "t#bar{t} part 1",
                "TTToSemiLeptonic_part2": "t#bar{t} part 2",
                "WJets": "W+jets",
                "WJets_part1": "W+jets part 1",
                "WJets_part2": "W+jets part 2",
            },
            
            # Test samples_groups - should work for dataset names when using --by-dataset
            "samples_groups": {
                "QCD_pt1-2": ["QCD_pt1", "QCD_pt2"],  # Group two QCD datasets
            },
            
            # Test colors_mc - should work for dataset names when using --by-dataset
            "colors_mc": {
                "QCD": "#3f90da",
                "QCD_pt1": "#1f70ba",
                "QCD_pt2": "#2f80ca",
                "QCD_pt3": "#4fa0ea",
                "QCD_pt1-2": "#2f80ca",
                "TTToSemiLeptonic": "#ffa90e",
                "TTToSemiLeptonic_part1": "#df890e",
                "TTToSemiLeptonic_part2": "#ffb92e",
                "WJets": "#bd1f01",
                "WJets_part1": "#9d0f01",
                "WJets_part2": "#cd2f11",
            },
            
            # Test exclude_samples - should work for dataset names when using --by-dataset
            # We won't exclude anything in the base config, but will test it separately
            
            "opts_figure": {
                "datamc": {"figsize": [12, 9]},
                "datamc_ratio": {
                    "figsize": [12, 12],
                    "gridspec_kw": {"height_ratios": [3, 1]},
                    "sharex": True
                }
            },
            
            "opts_data": {
                "color": "black",
                "elinewidth": 1,
                "label": "Data",
                "linestyle": "solid",
                "linewidth": 0,
                "marker": ".",
                "markersize": 5.0
            },
            
            "opts_mc": {
                "histtype": "fill",
                "stack": True,
                "flow": "sum",
                "edges": False
            },
            
            "opts_sig": {
                "histtype": "step",
                "stack": False,
                "flow": "sum",
                "edges": True,
                "linestyle": "solid",
                "linewidth": 1
            },
            
            "opts_unc": {
                "total": {
                    "color": [0., 0., 0., 0.4],
                    "facecolor": [0., 0., 0., 0.],
                    "hatch": "////",
                    "linewidth": 0,
                    "step": "post",
                    "zorder": 2
                }
            },
            
            "categorical_axes_data": {
                "year": "years",
                "cat": "categories"
            },
            
            "categorical_axes_mc": {
                "year": "years",
                "cat": "categories",
                "variation": "variations"
            },
            
            "plot_upper_label": {
                "by_year": {
                    "2018": "59.7"
                }
            }
        }
    }
    
    config_file = os.path.join(output_dir, "parameters_dump.yaml")
    OmegaConf.save(config, config_file)
    
    print(f"Created config at: {config_file}")
    return config_file


def run_test(test_name, test_dir, additional_args=None):
    """
    Run a single test case
    """
    print(f"\n{'='*60}")
    print(f"Running test: {test_name}")
    print(f"{'='*60}")
    
    output_plots_dir = os.path.join(test_dir, f"plots_{test_name.replace(' ', '_')}")
    
    cmd_parts = [
        "python",
        "-m", "pocket_coffea.scripts.plot.make_plots",
        "--input-dir", test_dir,
        "--outputdir", output_plots_dir,
        "--overwrite",
        "-v", "2"
    ]
    
    if additional_args:
        cmd_parts.extend(additional_args)
    
    cmd = " ".join(cmd_parts)
    print(f"Command: {cmd}")
    
    import subprocess
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    if result.returncode != 0:
        print(f"STDERR:\n{result.stderr}")
        print(f"STDOUT:\n{result.stdout}")
        return False
    else:
        print(f"SUCCESS! Plots saved to: {output_plots_dir}")
        
        # Check that some plots were created
        if os.path.exists(output_plots_dir):
            plot_files = []
            for root, dirs, files in os.walk(output_plots_dir):
                plot_files.extend([f for f in files if f.endswith('.png')])
            print(f"Created {len(plot_files)} plot files")
            if len(plot_files) > 0:
                print(f"Example plots: {plot_files[:3]}")
        return True


def main():
    """
    Main test function that runs all test cases
    """
    
    print("="*60)
    print("Testing --by-dataset feature for make_plots.py")
    print("="*60)
    
    # Create temporary directory for test
    test_dir = tempfile.mkdtemp(prefix="test_by_dataset_")
    print(f"\nTest directory: {test_dir}")
    
    try:
        # Create test data
        print("\n1. Creating synthetic test data...")
        create_test_data(test_dir)
        
        # Create plotting config
        print("\n2. Creating plotting configuration...")
        create_plotting_config(test_dir)
        
        # Run tests
        results = {}
        
        # Test 1: Baseline - no --by-dataset (default behavior)
        results["baseline"] = run_test(
            "Baseline (no split by dataset)",
            test_dir,
            []
        )
        
        # Test 2: Split QCD by dataset
        results["split_qcd"] = run_test(
            "Split QCD by dataset",
            test_dir,
            ["--by-dataset", "QCD"]
        )
        
        # Test 3: Split multiple samples by dataset
        results["split_multiple"] = run_test(
            "Split QCD and TTToSemiLeptonic by dataset",
            test_dir,
            ["--by-dataset", "QCD", "--by-dataset", "TTToSemiLeptonic"]
        )
        
        # Test 4: Split with exclude_samples (exclude one QCD dataset)
        config_exclude = os.path.join(test_dir, "config_exclude.yaml")
        OmegaConf.save({
            "plotting_style": {
                "exclude_samples": ["QCD_pt3"]
            }
        }, config_exclude)
        
        results["exclude_dataset"] = run_test(
            "Split QCD and exclude QCD_pt3 dataset",
            test_dir,
            ["--by-dataset", "QCD", "-op", config_exclude]
        )
        
        # Test 5: Split with samples_groups (group QCD_pt1 and QCD_pt2)
        # This tests that after splitting, datasets can be grouped
        results["group_datasets"] = run_test(
            "Split QCD and group QCD_pt1-2",
            test_dir,
            ["--by-dataset", "QCD"]
        )
        
        # Test 6: Test that colors and labels work with dataset names
        # This is already tested in the config, but let's verify
        results["labels_colors"] = run_test(
            "Split with custom labels and colors",
            test_dir,
            ["--by-dataset", "QCD", "--by-dataset", "WJets"]
        )
        
        # Print summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        
        all_passed = True
        for test_name, passed in results.items():
            status = "✓ PASSED" if passed else "✗ FAILED"
            print(f"{test_name:40s} {status}")
            if not passed:
                all_passed = False
        
        print("="*60)
        
        if all_passed:
            print("\n✓ All tests PASSED!")
            print(f"\nTest outputs are in: {test_dir}")
            print("You can inspect the plots to verify the feature works correctly.")
            return 0
        else:
            print("\n✗ Some tests FAILED!")
            print(f"\nTest outputs are in: {test_dir}")
            return 1
            
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Optionally clean up
    # shutil.rmtree(test_dir)


if __name__ == "__main__":
    sys.exit(main())
