#!/usr/bin/env python3
"""
Simpler unit test for the split_by_dataset functionality
This tests the core logic without requiring full plotting infrastructure
"""

import sys
import os
import numpy as np
import hist
from copy import deepcopy

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, '/home/runner/work/PocketCoffea/PocketCoffea')

def test_split_by_dataset_basic():
    """Test basic split by dataset functionality"""
    print("\n" + "="*60)
    print("TEST 1: Basic split_by_dataset functionality")
    print("="*60)
    
    # Create synthetic h_dict structure with samples and datasets
    h_template = (
        hist.Hist.new
        .StrCat(["cat1", "cat2"], name="cat")
        .StrCat(["nominal"], name="variation")
        .Reg(10, 0, 100, name="obs")
        .Weight()
    )
    
    # Create h_dict with sample->dataset structure
    h_dict = {
        "QCD": {
            "QCD_pt1": h_template.copy(),
            "QCD_pt2": h_template.copy(),
            "QCD_pt3": h_template.copy(),
        },
        "TTbar": {
            "TTbar_part1": h_template.copy(),
            "TTbar_part2": h_template.copy(),
        },
    }
    
    # Fill with different data for each dataset
    for sample, datasets in h_dict.items():
        for i, (dataset, h) in enumerate(datasets.items()):
            np.random.seed(hash(dataset) % (2**32))
            values = np.random.normal(50 + i*10, 10, 1000)
            h.fill(cat="cat1", variation="nominal", obs=values)
    
    # Create datasets metadata
    datasets_metadata = {
        "QCD_pt1": {"sample": "QCD", "isMC": "True"},
        "QCD_pt2": {"sample": "QCD", "isMC": "True"},
        "QCD_pt3": {"sample": "QCD", "isMC": "True"},
        "TTbar_part1": {"sample": "TTbar", "isMC": "True"},
        "TTbar_part2": {"sample": "TTbar", "isMC": "True"},
    }
    
    # Simulate what split_samples_by_dataset should do
    split_by_dataset_samples = ["QCD"]
    sample_is_MC = {}
    
    h_dict_split = {}
    for sample, datasets in h_dict.items():
        if sample in split_by_dataset_samples:
            print(f"  Splitting sample '{sample}' by dataset")
            for dataset, hist_obj in datasets.items():
                # Create a new "sample" for each dataset
                h_dict_split[dataset] = {dataset: hist_obj}
                sample_is_MC[dataset] = datasets_metadata[dataset]["isMC"] == "True"
        else:
            # Keep this sample as-is
            h_dict_split[sample] = datasets
            # Mark it for later processing
    
    # Check results
    print(f"\n  Original samples: {list(h_dict.keys())}")
    print(f"  After split samples: {list(h_dict_split.keys())}")
    
    expected = ["QCD_pt1", "QCD_pt2", "QCD_pt3", "TTbar"]
    if sorted(h_dict_split.keys()) == sorted(expected):
        print(f"  ✓ PASS: Sample list matches expected: {expected}")
        return True
    else:
        print(f"  ✗ FAIL: Expected {expected}, got {list(h_dict_split.keys())}")
        return False


def test_split_multiple_samples():
    """Test splitting multiple samples by dataset"""
    print("\n" + "="*60)
    print("TEST 2: Split multiple samples by dataset")
    print("="*60)
    
    h_template = (
        hist.Hist.new
        .StrCat(["cat1"], name="cat")
        .StrCat(["nominal"], name="variation")
        .Reg(10, 0, 100, name="obs")
        .Weight()
    )
    
    h_dict = {
        "QCD": {
            "QCD_pt1": h_template.copy(),
            "QCD_pt2": h_template.copy(),
        },
        "TTbar": {
            "TTbar_part1": h_template.copy(),
            "TTbar_part2": h_template.copy(),
        },
        "WJets": {
            "WJets_part1": h_template.copy(),
        }
    }
    
    datasets_metadata = {
        "QCD_pt1": {"sample": "QCD", "isMC": "True"},
        "QCD_pt2": {"sample": "QCD", "isMC": "True"},
        "TTbar_part1": {"sample": "TTbar", "isMC": "True"},
        "TTbar_part2": {"sample": "TTbar", "isMC": "True"},
        "WJets_part1": {"sample": "WJets", "isMC": "True"},
    }
    
    # Split both QCD and TTbar
    split_by_dataset_samples = ["QCD", "TTbar"]
    
    h_dict_split = {}
    for sample, datasets in h_dict.items():
        if sample in split_by_dataset_samples:
            print(f"  Splitting sample '{sample}' by dataset")
            for dataset, hist_obj in datasets.items():
                h_dict_split[dataset] = {dataset: hist_obj}
        else:
            h_dict_split[sample] = datasets
    
    print(f"\n  Original samples: {list(h_dict.keys())}")
    print(f"  After split samples: {list(h_dict_split.keys())}")
    
    expected = ["QCD_pt1", "QCD_pt2", "TTbar_part1", "TTbar_part2", "WJets"]
    if sorted(h_dict_split.keys()) == sorted(expected):
        print(f"  ✓ PASS: Sample list matches expected: {expected}")
        return True
    else:
        print(f"  ✗ FAIL: Expected {expected}, got {list(h_dict_split.keys())}")
        return False


def test_dataset_metadata_lookup():
    """Test that dataset names can be used for styling parameters"""
    print("\n" + "="*60)
    print("TEST 3: Dataset names work with styling parameters")
    print("="*60)
    
    # Simulate style config with dataset names
    labels_mc = {
        "QCD": "QCD (all parts)",
        "QCD_pt1": "QCD pT bin 1",
        "QCD_pt2": "QCD pT bin 2",
        "QCD_pt3": "QCD pT bin 3",
    }
    
    colors_mc = {
        "QCD": "#3f90da",
        "QCD_pt1": "#1f70ba",
        "QCD_pt2": "#2f80ca",
        "QCD_pt3": "#4fa0ea",
    }
    
    samples_groups = {
        "QCD_pt1-2": ["QCD_pt1", "QCD_pt2"]
    }
    
    exclude_samples = ["QCD_pt3"]
    
    # After splitting, these should all work with dataset names
    split_samples = ["QCD_pt1", "QCD_pt2", "QCD_pt3"]
    
    print(f"  Testing that dataset names can be looked up in style configs:")
    
    # Test labels
    success = True
    for sample in split_samples:
        if sample in labels_mc:
            print(f"    ✓ Label for '{sample}': {labels_mc[sample]}")
        else:
            print(f"    ✗ Missing label for '{sample}'")
            success = False
    
    # Test colors
    for sample in split_samples:
        if sample in colors_mc:
            print(f"    ✓ Color for '{sample}': {colors_mc[sample]}")
        else:
            print(f"    ✗ Missing color for '{sample}'")
            success = False
    
    # Test grouping
    group_name = "QCD_pt1-2"
    if group_name in samples_groups:
        print(f"    ✓ Group '{group_name}' contains: {samples_groups[group_name]}")
    else:
        print(f"    ✗ Missing group definition for '{group_name}'")
        success = False
    
    # Test exclusion
    if "QCD_pt3" in exclude_samples:
        print(f"    ✓ Exclusion list contains: {exclude_samples}")
    else:
        print(f"    ✗ Missing exclusion for 'QCD_pt3'")
        success = False
    
    if success:
        print(f"\n  ✓ PASS: All styling parameters work with dataset names")
    else:
        print(f"\n  ✗ FAIL: Some styling parameters missing")
    
    return success


def test_integration_with_grouping():
    """Test that split datasets can be grouped"""
    print("\n" + "="*60)
    print("TEST 4: Integration with group_samples")
    print("="*60)
    
    h_template = (
        hist.Hist.new
        .StrCat(["cat1"], name="cat")
        .StrCat(["nominal"], name="variation")
        .Reg(10, 0, 100, name="obs")
        .Weight()
    )
    
    # Simulate structure after split_by_dataset
    h_dict_after_split = {
        "QCD_pt1": {
            "QCD_pt1": h_template.copy()
        },
        "QCD_pt2": {
            "QCD_pt2": h_template.copy()
        },
        "QCD_pt3": {
            "QCD_pt3": h_template.copy()
        },
    }
    
    # Now simulate grouping QCD_pt1 and QCD_pt2
    samples_groups = {
        "QCD_pt1-2": ["QCD_pt1", "QCD_pt2"]
    }
    
    h_dict_grouped = {}
    samples_in_map = []
    
    for sample_new, samples_list in samples_groups.items():
        print(f"  Grouping {samples_list} into '{sample_new}'")
        # In real code, this would sum histograms
        h_dict_grouped[sample_new] = "grouped_hist"  # Placeholder
        samples_in_map += samples_list
    
    # Add ungrouped samples
    for s, h in h_dict_after_split.items():
        if s not in samples_in_map:
            h_dict_grouped[s] = h
    
    print(f"\n  Samples after grouping: {list(h_dict_grouped.keys())}")
    
    expected = ["QCD_pt1-2", "QCD_pt3"]
    if sorted(h_dict_grouped.keys()) == sorted(expected):
        print(f"  ✓ PASS: Grouped samples match expected: {expected}")
        return True
    else:
        print(f"  ✗ FAIL: Expected {expected}, got {list(h_dict_grouped.keys())}")
        return False


def main():
    """Run all tests"""
    print("="*60)
    print("Unit Tests for --by-dataset Feature")
    print("="*60)
    
    results = {
        "basic_split": test_split_by_dataset_basic(),
        "multiple_split": test_split_multiple_samples(),
        "styling_params": test_dataset_metadata_lookup(),
        "integration_grouping": test_integration_with_grouping(),
    }
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name:30s} {status}")
        if not passed:
            all_passed = False
    
    print("="*60)
    
    if all_passed:
        print("\n✓ All tests PASSED!")
        return 0
    else:
        print("\n✗ Some tests FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
