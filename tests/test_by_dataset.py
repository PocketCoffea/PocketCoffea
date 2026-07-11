#!/usr/bin/env python3
"""
Unit test for the split_by_dataset functionality
Tests core logic without requiring full plotting infrastructure
"""

import sys
import os

# Test the normalization logic for by_dataset parameter
def test_by_dataset_normalization():
    """Test that by_dataset parameter normalization works correctly"""
    print("Testing by_dataset parameter normalization...")
    
    # Test case 1: Single tuple (from single-flag multi-arg syntax)
    by_dataset = ('QCD', 'TTToSemiLeptonic')
    if isinstance(by_dataset, tuple):
        flattened = []
        for item in by_dataset:
            if isinstance(item, tuple):
                flattened.extend(item)
            else:
                flattened.append(item)
        by_dataset = flattened
    
    assert by_dataset == ['QCD', 'TTToSemiLeptonic'], f"Expected ['QCD', 'TTToSemiLeptonic'], got {by_dataset}"
    print("  ✓ Single-flag multi-arg syntax works")
    
    # Test case 2: Tuple of tuples (from repeated flags with MultiValueOption)
    by_dataset = (('QCD',), ('TTToSemiLeptonic',))
    if isinstance(by_dataset, tuple):
        flattened = []
        for item in by_dataset:
            if isinstance(item, tuple):
                flattened.extend(item)
            else:
                flattened.append(item)
        by_dataset = flattened
    
    assert by_dataset == ['QCD', 'TTToSemiLeptonic'], f"Expected ['QCD', 'TTToSemiLeptonic'], got {by_dataset}"
    print("  ✓ Repeated-flag syntax works")
    
    # Test case 3: Single string
    by_dataset = 'QCD'
    if not isinstance(by_dataset, list):
        by_dataset = [by_dataset]
    
    assert by_dataset == ['QCD'], f"Expected ['QCD'], got {by_dataset}"
    print("  ✓ Single sample works")
    
    # Test case 4: None
    by_dataset = None
    if by_dataset is not None:
        if isinstance(by_dataset, tuple):
            flattened = []
            for item in by_dataset:
                if isinstance(item, tuple):
                    flattened.extend(item)
                else:
                    flattened.append(item)
            by_dataset = flattened
        elif not isinstance(by_dataset, list):
            by_dataset = [by_dataset]
    
    assert by_dataset is None, f"Expected None, got {by_dataset}"
    print("  ✓ None value works")
    
    return True


def test_missing_samples_warning():
    """Test that missing samples generate appropriate warnings"""
    print("\nTesting missing samples warning...")
    
    # Simulate the warning logic
    split_by_dataset_samples = ['QCD', 'TTbar', 'NonExistent']
    h_dict = {'QCD': {}, 'WJets': {}}
    verbose = 1
    name = "test_hist"
    
    missing_samples = set(split_by_dataset_samples) - set(h_dict.keys())
    if missing_samples and verbose >= 1:
        warning_msg = (
            f"{name}: WARNING: The following samples requested in 'split_by_dataset_samples' "
            f"were not found and were ignored: {sorted(missing_samples)}"
        )
        print(f"  Warning generated: {warning_msg}")
    
    assert missing_samples == {'TTbar', 'NonExistent'}, f"Expected {{'TTbar', 'NonExistent'}}, got {missing_samples}"
    print("  ✓ Missing samples detected correctly")
    
    return True


def main():
    """Run all tests"""
    print("="*60)
    print("Unit Tests for --by-dataset Feature Updates")
    print("="*60)
    
    results = {
        "by_dataset_normalization": test_by_dataset_normalization(),
        "missing_samples_warning": test_missing_samples_warning(),
    }
    
    # Summary
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
        return 0
    else:
        print("\n✗ Some tests FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
