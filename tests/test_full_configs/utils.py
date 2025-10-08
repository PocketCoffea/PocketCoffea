import pytest
import os
import numpy as np
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import hist
import numpy as np
from typing import Union


@pytest.fixture(scope="session")
def events():
    filename = "root://eoscms.cern.ch//eos/cms/store/mc/RunIISummer20UL18NanoAODv9/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/280000/01881676-C30A-2142-B3B2-7A8449DAF8EF.root"

    print(filename)
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()
    return events


def compare_outputs(output, old_output, exclude_variables=None):
    for cat, data in old_output["sumw"].items():
        assert cat in output["sumw"]
        for dataset, _data in data.items():
            assert dataset in output["sumw"][cat]
            for sample, sumw in _data.items():
                assert np.isclose(sumw, output["sumw"][cat][dataset][sample], rtol=1e-5)

    # Testing cutflow
    for cat, data in old_output["cutflow"].items():
        assert cat in output["cutflow"]
        for dataset, _data in data.items():
            assert dataset in output["cutflow"][cat]
            if cat in ["initial", "skim", "presel"]:
                assert np.allclose(_data, output["cutflow"][cat][dataset], rtol=1e-5)
                continue
            for sample, cutflow in _data.items():
                assert np.allclose(cutflow, output["cutflow"][cat][dataset][sample], rtol=1e-5)

    metadata = output["datasets_metadata"]["by_dataset"]
    # Testing variables
    for variables, data in old_output["variables"].items():
        if exclude_variables is not None and variables in exclude_variables:
            continue
        assert variables in output["variables"]    
        for sample, _data in data.items():
            assert sample in output["variables"][variables]
            for dataset, hist in _data.items():
                if metadata[dataset]["isMC"] == "True":
                    variations = list([hist.axes[1].value(i) for i in range(hist.axes[1].size)])
                    cats = list([hist.axes[0].value(i) for i in range(hist.axes[0].size)])
                    for variation in variations:
                        for cat in cats:
                            print(f"Checking {variables} {dataset} {sample} {variation}")
                            assert np.allclose(hist[{"variation":variation, "cat":cat}].values(),
                                   output["variables"][variables][sample][dataset][{"variation":variation, "cat":cat}].values(),
                                       rtol=1e-5)
                else:
                    assert np.allclose(hist.values(),
                                       output["variables"][variables][sample][dataset].values(),
                                       rtol=1e-5)
                            


def compare_totalweight(output, variables):
    for variable in variables:        
        for category, datasets in output["sumw"].items():
            for dataset, samples in datasets.items():
                for sample, sumw in samples.items():
                    if dataset not in output["variables"][variable][sample]:
                        continue
                    print(f"Checking {variable} for {category} in {dataset} for {sample}")
                    print(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)
                    assert np.isclose(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)



def check_single_bin_shift(hist_a: np.ndarray, hist_b: np.ndarray, 
                          tolerance: float = 1e-10) -> bool:
    """
    Check if the difference between two histograms represents a single bin shift.
    
    A single bin shift is characterized by:
    - Two adjacent bins having opposite non-zero values in the difference (A - B)
    - All other bins having differences close to zero (within tolerance)
    - The absolute values of the two opposite entries should be equal
    
    Parameters
    ----------
    hist_a : np.ndarray
        First histogram bin counts
    hist_b : np.ndarray
        Second histogram bin counts
    tolerance : float, optional
        Tolerance for considering values as zero, by default 1e-10
        
    Returns
    -------
    bool
        True if the difference represents a single bin shift, False otherwise
        
    Examples
    --------
    >>> hist_a = np.array([10, 5, 15, 8])
    >>> hist_b = np.array([10, 6, 14, 8])  # One event moved from bin 2 to bin 1
    >>> check_single_bin_shift(hist_a, hist_b)
    True
    
    >>> hist_a = np.array([10, 5, 15, 8])
    >>> hist_b = np.array([11, 5, 15, 7])  # Different pattern
    >>> check_single_bin_shift(hist_a, hist_b)
    False
    """
    if len(hist_a) != len(hist_b):
        raise ValueError("Histograms must have the same length")
    
    if len(hist_a) < 2:
        raise ValueError("Histograms must have at least 2 bins")
    
    # Calculate the difference
    diff = hist_a - hist_b
    
    # Find bins with non-zero differences (beyond tolerance)
    non_zero_mask = np.abs(diff) > tolerance
    non_zero_indices = np.where(non_zero_mask)[0]
   
    # For a single bin shift, we should have exactly 2 non-zero differences
    if len(non_zero_indices) != 2:
        return False
    
    # Check if the two non-zero bins are adjacent
    idx1, idx2 = non_zero_indices
    if abs(idx1 - idx2) != 1:
        return False
    
    # Check if the values are opposite (sum should be close to zero)
    val1, val2 = diff[idx1], diff[idx2]
    if abs(val1 + val2) > tolerance:
        return False
    
    # Check if the absolute values are equal (within tolerance)
    if abs(abs(val1) - abs(val2)) > tolerance:
        return False
    
    return True


def find_single_bin_shifts(hist_a: np.ndarray, hist_b: np.ndarray, 
                          tolerance: float = 1e-10) -> list:
    """
    Find all pairs of adjacent bins that show single bin shift patterns.
    
    This is useful when multiple single bin shifts might be present in the
    histogram difference.
    
    Parameters
    ----------
    hist_a : np.ndarray
        First histogram bin counts
    hist_b : np.ndarray
        Second histogram bin counts
    tolerance : float, optional
        Tolerance for considering values as zero, by default 1e-10
        
    Returns
    -------
    list of tuples
        List of (bin1_idx, bin2_idx, shift_value) tuples where shift_value
        is the amount shifted from bin1 to bin2
        
    Examples
    --------
    >>> hist_a = np.array([10, 5, 15, 8, 12])
    >>> hist_b = np.array([10, 6, 14, 9, 11])  # Two shifts: (2->1) and (4->3)
    >>> shifts = find_single_bin_shifts(hist_a, hist_b)
    >>> print(shifts)
    [(1, 2, 1.0), (3, 4, 1.0)]
    """
    if len(hist_a) != len(hist_b):
        raise ValueError("Histograms must have the same length")
    
    diff = hist_a - hist_b
    shifts = []
    
    # Look for adjacent pairs with opposite signs
    for i in range(len(diff) - 1):
        val1, val2 = diff[i], diff[i + 1]
        
        # Skip if either value is effectively zero
        if abs(val1) <= tolerance or abs(val2) <= tolerance:
            continue
            
        # Check if they have opposite signs and equal magnitudes
        if (val1 * val2 < 0 and  # opposite signs
            abs(abs(val1) - abs(val2)) <= tolerance):  # equal magnitudes
            
            # Determine shift direction and magnitude
            if val1 > 0:  # shift from bin i+1 to bin i
                shifts.append((i, i + 1, val1))
            else:  # shift from bin i to bin i+1
                shifts.append((i, i + 1, -val1))
    
    return shifts


def validate_histogram_shift(hist_a: np.ndarray, hist_b: np.ndarray,
                           expected_shifts: list, tolerance: float = 1e-10) -> bool:
    """
    Validate that the difference between two histograms matches expected shifts.
    
    Parameters
    ----------
    hist_a : np.ndarray
        First histogram bin counts
    hist_b : np.ndarray
        Second histogram bin counts
    expected_shifts : list of tuples
        Expected shifts as (from_bin, to_bin, amount) tuples
    tolerance : float, optional
        Tolerance for floating point comparisons, by default 1e-10
        
    Returns
    -------
    bool
        True if the histograms match the expected shift pattern
    """
    if len(hist_a) != len(hist_b):
        raise ValueError("Histograms must have the same length")
    
    # Create expected difference array
    expected_diff = np.zeros_like(hist_a, dtype=float)
    
    for from_bin, to_bin, amount in expected_shifts:
        expected_diff[from_bin] += amount
        expected_diff[to_bin] -= amount
    
    # Compare with actual difference
    actual_diff = hist_a - hist_b
    
    return np.allclose(actual_diff, expected_diff, atol=tolerance)