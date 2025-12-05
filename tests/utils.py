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

@pytest.fixture(scope="session")
def events_run3():
    filenames_run3 = {
        "2022_preEE" : "root://xrootd-cms.infn.it//store/mc/Run3Summer22NanoAODv12/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_v5-v2/2520000/66b834d6-61f7-4109-b5ae-54a150d4814b.root",
       # "2022_postEE" : "root://dcache-cms-xrootd.desy.de//store/mc/Run3Summer22EENanoAODv12/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6_ext1-v2/2520000/00525c9a-c32a-4bed-8290-5a8cfb3c2536.root",
        "2023_preBPix" : "root://xrootd-cms.infn.it//store/mc/Run3Summer23NanoAODv12/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v14-v2/70000/01c164b9-8388-4aa3-b506-16ec245d2056.root",
        "2023_postBPix" : "root://xrootd-cms.infn.it//store/mc/Run3Summer23BPixNanoAODv12/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v2-v3/2550000/1fc49961-22ba-4b79-86d7-e85128f21146.root"
    }
    events_run3 = {}

    for year, filename in filenames_run3.items():
        print(filename)
        events_run3[year] = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=100).events()

    return events_run3

def compare_outputs(output, old_output, exclude_variables=None):
    for cat, data in old_output["sumw"].items():
        assert cat in output["sumw"]
        for dataset, _data in data.items():
            assert dataset in output["sumw"][cat]
            for sample, sumw in _data.items():
                assert np.isclose(sumw, output["sumw"][cat][dataset][sample]["nominal"], rtol=1e-5)

    # Testing cutflow
    for cat, data in old_output["cutflow"].items():
        assert cat in output["cutflow"]
        for dataset, _data in data.items():
            assert dataset in output["cutflow"][cat]
            # TODO: expand to more variations
            if cat in ["initial", "skim"]:
                assert np.allclose(_data, output["cutflow"][cat][dataset], rtol=1e-5)
                continue
            elif cat in ["presel"]:
                assert np.allclose(_data, output["cutflow"][cat][dataset]["nominal"], rtol=1e-5)
                continue
            for sample, cutflow in _data.items():
                assert np.allclose(cutflow, output["cutflow"][cat][dataset][sample]["nominal"], rtol=1e-5)

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
                            H1 = hist[{"variation":variation, "cat":cat}].values()
                            H2 = output["variables"][variables][sample][dataset][{"variation":variation, "cat":cat}].values()
                            if not np.allclose(H1, H2, rtol=1e-5):
                                assert check_single_bin_shift(H1, H2), f"Histograms for {variables} {dataset} {sample} {variation} do not match and are not a single bin shift"
                else:
                    assert np.allclose(hist.values(),
                                       output["variables"][variables][sample][dataset].values(),
                                       rtol=1e-5)

def compare_totalweight(output, variables):
    for variable in variables:        
        for category, datasets in output["sumw"].items():
            for dataset, samples in datasets.items():
                for sample, sumw in samples.items():
                    sumw = sumw["nominal"]
                    if dataset not in output["variables"][variable][sample]:
                        continue
                    print(f"Checking {variable} for {category} in {dataset} for {sample}")
                    print(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)
                    assert np.isclose(output["variables"][variable][sample][dataset][hist.loc(category), hist.loc("nominal"), :].sum(flow=True).value, sumw)

def compare_columns(output, old_output, exclude_columns=None):
    """Compare columns between two outputs.
    
    Args:
        output: New output dictionary containing columns
        old_output: Old output dictionary containing columns
        exclude_columns: List of column names to exclude from comparison
    """
    for sample, data in old_output["columns"].items():
        assert sample in output["columns"], f"Sample {sample} not found in output columns"
        for dataset, _data in data.items():
            assert dataset in output["columns"][sample], f"Dataset {dataset} not found in output columns for sample {sample}"
            for cat, columns in _data.items():
                assert cat in output["columns"][sample][dataset], f"Category {cat} not found in output columns for sample {sample}, dataset {dataset}"
                for column_name, column_data in columns.items():
                    if exclude_columns is not None and column_name in exclude_columns:
                        continue
                    assert column_name in output["columns"][sample][dataset][cat]["nominal"], f"Column {column_name} not found in output for sample {sample}, dataset {dataset}, category {cat}"
                    print(f"Checking column {column_name} for {cat} in {dataset} for {sample}")
                    assert np.allclose(column_data.value, output["columns"][sample][dataset][cat]["nominal"][column_name].value, rtol=1e-5), \
                        f"Column {column_name} mismatch for sample {sample}, dataset {dataset}, category {cat}"


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
