"""
Debug logging utilities for PocketCoffea processing.

This module provides a comprehensive debug logging system that can write detailed
processing information to file, including:
- Processing steps and their timing
- Weights computed and applied
- Calibrators prepared and applied
- Histograms filled
- Sample tensor data for validation
"""

import awkward as ak
import numpy as np
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
import sys


class SuperVerboseDebugLogger:
    """
    A debug logger that writes comprehensive processing information to file.
    
    When enabled, it logs:
    - All processing steps with timestamps
    - Events passing each selection stage
    - Calibrator applications and their effects
    - Weight computations and statistics
    - Histogram filling operations
    - Sample data from arrays (to check for zeros, NaNs, etc.)
    """
    
    def __init__(self, enabled: bool = False, output_dir: str = "debug_logs", 
                 log_filename: Optional[str] = None,
                 sample_size: int = 10,
                 check_zeros: bool = True,
                 check_nans: bool = True,
                 check_infs: bool = True):
        """
        Initialize the debug logger.
        
        Parameters:
        -----------
        enabled : bool
            Whether debug logging is enabled
        output_dir : str
            Directory where log files will be written
        log_filename : str, optional
            Custom log filename. If None, auto-generated based on timestamp
        sample_size : int
            Number of entries to sample from arrays for debugging
        check_zeros : bool
            Check for zero values in arrays
        check_nans : bool
            Check for NaN values in arrays
        check_infs : bool
            Check for infinite values in arrays
        """
        self.enabled = enabled
        self.output_dir = Path(output_dir)
        self.sample_size = sample_size
        self.check_zeros = check_zeros
        self.check_nans = check_nans
        self.check_infs = check_infs
        
        if not self.enabled:
            self.log_file = None
            return
            
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate log filename
        if log_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            log_filename = f"debug_log_{timestamp}.txt"
        
        self.log_filepath = self.output_dir / log_filename
        self.log_file = open(self.log_filepath, 'w', buffering=1)  # Line buffered
        
        # Tracking
        self.step_timings = {}
        self.current_step_start = None
        self.processing_start_time = time.time()
        
        # Write header
        self._write_header()
    
    def _write_header(self):
        """Write log file header with metadata."""
        header = f"""
{'='*80}
PocketCoffea Super Verbose Debug Log
{'='*80}
Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Log file: {self.log_filepath}
{'='*80}

"""
        self.log_file.write(header)
    
    def _write(self, message: str, level: str = "INFO"):
        """Write a message to the log file."""
        if not self.enabled or self.log_file is None:
            return
        
        timestamp = time.time() - self.processing_start_time
        formatted_msg = f"[{timestamp:>8.3f}s] [{level:>7s}] {message}\n"
        self.log_file.write(formatted_msg)
    
    def _analyze_array(self, arr, name: str = "array") -> Dict[str, Any]:
        """
        Analyze an awkward/numpy array and return statistics.
        
        Returns dict with:
        - shape/length
        - dtype
        - sample values
        - statistics (min, max, mean, std)
        - flags for zeros, NaNs, infinities
        """
        analysis = {
            "name": name,
            "type": type(arr).__name__
        }
        
        try:
            # Handle awkward arrays
            if isinstance(arr, ak.Array):
                analysis["length"] = len(arr)
                analysis["ndim"] = arr.ndim
                analysis["fields"] = arr.fields if hasattr(arr, 'fields') else None
                
                # Flatten for analysis
                flat = ak.flatten(arr, axis=None)
                arr_np = ak.to_numpy(flat) if len(flat) > 0 else np.array([])
            else:
                # Assume numpy-like
                arr_np = np.asarray(arr).flatten()
                analysis["shape"] = arr.shape if hasattr(arr, 'shape') else len(arr)
            
            if len(arr_np) == 0:
                analysis["empty"] = True
                return analysis
            
            analysis["dtype"] = str(arr_np.dtype)
            
            # Sample values (first N entries)
            sample_size = min(self.sample_size, len(arr_np))
            analysis["sample_values"] = arr_np[:sample_size].tolist()
            
            # Statistics for numeric arrays
            if np.issubdtype(arr_np.dtype, np.number):
                analysis["min"] = float(np.min(arr_np))
                analysis["max"] = float(np.max(arr_np))
                analysis["mean"] = float(np.mean(arr_np))
                analysis["std"] = float(np.std(arr_np))
                
                # Check for problematic values
                if self.check_zeros:
                    n_zeros = np.sum(arr_np == 0)
                    analysis["n_zeros"] = int(n_zeros)
                    analysis["frac_zeros"] = float(n_zeros / len(arr_np))
                
                if self.check_nans:
                    n_nans = np.sum(np.isnan(arr_np))
                    analysis["n_nans"] = int(n_nans)
                    if n_nans > 0:
                        analysis["WARNING"] = "NaN values detected!"
                
                if self.check_infs:
                    n_infs = np.sum(np.isinf(arr_np))
                    analysis["n_infs"] = int(n_infs)
                    if n_infs > 0:
                        analysis["WARNING"] = "Infinite values detected!"
            
        except Exception as e:
            analysis["error"] = str(e)
        
        return analysis
    
    def step_start(self, step_name: str, metadata: Optional[Dict] = None):
        """Mark the start of a processing step."""
        if not self.enabled:
            return
        
        self.current_step_start = time.time()
        
        msg = f"\n{'='*70}\nSTEP START: {step_name}\n{'='*70}"
        if metadata:
            msg += f"\nMetadata: {json.dumps(metadata, indent=2)}"
        
        self._write(msg, "STEP")
    
    def step_end(self, step_name: str, summary: Optional[Dict] = None):
        """Mark the end of a processing step."""
        if not self.enabled or self.current_step_start is None:
            return
        
        elapsed = time.time() - self.current_step_start
        self.step_timings[step_name] = elapsed
        
        msg = f"STEP END: {step_name} (elapsed: {elapsed:.3f}s)"
        if summary:
            msg += f"\nSummary: {json.dumps(summary, indent=2, default=str)}"
        msg += f"\n{'-'*70}"
        
        self._write(msg, "STEP")
        self.current_step_start = None
    
    def log_events_count(self, stage: str, n_events: int, events: Optional[ak.Array] = None):
        """Log the number of events at a processing stage."""
        if not self.enabled:
            return
        
        msg = f"Events at {stage}: {n_events}"
        
        if events is not None and len(events) > 0:
            # Add some event-level info
            sample_indices = list(range(min(5, len(events))))
            msg += f"\n  Sample event indices: {sample_indices}"
            
        self._write(msg, "COUNT")
    
    def log_selection_mask(self, selection_name: str, mask, events_before: int):
        """Log information about a selection mask."""
        if not self.enabled:
            return
        
        mask_analysis = self._analyze_array(mask, f"mask_{selection_name}")
        
        if isinstance(mask, ak.Array):
            mask_flat = ak.flatten(mask, axis=None)
            n_pass = ak.sum(mask_flat)
        else:
            n_pass = np.sum(mask)
        
        efficiency = n_pass / events_before if events_before > 0 else 0
        
        msg = f"""Selection: {selection_name}
  Events before: {events_before}
  Events passing: {n_pass}
  Efficiency: {efficiency:.4f} ({efficiency*100:.2f}%)
  Mask analysis: {json.dumps(mask_analysis, indent=4, default=str)}"""
        
        self._write(msg, "SELECT")
    
    def log_calibrator_init(self, calibrator_name: str, calibrated_collections: List[str],
                           has_variations: bool, variations: Optional[List[str]] = None):
        """Log calibrator initialization."""
        if not self.enabled:
            return
        
        msg = f"""Calibrator initialized: {calibrator_name}
  Calibrated collections: {calibrated_collections}
  Has variations: {has_variations}
  Variations: {variations if variations else 'None'}"""
        
        self._write(msg, "CALIB")
    
    def log_calibrator_apply(self, calibrator_name: str, variation: str, 
                            collections_modified: List[str],
                            events_before: Optional[ak.Array] = None,
                            events_after: Optional[ak.Array] = None):
        """Log calibrator application."""
        if not self.enabled:
            return
        
        msg = f"""Calibrator applied: {calibrator_name}
  Variation: {variation}
  Collections modified: {collections_modified}"""
        
        # Log before/after for modified collections
        if events_before is not None and events_after is not None:
            for coll_field in collections_modified:
                if "." in coll_field:
                    coll, field = coll_field.split(".", 1)
                    try:
                        before_analysis = self._analyze_array(
                            events_before[coll, field], 
                            f"{coll}.{field}_before"
                        )
                        after_analysis = self._analyze_array(
                            events_after[coll, field],
                            f"{coll}.{field}_after"
                        )
                        msg += f"\n  {coll}.{field} changes:"
                        msg += f"\n    Before: {json.dumps(before_analysis, indent=6, default=str)}"
                        msg += f"\n    After: {json.dumps(after_analysis, indent=6, default=str)}"
                    except Exception as e:
                        msg += f"\n  Could not analyze {coll_field}: {e}"
        
        self._write(msg, "CALIB")
    
    def log_weight_init(self, weight_name: str, has_variations: bool, 
                       variations: Optional[List[str]] = None):
        """Log weight initialization."""
        if not self.enabled:
            return
        
        msg = f"""Weight initialized: {weight_name}
  Has variations: {has_variations}
  Variations: {variations if variations else 'None'}"""
        
        self._write(msg, "WEIGHT")
    
    def log_weight_compute(self, weight_name: str, shape_variation: str,
                          nominal: Optional[ak.Array] = None,
                          up: Optional[ak.Array] = None,
                          down: Optional[ak.Array] = None):
        """Log weight computation."""
        if not self.enabled:
            return
        
        msg = f"""Weight computed: {weight_name}
  Shape variation: {shape_variation}"""
        
        if nominal is not None:
            nominal_analysis = self._analyze_array(nominal, f"{weight_name}_nominal")
            msg += f"\n  Nominal weights: {json.dumps(nominal_analysis, indent=4, default=str)}"
        
        if up is not None:
            up_analysis = self._analyze_array(up, f"{weight_name}_up")
            msg += f"\n  Up variation: {json.dumps(up_analysis, indent=4, default=str)}"
        
        if down is not None:
            down_analysis = self._analyze_array(down, f"{weight_name}_down")
            msg += f"\n  Down variation: {json.dumps(down_analysis, indent=4, default=str)}"
        
        self._write(msg, "WEIGHT")
    
    def log_weight_total(self, category: Optional[str], modifier: Optional[str],
                        total_weight: ak.Array, n_events: int):
        """Log total weight for a category."""
        if not self.enabled:
            return
        
        weight_analysis = self._analyze_array(total_weight, "total_weight")
        
        msg = f"""Total weight calculated:
  Category: {category if category else 'inclusive'}
  Modifier: {modifier if modifier else 'nominal'}
  N events: {n_events}
  Weight statistics: {json.dumps(weight_analysis, indent=4, default=str)}"""
        
        self._write(msg, "WEIGHT")
    
    def log_histogram_fill(self, hist_name: str, category: str, variation: str,
                          n_entries: int, subsample: Optional[str] = None,
                          mask: Optional[ak.Array] = None,
                          weights: Optional[ak.Array] = None):
        """Log histogram filling."""
        if not self.enabled:
            return
        
        msg = f"""Histogram filled: {hist_name}
  Category: {category}
  Variation: {variation}
  Subsample: {subsample if subsample else 'None'}
  N entries: {n_entries}"""
        
        if mask is not None:
            mask_analysis = self._analyze_array(mask, "fill_mask")
            msg += f"\n  Mask: {json.dumps(mask_analysis, indent=4, default=str)}"
        
        if weights is not None:
            weight_analysis = self._analyze_array(weights, "fill_weights")
            msg += f"\n  Weights: {json.dumps(weight_analysis, indent=4, default=str)}"
        
        self._write(msg, "HIST")
    
    def log_array(self, name: str, array, context: str = ""):
        """Log detailed information about an array."""
        if not self.enabled:
            return
        
        analysis = self._analyze_array(array, name)
        
        msg = f"Array analysis: {name}"
        if context:
            msg += f"\n  Context: {context}"
        msg += f"\n  Details: {json.dumps(analysis, indent=4, default=str)}"
        
        self._write(msg, "ARRAY")
    
    def log_info(self, message: str):
        """Log a general info message."""
        if not self.enabled:
            return
        self._write(message, "INFO")
    
    def log_warning(self, message: str):
        """Log a warning message."""
        if not self.enabled:
            return
        self._write(message, "WARN")
    
    def log_error(self, message: str):
        """Log an error message."""
        if not self.enabled:
            return
        self._write(message, "ERROR")
    
    def summary(self) -> str:
        """Generate a summary of the processing."""
        if not self.enabled:
            return ""
        
        total_time = time.time() - self.processing_start_time
        
        summary = f"""
{'='*80}
PROCESSING SUMMARY
{'='*80}
Total processing time: {total_time:.3f}s

Step timings:
"""
        for step, elapsed in self.step_timings.items():
            summary += f"  {step}: {elapsed:.3f}s ({elapsed/total_time*100:.1f}%)\n"
        
        summary += f"\n{'='*80}\n"
        
        self._write(summary, "SUMMARY")
        return summary
    
    def close(self):
        """Close the debug logger and write summary."""
        if not self.enabled or self.log_file is None:
            return
        
        self.summary()
        self.log_file.close()
        self.log_file = None
    
    def __del__(self):
        """Ensure log file is closed."""
        if hasattr(self, 'log_file') and self.log_file is not None:
            self.close()
