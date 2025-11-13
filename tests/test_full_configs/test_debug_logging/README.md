# Debug Logging Test Configuration

## Purpose

This test configuration verifies that the super verbose debug logging feature works correctly in PocketCoffea. It ensures that:

1. Debug logs are created when `super_debug.enabled = True`
2. All key processing steps are logged
3. The log file contains expected sections (processing steps, weights, selections, histograms)
4. The logging doesn't interfere with normal processing
5. The summary section is generated correctly

## Configuration

The test uses a minimal configuration with:
- **Sample**: TTTo2L2Nu (2018)
- **Categories**: baseline, 1jet, 2jet
- **Weights**: genWeight, lumi, XS, pileup, sf_mu_id (with variations)
- **Histograms**: Jet histograms and JetGood counts
- **Calibrators**: None (to simplify testing)
- **Debug options**:
  - `enabled: True`
  - `output_dir: "debug_logs"`
  - `log_filename: "test_debug.log"`
  - `sample_size: 5` (small sample for faster testing)
  - `check_zeros: True`
  - `check_nans: True`
  - `check_infs: True`

## Test Verification

The test (`test_debug_logging` in `test_full_configs.py`) checks:

1. **Log file creation**: Verifies `debug_logs/test_debug.log` exists
2. **Log header**: Checks for "PocketCoffea Super Verbose Debug Log"
3. **Processing steps**: 
   - PROCESSING_START
   - SKIMMING
   - INITIALIZATION
   - VARIATION_nominal
4. **Event counts**:
   - "Events at initial"
   - "Events at after_skim"
5. **Component logging**:
   - Weight initialization and computation
   - Histogram filling
   - Selection masks
6. **Summary**: Processing summary with timing
7. **Functionality**: Verifies processing output is valid

## Expected Log Structure

```
================================================================================
PocketCoffea Super Verbose Debug Log
================================================================================
Started: 2025-11-05 ...
Log file: debug_logs/test_debug.log
================================================================================

[   0.000s] [  STEP  ] STEP START: PROCESSING_START
[   0.012s] [  COUNT ] Events at initial: ...
[   0.045s] [  ARRAY ] Array analysis: genWeight
...
[   0.234s] [ SELECT] Selection: ...
...
[   0.567s] [ WEIGHT] Weight initialized: pileup
...
[  15.678s] [ WEIGHT] Weight computed: pileup
...
[  20.456s] [   HIST] Histogram filled: nJetGood
...
[  45.678s] [SUMMARY] PROCESSING SUMMARY
                       Total time: ...
================================================================================
```

## Running the Test

From the PocketCoffea root directory:

```bash
# Run just this test
pytest tests/test_full_configs/test_full_configs.py::test_debug_logging -v

# Run all tests
pytest tests/test_full_configs/test_full_configs.py -v
```

## Files

- `config.py`: Main configuration with debug enabled
- `workflow.py`: Basic processor workflow
- `params/`: Parameter files (object preselection, triggers)
- `datasets/`: Dataset definitions
- `debug_logs/`: Output directory for debug logs (created during test)

## Notes

- The test uses a small chunk size (500 events) and limits to 1 file/1 chunk for speed
- Debug logging adds ~5-10% overhead, which is acceptable for testing
- The log file can be inspected after the test for detailed debugging information
- The test is designed to be fast (<30 seconds) while still exercising all debug features
