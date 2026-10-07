# Calibrators

The PocketCoffea framework provides a flexible and powerful calibration system to handle object corrections and systematic variations in CMS analyses. The calibration system is designed around the concept of **Calibrators** - modular components that apply corrections to physics objects (jets, electrons, muons, MET, etc.) and manage their systematic variations.

## Overview

The calibration system consists of three main components:

1. **Calibrator**: Abstract base class that defines individual calibration steps
2. **CalibratorsManager**: Orchestrates the application of multiple calibrators in sequence
3. **Base Workflow Integration**: Automatic handling of systematic variations in the analysis workflow

### Key Features

- **Sequential Processing**: Calibrators are applied in a user-defined sequence, allowing for complex interdependencies
- **Automatic Variation Handling**: Each calibrator can define its own systematic variations that are automatically propagated through the analysis and made available in the configuration file.
- **Original Collection Preservation**: The system maintains references to original collections for calibrators that need uncorrected inputs
- **Flexible Configuration**: Calibrators can be configured through parameters and enabled/disabled per data-taking period. Also the systematic variations can be different by period or event type (Sample).
- **Type Safety**: Built-in checks ensure calibrators only modify collections they declare to handle

## Calibrator Base Class

All calibrators inherit from the abstract `Calibrator` class and must implement specific methods:

### Class Attributes

```python
class YourCalibrator(Calibrator):
    name: str = "your_calibrator_name"  # Unique identifier
    has_variations: bool = True  # Whether this calibrator provides variations
    isMC_only: bool = False  # Whether to run only on MC
    calibrated_collections: List[str] = [
        "Collection.field"
    ]  # Collections this calibrator modifies
```

### Required Methods

#### Constructor `__init__(self, params, metadata, do_variations, **kwargs)`
Called to initialize the Calibrator and store necessary metadata for easy later usage. `do_variations` tells the calibrator whether its variations are requested for the current chunk (set by the `CalibratorsManager`, based on the configuration).

#### `initialize(events)`
Called once per chunk to prepare calibration data:

```python
def initialize(self, events):
    # Prepare calibration factors, load correction files
    # Set up variations list: self._variations = ["variation1Up", "variation1Down", ...]
    pass
```

This method should setup the `self._variations` variable to define dynamically the list of variations made available for 
the current chunk of events. Both the **Up** and **Down** variations should be defined (meaning that the framework does not assume any variation automatically). 

#### `calibrate(events, events_original_collections, variation, already_applied_calibrators)`
Called for each systematic variation to apply corrections:

```python
def calibrate(
    self,
    events,
    events_original_collections,
    variation,
    already_applied_calibrators=None,
):
    # Apply corrections based on the requested variation
    # Return dictionary: {"Collection.field": corrected_values}
    return {"Jet.pt": corrected_jet_pts}
```



## Built-in Calibrators

PocketCoffea provides several ready-to-use calibrators in `pocket_coffea.lib.calibrators.common`:

### JetsCalibrator
- **Name**: `"jet_calibration"`
- **Purpose**: Applies Jet Energy Corrections (JEC) and Jet Energy Resolution (JER) smearing. If pT regression is requested for a jet type (`apply_pt_regr_MC`/`apply_pt_regr_Data`), it is applied first, before the JEC.
- **Collections**: Configurable. Every jet collection listed in `jets_calibration.collection[year]` (e.g. `Jet`, `FatJet`) that has `apply_jec_MC`/`apply_jec_Data` enabled for its jet type is calibrated.
- **Variations**: One `"{jet_type}_{source}Up"` / `"{jet_type}_{source}Down"` pair per entry configured in `jets_calibration.variations[jet_type][year]` (e.g. `"AK4PFchs_jecUp"`, `"AK4PFchs_jerDown"`).
- **Options**: the JEC can be applied [by level](#jec-applied-by-level) instead of with the compound correction, and the [Run3 forward jets mitigations](#run3-forward-jets-mitigations) can be switched on.

### JetsSoftdropMassCalibrator
- **Name**: `"msoftdrop_calibration"`
- **Purpose**: Applies the JEC to the softdrop mass of AK8 (`AK8PFPuppi`) jets, using the corresponding AK4 subjet corrections.
- **Collections**: `["FatJet.msoftdrop"]`
- **Variations**: Not yet implemented for the softdrop mass (only the nominal correction is available).
- **Note**: Not part of `default_calibrators_sequence`; add it explicitly to your calibrator sequence if softdrop mass calibration is needed.

### METCalibrator
- **Name**: `"met_type1_calibration"`
- **Purpose**: Recomputes Type-1 corrected MET starting from the raw MET, propagating the jet corrections and, if they ran earlier in the sequence, the electron/muon scale and smearing corrections.
- **Collections**: `["MET.pt", "MET.phi"]` or `["PuppiMET.pt", "PuppiMET.phi"]`, depending on `met_calibration.MET_collection`
- **Variations**: `"unclust_EnUp"`, `"unclust_EnDown"` (unclustered energy variations)
- **Dependencies**: Must run after `JetsCalibrator`, and after any electron/muon calibrators whose corrections should be propagated to the MET (it compares the original and calibrated `Electron.pt`/`Muon.pt` to do so)

### ElectronsScaleCalibrator
- **Name**: `"electron_scale_and_smearing"`
- **Purpose**: Applies electron energy scale (data) and resolution smearing (MC) corrections
- **Collections**: `["Electron.pt", "Electron.pt_original"]`
- **Variations**: `"ele_scaleUp/Down"`, `"ele_smearUp/Down"` (MC only)

### MuonsCalibrator
- **Name**: `"muons_scale_and_resolution"`
- **Purpose**: Applies muon momentum scale and resolution corrections
- **Collections**: `["Muon.pt", "Muon.pt_original", "Muon.energyErr"]`
- **Variations**: `"muon_scaleUp/Down"`, `"muon_smearUp/Down"` (MC only)

`default_calibrators_sequence` runs these in the order `[JetsCalibrator, ElectronsScaleCalibrator, MuonsCalibrator, METCalibrator]` (`JetsSoftdropMassCalibrator` is not included).

## Configuration

### Basic Setup

In your analysis configuration file:

```python
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

cfg = Configurator(
    # ... other configuration ...
    # Use default calibrator sequence
    calibrators=default_calibrators_sequence,
    # Configure shape variations
    variations={
        "shape": {
            "common": {
                "inclusive": ["jet_calibration"],  # Run jet variations for all samples
            },
            "bysample": {
                "MC_Sample": {
                    "inclusive": [
                        "electron_scale_and_smearing"
                    ],  # Run electron variations for specific samples
                }
            },
        }
    },
)
```

### Custom Calibrator Sequence

You can define your own calibrator sequence:

```python
from pocket_coffea.lib.calibrators.common import JetsCalibrator, METCalibrator
from your_module import CustomCalibrator

custom_sequence = [JetsCalibrator, METCalibrator, CustomCalibrator]

cfg = Configurator(
    calibrators=custom_sequence,
    # ... rest of configuration
)
```

### Parameters Configuration

Calibrators read their configuration from the parameters system. Example for jet calibration:

```yaml
# params/jets_calibration.yaml
jets_calibration:
  collection:
    2022:
      AK4PFchs: "Jet"
      AK8PFPuppi: "FatJet"

  jet_types:
    AK4PFchs:
        2016_PreVFP:
        json_path: ${cvmfs:Run2-2016preVFP-UL-NanoAODv9,JME,jet_jerc.json.gz}
        jec_mc: Summer19UL16APV_V7_MC
        jec_data:
          B: Summer19UL16APV_RunBCD_V7_DATA
          C: Summer19UL16APV_RunBCD_V7_DATA
          D: Summer19UL16APV_RunBCD_V7_DATA
          E: Summer19UL16APV_RunEF_V7_DATA
          F: Summer19UL16APV_RunEF_V7_DATA
        jer: Summer20UL16APV_JRV3_MC
        level: L1L2L3Res
    ... 


  apply_jec_MC:
    2022:
      AK4PFchs: true
      AK8PFPuppi: true

  apply_jec_Data:
    2022:
      AK4PFchs: true
      AK8PFPuppi: false

  variations:
    2022:
      AK4PFchs: ["jec", "jer"]
      AK8PFPuppi: ["jec"]
```

### JEC applied by level

By default the JEC is the single *compound* correction named by `level` (e.g. `L1L2L3Res`).
With `by_level: True` in the calibration parameters of a jet type and year, `level` is instead
a list of single JEC levels, applied one after the other:

```yaml
jets_calibration:
  jet_types:
    AK4PFPuppi:
      "2024":
        json_path: ...
        jec_mc: Summer24Prompt24_V5_MC
        jec_data: Summer24Prompt24_V5_DATA
        jer: Summer24Prompt24_JRV2_MC
        by_level: True
        level: [L1FastJet, L2Relative, L3Absolute, L2L3Residual]
```

Each level is evaluated on the running, partially-corrected pt and its factor is multiplied into
the total correction, exactly as done internally by the compound correction: without any
customization the result is identical. Applying the levels explicitly allows evaluating a level
at a different pt than the one the correction propagates on (used by the `residual_pt_floor`
mitigation below). The implementation is `pocket_coffea.lib.jets.jec_by_level`; the JER and the
JES/JER systematics are the same in the two modes.

### Run3 forward jets mitigations

The JME POG recommends some mitigations for the issues of the Run3 jets in the endcaps
(HE, 2.5 < |η| < 3, the "horns") and in the forward calorimeter (HF, 3 < |η| < 5):

| Year | HF (3 < \|η\| < 5) | HE (2.5 < \|η\| < 3) | 2.0 < \|η\| < 2.5 |
|------|--------------------|-----------------------|-------------------|
| 2022 | Require pT > 50 GeV | Require pT > 50 GeV + JER for gen-matched only | - |
| 2023 | Require pT > 50 GeV | Require pT > 50 GeV + JER for gen-matched only | - |
| 2024 | Fixed | Require pT > 50 GeV + JER for gen-matched only | Data: for MC-truth corrected pT < 30 GeV, use the L2L3Residual evaluated at MC-truth corrected pT = 30 GeV |
| 2025 | Fixed, but worsening due to radiation damage | Much improved | Should not be an issue |

They are configured in `jets_calibration.forward_jets_mitigation` (`pocket_coffea/parameters/jets_calibration.yaml`)
and are **all switched off by default**. Each mitigation is turned on with its `apply` key and acts
only in the years for which `eta_regions` (a list of `[eta_min, eta_max)` intervals in |η|) are
defined, so the defaults reproduce the table above:

```yaml
jets_calibration:
  forward_jets_mitigation:
    jet_types: [AK4PFPuppi]   # jet types the JER and residual mitigations are applied to
    pt_cut:                   # jet_selection: reject jets with pt < pt_min in the regions
      apply: False
      pt_min: 50.
      eta_regions:
        2022_preEE: [[2.5, 3.0], [3.0, 5.0]]
        ...                   # same for 2022_postEE, 2023_preBPix, 2023_postBPix
        "2024": [[2.5, 3.0]]
    jer_genmatched_only:      # MC: JER smearing only for the gen-matched jets in the regions
      apply: False
      eta_regions:
        2022_preEE: [[2.5, 3.0]]
        ...                   # 2022, 2023 and 2024
    residual_pt_floor:        # Data: evaluate `level` at pt_min for MC-truth corrected pt < pt_min
      apply: False
      level: L2L3Residual
      pt_min: 30.
      levels: [L1FastJet, L2Relative, L3Absolute, L2L3Residual]
      eta_regions:
        "2024": [[2.0, 2.5]]
```

To activate them, override the `apply` keys in the analysis parameters, either in a parameters
yaml file passed to `defaults.merge_parameters_from_files`:

```yaml
jets_calibration:
  forward_jets_mitigation:
    pt_cut:
      apply: True
    jer_genmatched_only:
      apply: True
    residual_pt_floor:
      apply: True
```

or directly in the configuration:

```python
parameters = defaults.merge_parameters_from_files(default_parameters, ...)
parameters.jets_calibration.forward_jets_mitigation.pt_cut.apply = True
parameters.jets_calibration.forward_jets_mitigation.jer_genmatched_only.apply = True
parameters.jets_calibration.forward_jets_mitigation.residual_pt_floor.apply = True
```

- **`pt_cut`** is a selection, applied by `pocket_coffea.lib.jets.jet_selection` to every jet
  collection it selects. The `forward_jet_veto` argument of `jet_selection` overrides the `apply`
  key for a single selection: `jet_selection(events, "Jet", params, year, forward_jet_veto=True)`
  applies the cut even if it is off in the parameters, `forward_jet_veto=False` never applies it,
  and the default `None` follows the parameters. The cut is on the `pt` of the selected collection.
- **`jer_genmatched_only`** (MC only, applied by the `JetsCalibrator` to the `jet_types`): in the
  regions the JER smearing is applied only to the jets using the scaling method, i.e. with a
  gen-jet match (ΔR < R/2 and |pT − pT,gen| < 3 σ<sub>JER</sub> pT). The jets without a match keep
  their JES-corrected pt instead of being stochastically smeared. The same applies to the JER
  up/down variations.
- **`residual_pt_floor`** (data only, applied by the `JetsCalibrator` to the `jet_types`): for the
  jets in the regions with MC-truth corrected pt (pt after the levels before `level`) below
  `pt_min`, the `level` correction factor is evaluated at `pt_min`, and applied to the real pt.
  This needs the JEC applied by level: if the jet type is configured with the compound
  correction, the single `levels` are used instead for the years where the mitigation is active.
  The jet types sharing the AK4 PUPPI calibration (e.g. `AK4CorrT1METJetPuppi`, aliased to
  `AK4PFPuppi`) are corrected consistently, so that the MET propagation sees the same JEC.

## Creating Custom Calibrators

### Simple Example

Here's a template for a custom calibrator:

```python
from pocket_coffea.lib.calibrators.calibrator import Calibrator
import awkward as ak


class MyCustomCalibrator(Calibrator):
    name = "my_custom_calibrator"
    has_variations = True
    isMC_only = False
    calibrated_collections = ["MyObject.pt", "MyObject.mass"]

    def __init__(self, params, metadata, do_variations, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        # Access configuration
        self.my_config = self.params.my_calibrator_config

    def initialize(self, events):
        # Prepare correction factors
        self.scale_factor = self.calculate_scale_factor(events)

        # Define available variations
        if self.isMC:
            self._variations = ["myUncertaintyUp", "myUncertaintyDown"]
        else:
            self._variations = []

    def calibrate(
        self, events, orig_colls, variation, already_applied_calibrators=None
    ):
        # Get the objects to calibrate
        objects = events["MyObject"]

        # Apply nominal correction
        corrected_pt = objects.pt * self.scale_factor
        corrected_mass = objects.mass * self.scale_factor

        # Apply systematic variations
        if variation == "myUncertaintyUp":
            corrected_pt = corrected_pt * 1.02
        elif variation == "myUncertaintyDown":
            corrected_pt = corrected_pt * 0.98

        return {"MyObject.pt": corrected_pt, "MyObject.mass": corrected_mass}
```

### Advanced Example with Dependencies

For calibrators that depend on other calibrators' output and/or on the original uncalibrated information. 

```python
class AdvancedCalibrator(Calibrator):
    name = "advanced_calibrator"
    has_variations = True
    isMC_only = True
    calibrated_collections = ["DerivedQuantity"]

    def calibrate(
        self, events, orig_colls, variation, already_applied_calibrators=None
    ):
        # Check dependencies
        if "jet_calibration" not in already_applied_calibrators:
            raise ValueError(
                "This calibrator requires jet_calibration to be applied first"
            )

        # Use original jets if needed for some calculation
        if "Jet" in orig_colls:
            original_jets = orig_colls["Jet"]

        # Use calibrated jets from events
        # Reading from "events" in practice is taking all the objects calibrated up to this point in the sequence.
        calibrated_jets = events["Jet"]

        # Compute derived quantity
        derived = self.compute_derived_quantity(
            original_jets, calibrated_jets, variation
        )

        return {"DerivedQuantity": derived}
```

## Systematic Variations

### Variation Naming Convention

Systematic variations should follow the pattern: `"{source}Up"` / `"{source}Down"` where `source` describes the uncertainty source (e.g., "AK4PFchs_jec", "AK4PFchs_jer", "ele_scale").

Examples:
- `"AK4PFchs_jecUp"`, `"AK4PFchs_jerDown"` (from `JetsCalibrator`)
- `"ele_scaleUp"`, `"ele_scaleDown"` (from `ElectronsScaleCalibrator`)

### Configuration in Analysis

Variations are configured in the `variations.shape` section:

```python
variations = {
    "shape": {
        "common": {
            "inclusive": [
                "jet_calibration",  # All JEC/JER variations
                "electron_scale_and_smearing",  # All electron variations
            ],
        },
        "bysample": {
            "TTbar": {
                "inclusive": ["custom_calibrator"],  # Sample-specific variations
            }
        },
    }
}
```

### Automatic Propagation

The framework automatically:
1. Collects all variations from configured calibrators
2. Loops over each variation during processing
3. Fills separate histograms for each variation
4. Resets events to original state between variations

## Integration with Workflow

The calibration system is seamlessly integrated into the base workflow:

### Initialization
```python
def initialize_calibrators(self):
    self.calibrators_manager = CalibratorsManager(
        self.cfg.calibrators,
        self.events,
        self.params,
        self._metadata,
        requested_calibrator_variations=self.cfg.available_shape_variations[self._sample],
    )
```

### Variation Loop
```python
def loop_over_variations(self):
    for variation, events_calibrated in self.calibrators_manager.calibration_loop(
        self.events,
        variations_for_calibrators=self.cfg.available_shape_variations[self._sample],
    ):
        self.events = events_calibrated
        yield variation
```

## Best Practices

### Performance
- **Heavy computations** should be done in `initialize()` once per chunk
- **Light corrections** can be applied dynamically in `calibrate()`
- **Cache expensive operations** when possible

### Dependencies
- **Declare dependencies explicitly** by checking `already_applied_calibrators`
- **Use original collections** from `orig_colls` when needed
- **Order calibrators carefully** in your sequence

### Error Handling
- **Validate inputs** in both `initialize()` and `calibrate()`
- **Check collection existence** before accessing
- **Provide meaningful error messages**

### Testing
- **Test with both MC and Data** if applicable
- **Verify all declared collections** are actually modified
- **Check variation names** follow conventions
- **Validate with different parameter configurations**


### Common Issues
1. **Collection not found**: Check `calibrated_collections` declaration
2. **Variation not applied**: Verify variation name and configuration
3. **Performance issues**: Move heavy computation to `initialize()`
4. **Dependency errors**: Check calibrator order and requirements

