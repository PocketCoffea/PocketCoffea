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
    has_variations: bool = True         # Whether this calibrator provides variations
    isMC_only: bool = False            # Whether to run only on MC
    calibrated_collections: List[str] = ["Collection.field"]  # Collections this calibrator modifies
```

### Required Methods

#### Constructor `__init__(self, params, metadata, **kwargs)`
Called to initialize the Calibrator and store necessary metadata for easy later usage. 

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
def calibrate(self, events, events_original_collections, variation, already_applied_calibrators=None):
    # Apply corrections based on the requested variation
    # Return dictionary: {"Collection.field": corrected_values}
    return {"Jet.pt": corrected_jet_pts}
```



## Built-in Calibrators

PocketCoffea provides several ready-to-use calibrators:

### JetsCalibrator
- **Name**: `"jet_calibration"`
- **Purpose**: Applies Jet Energy Corrections (JEC) and Jet Energy Resolution (JER)
- **Collections**: `["Jet", "FatJet"]`
- **Variations**: JEC and JER uncertainties (e.g., `"jet_jecUp"`, `"jet_jerDown"`)

### METCalibrator
- **Name**: `"met_rescaling"`
- **Purpose**: Propagates jet corrections to Missing Energy (MET)
- **Collections**: `["MET.pt", "MET.phi"]` (configurable)
- **Dependencies**: Must run after JetsCalibrator

### ElectronsScaleCalibrator
- **Name**: `"electron_scale_and_smearing"`
- **Purpose**: Applies electron energy scale and resolution corrections
- **Collections**: `["Electron.pt", "Electron.pt_original"]`
- **Variations**: `"ele_scaleUp/Down"`, `"ele_smearUp/Down"` (MC only)

## Configuration

### Basic Setup

In your analysis configuration file:

```python
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

cfg = Configurator(
    # ... other configuration ...
    
    # Use default calibrator sequence
    calibrators = default_calibrators_sequence
    
    # Configure shape variations
    variations = {
        "shape": {
            "common": {
                "inclusive": ["jet_calibration"],  # Run jet variations for all samples
            },
            "bysample": {
                "MC_Sample": {
                    "inclusive": ["electron_scale_and_smearing"],  # Run electron variations for specific samples
                }
            }
        }
    }
)
```

### Custom Calibrator Sequence

You can define your own calibrator sequence:

```python
from pocket_coffea.lib.calibrators.common import JetsCalibrator, METCalibrator
from your_module import CustomCalibrator

custom_sequence = [
    JetsCalibrator,
    METCalibrator, 
    CustomCalibrator
]

cfg = Configurator(
    calibrators = custom_sequence,
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

    def __init__(self, params, metadata, **kwargs):
        super().__init__(params, metadata, **kwargs)
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
    
    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
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
            
        return {
            "MyObject.pt": corrected_pt,
            "MyObject.mass": corrected_mass
        }
```

### Advanced Example with Dependencies

For calibrators that depend on other calibrators' output and/or on the original uncalibrated information. 

```python
class AdvancedCalibrator(Calibrator):
    name = "advanced_calibrator"
    has_variations = True
    isMC_only = True
    calibrated_collections = ["DerivedQuantity"]

    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        # Check dependencies
        if "jet_calibration" not in already_applied_calibrators:
            raise ValueError("This calibrator requires jet_calibration to be applied first")
        
        # Use original jets if needed for some calculation
        if "Jet" in orig_colls:
            original_jets = orig_colls["Jet"]
        
        # Use calibrated jets from events
        # Reading from "events" in practice is taking all the objects calibrated up to this point in the sequence.
        calibrated_jets = events["Jet"]
        
        # Compute derived quantity
        derived = self.compute_derived_quantity(original_jets, calibrated_jets, variation)
        
        return {"DerivedQuantity": derived}
```

## Systematic Variations

### Variation Naming Convention

Systematic variations should follow the pattern: `"{source}_{direction}"` where:
- `source`: describes the uncertainty source (e.g., "jec", "jer", "ele_scale")
- `direction`: either "Up" or "Down"

Examples:
- `"jet_jecUp"`, `"jet_jecDown"`
- `"ele_scaleUp"`, `"ele_scaleDown"`

### Configuration in Analysis

Variations are configured in the `variations.shape` section:

```python
variations = {
    "shape": {
        "common": {
            "inclusive": [
                "jet_calibration",           # All JEC/JER variations
                "electron_scale_and_smearing" # All electron variations
            ],
        },
        "bysample": {
            "TTbar": {
                "inclusive": ["custom_calibrator"],  # Sample-specific variations
            }
        }
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
        jme_factory=self.jmefactory,  # Additional arguments passed to calibrators
    )
```

### Variation Loop
```python
def loop_over_variations(self):
    for variation, events_calibrated in self.calibrators_manager.calibration_loop(
        self.events,
        variations_for_calibrators=self.cfg.available_shape_variations[self._sample]
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

