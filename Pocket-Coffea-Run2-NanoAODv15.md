# Run2 NanoAODv15 Configuration Update Documentation

## 1. Overview

This document summarizes the configuration changes introduced to support CMS Run2 NanoAODv15 samples in the Pocket-Coffea analysis setup.

The analysis was originally performed using Run2 NanoAODv9 files. The configuration was extended to support Run2 NanoAODv15 samples in order to use the improved b-tagging setup available in NanoAODv15.

New NanoAODv15-specific year labels were added throughout the configuration, for example:

- `2016_PreVFP_v15`
- `2016_PostVFP_v15`
- `2017_v15`
- `2018_v15`


---

## 2. Configuration Files and Changes

### 2.1 `btagging`

Correct b-tagging working points for Run2 NanoAODv15 were added.

The working points were taken from the BTV wiki.

The corresponding reference links were added to the YAML configuration file.


---

### 2.2 `event_flags`

The event flags are the same as in the Run2 NanoAODv9 configuration.

The only required change was to add the `_v15` year labels correctly.

No NanoAODv15-specific event flag changes were applied.

---

### 2.3 `executor_options_defaults`

No changes were required.

The existing configuration is used unchanged.

---

### 2.4 `lumi`

The luminosity values are the same as in the Run2 NanoAODv9 configuration.

No NanoAODv15-specific luminosity changes were required.

The luminosity labels used for plotting were updated separately in the plotting style configuration.

---

### 2.5 `nano version`

NanoAOD version 15 was assigned to all newly added `_v15` years.

---

### 2.6 `met_xy`

The MET XY corrections are the same as in the Run2 NanoAODv9 configuration.

No NanoAODv15-specific changes were required for this part of the setup.

---

### 2.7 `variations`

The systematic variation setup is the same as for `2016_PreVFP`.

All years and NanoAOD versions use the same `sf_btag` and `sf_ctag` weight variations.

Even though c-tagging scale factors are currently not relevant for the analysis, the general variation structure remains consistent with the existing configuration.

---

### 2.8 `photon_scale_factors`

The NanoAODv15 eras were mapped to the corresponding NanoAODv9 eras.

The same JSON files as in the Run2 NanoAODv9 configuration are used.

No dedicated NanoAODv15 photon scale factor files were added.

---

### 2.9 `plotting_style`

Luminosity year labels were added according to the luminosity configuration.

The labels are the same as in the Run2 NanoAODv9 setup.

---

### 2.10 `MET calibration`

The MET calibration setup for the newly added years is the same as in the 2024 configuration.

---

## 3. Lepton Scale Factors

### 3.1 `electron_sf`

The era mapping for electron scale factors was kept unchanged.

For NanoAODv15, electron scale and smearing corrections were enabled.

The setting

~~~yaml
scale_and_smearing:
  v15: true
~~~

was added or activated.

The configured correction is:

~~~text
electronSS_EtDependent
~~~

The scale correction is configured as:

~~~text
Scale
~~~

The smearing and systematic correction is configured as:

~~~text
SmearAndSyst
~~~

The JSON files were taken from the CMS Analysis Corrections documentation for Run2-2018-UL NanoAODv15 EGM corrections:

~~~text
https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run2-2018-UL-NanoAODv15/EGM/2025-12-05/#electronjsongz
~~~

---

### 3.2 `muon_sf`

Muon scale factors were configured for:

~~~text
MuonGood
~~~

The included muon corrections are:

- identification scale factors
- isolation scale factors
- trigger scale factors

The JSON files are the same as in the previous setup.

---

### 3.3 `rochester`

Rochester corrections are applied.

The same Rochester correction files as in the Run2 NanoAODv9 configuration are used.

---

### 3.4 Muon scale and resolution

Muon scale and resolution corrections are disabled.

The corresponding setting is:

~~~yaml
scale_and_resolution: false
~~~

---

## 4. Jet Calibration

### 4.1 Correctionlib factory configuration

The correctionlib factory configuration for

~~~text
clib/ak4PFPuppi
~~~

was added or updated.

The configuration was derived from the CMS Analysis Corrections documentation for Run2 NanoAODv15 JME corrections:

~~~text
https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run2-2018-UL-NanoAODv15/JME/2026-06-05/#jet_jercjsongz
~~~

---

### 4.2 AK8 configuration

The AK8 configuration is imported from the AK4 configuration.

This follows the same approach as in the 2024 configuration.

---

### 4.3 Jet energy correction variations

#### Full variations

The full JEC/JER variations are the same as in the 2024 configuration.

The year names were adapted to match the Run2 NanoAODv15 naming used for `AK4PFPuppi`.

For `AK8PFPuppi`, the full variations are imported from the AK4 dictionary.

This follows the same approach as in the 2024 configuration.

#### Total variations

The total JEC/JER variations are the same as in the 2024 configuration for `AK4PFPuppi`.

For `AK8PFPuppi`, the total variations are imported from the AK4 dictionary.

This also follows the same approach as in the 2024 configuration.

---

### 4.4 `jet_calibration`

The `jet_types` configuration was completed and does not need further edits.

The following settings are the same as in the 2024 configuration:

- `collection`
- `collection_name_alias`
- `merge_collections_for_variations`
- `sort_by_pt`
- `apply_jec_MC`
- `apply_jer_MC`
- `apply_jer_data`
- `apply_pt_regr_MC`
- `apply_pt_regr_data`
- `apply_jec_msoftdrop_MC`


---

## 5. Jet Scale Factors

### 5.1 B-tagging scale factors

B-tagging scale factors were added according to the CMS Analysis Corrections documentation.

These scale factors correspond to the NanoAODv15 b-tagging setup.

This is a central part of the Run2 NanoAODv15 update.

---

### 5.2 C-tagging scale factors

C-tagging scale factors were not added.

They are currently irrelevant for this analysis.

Additionally, no corresponding `json.gz` correction file was available in the corrections wiki at the time of implementation.

---

### 5.3 Jet pileup ID

Jet pileup ID scale factors were added.

The setup is the same as in the Run2 NanoAODv9 configuration.

---

### 5.4 Jet veto maps

Jet veto maps were added according to the CMS Analysis Corrections documentation.

The corresponding correction links were added in the configuration file.

---

## 6. Summary Table

| Area | Change for Run2 NanoAODv15 | Reference setup |
| --- | --- | --- |
| `btagging` | Added correct Run2 NanoAODv15 working points | BTV wiki |
| `event_flags` | Added `_v15` year labels | Run2 NanoAODv9 |
| `executor_options_defaults` | No changes | Existing setup |
| `lumi` | Reused luminosities | Run2 NanoAODv9 |
| `nano version` | Added NanoAOD version 15 for `_v15` years | NanoAODv15 setup |
| `met_xy` | Reused MET XY corrections | Run2 NanoAODv9 |
| `variations` | Reused `sf_btag` and `sf_ctag` variation structure | Existing setup |
| `photon_scale_factors` | Mapped v15 eras to v9 eras | Run2 NanoAODv9 |
| `plotting_style` | Added luminosity year labels | Run2 NanoAODv9 |
| `MET calibration` | Reused setup | 2024 |
| `electron_sf` | Enabled NanoAODv15 scale and smearing corrections | NanoAODv15 EGM corrections |
| `muon_sf` | Reused ID, isolation, and trigger setup | Existing setup |
| `rochester` | Applied Rochester corrections using same files as before | Run2 NanoAODv9 |
| Muon scale/resolution | Disabled | Existing setup |
| Jet calibration | Added/updated AK4 PFPuppi correctionlib setup | NanoAODv15 JME corrections / 2024 setup |
| AK8 jets | Imported configuration from AK4 | 2024 |
| Jet variations | Reused 2024 variations with adapted year names | 2024 |
| B-tagging SF | Added according to corrections documentation | NanoAODv15 corrections |
| C-tagging SF | Not added; irrelevant for this analysis and unavailable in corrections wiki | Not used |
| Jet pileup ID | Added | Run2 NanoAODv9 |
| Jet veto maps | Added according to corrections documentation | NanoAODv15 corrections |

---

## 7. Notes for Future Developers

The Run2 NanoAODv15 years were introduced by extending the existing year definitions with `_v15` suffixes.

The intended NanoAODv15 Run2 year labels are:

- `2016_PreVFP_v15`
- `2016_PostVFP_v15`
- `2017_v15`
- `2018_v15`

The configuration intentionally reuses Run2 NanoAODv9 settings where the relevant correction did not change with the NanoAOD version.

The configuration intentionally reuses 2024 settings where the Run2 NanoAODv15 setup is structurally closer to the NanoAODv15-based 2024 setup.

The b-tagging configuration was explicitly updated with correct Run2 NanoAODv15 working points and b-tagging scale factors.

C-tagging scale factors are currently not used because c-tagging is irrelevant for the analysis.

C-tagging scale factors may be added later if official NanoAODv15 correction files become available in the corrections wiki.

Future updates should verify that the linked correction JSON files, working points, and era mappings remain consistent with the official CMS correction recommendations.

---


## 9. External References

### B-tagging

Working points were taken from the BTV wiki.

The corresponding links are stored in the YAML configuration file.

### Electron corrections

~~~text
https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run2-2018-UL-NanoAODv15/EGM/2025-12-05/#electronjsongz
~~~

### Jet calibration corrections

~~~text
https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run2-2018-UL-NanoAODv15/JME/2026-06-05/#jet_jercjsongz
~~~

### Jet veto maps and b-tagging scale factors

The corresponding links are stored in the relevant configuration files.

---

## 10. Status

The Run2 NanoAODv15 configuration update is considered complete for the current analysis.

The confirmed setup includes:

- Run2 NanoAODv15 year labels
- NanoAOD version 15 mapping
- updated b-tagging working points
- updated b-tagging scale factors
- updated electron scale and smearing corrections
- updated jet calibration setup
- updated jet veto maps
- reused Run2 NanoAODv9 luminosity, event flags, MET XY corrections, photon scale factors, and selected scale factor setups
- reused 2024-style MET calibration and jet calibration structure where appropriate

C-tagging scale factors are intentionally not included at the moment.