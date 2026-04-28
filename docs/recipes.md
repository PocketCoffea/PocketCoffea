# HOW-TOs for common tasks
:::{alert}
Page under construction! Come back for more common analysis steps recipes.
:::

## HLT trigger selection

## Define a new cut function

## Skimming events
Skimming NanoAOD events and save the reduced files on disk can speedup a lot the processing of the analysis. The recommended executor for the skimming process is the direct condor-job executor, which splits the workload in condor jobs without using the dask scheduler. This makes the resubmission of failed skim jobs easier. 

Follow these instructions to skim the files on EOS:
1. Add the `save_skimmed_files` argument to the configurator with a suitable folder name: e.g. `  save_skimmed_files = "root://eoscms.cern.ch//eos/cms/store/group/phys_higgs/ttHbb/Run3_semileptonic_skim/"`
    
2. It is recommended to run the processing on HTCondor at CERN using the new direct condor executor. That will send out standard jobs instead of using dask. Please make sure your dataset list is up-to-date before sending the jobs. 
   ```pocket-coffea run --cfg config_skim.py  -o output_skim_config -e condor@lxplus --scaleout NUMBEROFJOBS --chunksize 200000 --job-dir jobs --job-name skim --queue workday --dry-run``` . Use the `--dry-run` option to check the job splitting configuration and remove it when you are happy to submit the jobs.

3. Check the status of the jobs with `pocket-coffea check-jobs -j jobs-dir/skim`.  Optionally activate the automatic resubmitting option to resubmit failed jobs. 

4. Once done, we usually do an hadd to sum all the small files produced by each saved chunk. An utility script to compute the groups and correctly hadd them is available `pocket-coffea hadd-skimmed-files -fl ../output_total.coffea -o root://eoscms.cern.ch//eos/cms/store/group/phys_higgs/ttHbb/Run3_dileptonic_skim_hadd -e 400000 --dry  -s 6 `
   this script creates some files to be able to send out jobs that runs the hadd for each group of files.

5. From all this process you will get out at the end an updated `dataset_definition_file.json` to be used in your analysis config.

## Subsamples
WIP


### Primary dataset cross-cleaning
WIP


## Define a custom weight
WIP

### Define a custom weights with custom variations
WIP

## promptMVA lepton selection and on-the-fly re-evaluation

The promptMVA (`mvaTTH`) is a BDT-based lepton ID used in ttH analyses to suppress
non-prompt leptons. For Run3 2022–2023 datasets produced with NanoAODv12/v13, the
`mvaTTH` branch stored in NanoAOD was trained on a different data-taking period and
must be **re-evaluated on the fly** before applying the working-point cut. For 2024
datasets (NanoAODv15) the value is correct out of the box.

PocketCoffea provides:
- `ElectronMVAEvaluator` / `MuonMVAEvaluator` — XGBoost wrappers in
  `pocket_coffea.lib.xgboost_evaluator` that reproduce the TMVA score in `[-1, 1]`.
- `lepton_selection_promptMVA` — drop-in replacement for `lepton_selection` in
  `pocket_coffea.lib.leptons` that applies the full preselection and optional MVA WP cut.
- Pre-configured model weights and scale factors in
  `pocket_coffea/parameters/lepton_scale_factors.yaml` under
  `lepton_scale_factors.{electron,muon}_sf.promptMVA_jsons`.

### Object preselection parameters

The `lepton_selection_promptMVA` function requires several additional keys under
`object_preselection` compared to the standard `lepton_selection`. Add these to your
`params/object_preselection.yaml`:

```yaml
object_preselection:
  Electron:
    pt: 15.0
    eta: 2.5
    sip3d: 8.0          # 3D impact-parameter significance
    lostHits: 1         # max number of lost tracker hits
    dxy: 0.05           # |dxy| < dxy [cm]
    dz: 0.1             # |dz| < dz [cm]
    id:                 # NanoAOD field used as MVA score (per year)
      "2022_preEE":    mvaTTH_redo
      "2022_postEE":   mvaTTH_redo
      "2023_preBPix":  mvaTTH_redo
      "2023_postBPix": mvaTTH_redo
      "2024":          mvaTTH           # read directly from NanoAOD in v15
    mva_wp:             # MVA working-point threshold (per year)
      "2022_preEE":    0.90
      "2022_postEE":   0.90
      "2023_preBPix":  0.90
      "2023_postBPix": 0.90
      "2024":          0.90
    btag_cut:           # DeepFlavB score veto on the closest jet (per year)
      2022_preEE: 0.3086
      2022_postEE: 0.3196
      2023_preBPix: 0.2431
      2023_postBPix: 0.2435 
      "2024": 999.

  Muon:
    pt: 10.0
    eta: 2.4
    iso: 0.4
    sip3d: 8.0
    dxy: 0.05
    dz: 0.1
    base_id: looseId    # NanoAOD field for baseline muon ID
    id:
      "2022_preEE":    mvaTTH_redo
      "2022_postEE":   mvaTTH_redo
      "2023_preBPix":  mvaTTH_redo
      "2023_postBPix": mvaTTH_redo
      "2024":          mvaTTH
    mva_wp:
      "2022_preEE":    0.64
      "2022_postEE":   0.64
      "2023_preBPix":  0.64
      "2023_postBPix": 0.64
      "2024":          0.64
    btag_cut:
      2022_preEE: 0.3086
      2022_postEE: 0.3196
      2023_preBPix: 0.2431
      2023_postBPix: 0.2435 
      "2024": 999.
```

### Workflow implementation

In `apply_object_preselection`, first compute scores for 2022–2023 years and attach
them as `mvaTTH_redo`, then call `lepton_selection_promptMVA` with `apply_mva_cut=True`:

```python
import awkward as ak
from pocket_coffea.workflows.base import BaseProcessorABC
from pocket_coffea.lib.objects import jet_selection, btagging
from pocket_coffea.lib.leptons import lepton_selection_promptMVA
from pocket_coffea.lib.xgboost_evaluator import ElectronMVAEvaluator, MuonMVAEvaluator

REDO_MVA_YEARS = ["2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix"]


class MyProcessor(BaseProcessorABC):

    def apply_object_preselection(self, variation):
        # Re-evaluate promptMVA on the fly for 2022/2023 datasets
        if self._year in REDO_MVA_YEARS:
            ele_evaluator = ElectronMVAEvaluator(
                self.params.lepton_scale_factors.electron_sf.promptMVA_jsons[self._year].model_weights
            )
            muon_evaluator = MuonMVAEvaluator(
                self.params.lepton_scale_factors.muon_sf.promptMVA_jsons[self._year].model_weights
            )

            _, presel_ele_mask = lepton_selection_promptMVA(
                self.events, "Electron", self.params, self._year, apply_mva_cut=False
            )
            self.events["Electron"] = ak.with_field(
                self.events["Electron"],
                ele_evaluator.evaluate(self.events["Electron"], self.events["Jet"], mask=presel_ele_mask),
                "mvaTTH_redo",
            )

            _, presel_muon_mask = lepton_selection_promptMVA(
                self.events, "Muon", self.params, self._year, apply_mva_cut=False
            )
            self.events["Muon"] = ak.with_field(
                self.events["Muon"],
                muon_evaluator.evaluate(self.events["Muon"], self.events["Jet"], mask=presel_muon_mask),
                "mvaTTH_redo",
            )

        # Apply final selection using the (re-)computed MVA score
        self.events["ElectronGood"], _ = lepton_selection_promptMVA(
            self.events, "Electron", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Electron.id[self._year],
        )
        self.events["MuonGood"], _ = lepton_selection_promptMVA(
            self.events, "Muon", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Muon.id[self._year],
        )

        leptons = ak.with_name(
            ak.concatenate((self.events.MuonGood, self.events.ElectronGood), axis=1),
            name="PtEtaPhiMCandidate",
        )
        self.events["LeptonGood"] = leptons[ak.argsort(leptons.pt, ascending=False)]
        self.events["JetGood"], self.jetGoodMask = jet_selection(
            self.events, "Jet", self.params, self._year, leptons_collection="LeptonGood"
        )
        self.events["BJetGood"] = btagging(
            self.events["JetGood"],
            self.params.btagging.working_point[self._year],
            wp=self.params.object_preselection.Jet["btag"]["wp"],
        )
```

### Scale factors

The corresponding promptMVA lepton ID scale factors are provided as the
`sf_ele_promptMVA` and `sf_mu_promptMVA` weight wrappers, already included in
`common_weights`. Add them to the `weights` section of your configurator:

```python
weights = {
    "common": {
        "inclusive": [
            "genWeight", "lumi", "XS",
            "sf_ele_promptMVA",
            "sf_mu_promptMVA",
        ],
    },
}
```

:::{note}
For 2022–2023 the scale factors are derived from the TTH multilepton team and stored
under `pocket_coffea/parameters/custom_SFs/{electron,muon}_promptMVA/`. For 2024
(NanoAODv15) central POG scale factors are used automatically.
:::

## Apply corrections
Here we describe how to apply certain corrections recommended by CMS POGs.

### MET-xy
From a purely physical point of view, the distribution of the $\phi$-component of the missing transverse momentum (a.k.a. MET) should be uniform due to rotational symmetry. However, for a variety of detector-related reasons, the distribution is not uniform in practice, but shows a sinus-like behavior. To correct this behavior, the x- and y-component of the MET can be altered in accordance to the recommendation of JME. In the PocketCoffea workflow, these corrections can be applied using the `met_xy_correction()` function:

```
from pocket_coffea.lib.jets import met_xy_correction
met_pt_corr, met_phi_corr = met_xy_correction(self.params, self.events, self._year, self._era)
```  
Note, that this shift also alters the $p_\mathrm{T}$ component! Also, the corrections are only implemented for Run2 UL (thus far).

### Jet calibration configuration

PocketCoffea provides a flexible jet calibration system that handles Jet Energy Corrections (JEC), Jet Energy Resolution (JER), and systematic variations. The calibration is configured through the `jets_calibration.yaml` parameters file and applied using specialized calibrator classes.

#### Core Concepts

The jet calibration system is built around three main concepts:

1. **Jet Types**: Abstract jet algorithm/configuration identifiers (e.g., `AK4PFchs`, `AK4PFPuppi`, `AK8PFPuppi`)
2. **Jet Collections**: Actual NanoAOD collections that store jet objects (e.g., `Jet`, `FatJet`)
3. **Factory Configurations**: JEC/JER correction files and settings associated with each jet type

The system maps jet types to collections and applies the appropriate corrections based on data-taking period, MC/Data status, and systematic variation requests.

#### Available Calibrators

**JetsCalibrator**: Standard JEC/JER calibrator for regular jet collections
- Applies JEC (L1FastJet, L2Relative, etc.) and JER corrections
- Handles systematic variations (JES uncertainties, JER variations)
- Works with both MC and Data
- Can apply the pT regression to some jets collections before calibration.


#### Configuration Structure

The configuration is organized in several sections:

##### Factory Configuration
Defines the correction files for each jet type, data-taking period, and correction level:

```yaml
default_jets_calibration:
  factory_config_clib:
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

      2016_PostVFP:
        json_path: ${cvmfs:Run2-2016postVFP-UL-NanoAODv9,JME,jet_jerc.json.gz}
        jec_mc: Summer19UL16_V7_MC
        jec_data: Summer19UL16_RunFGH_V7_DATA
        jer: Summer20UL16_JRV3_MC
        level: L1L2L3Res

    ....
    AK4PFPuppi:
      2022_preEE:
        json_path: ${cvmfs:Run3-22CDSep23-Summer22-NanoAODv12,JME,jet_jerc.json.gz}
        jec_mc: Summer22_22Sep2023_V3_MC
        jec_data: Summer22_22Sep2023_RunCD_V3_DATA
        jer: Summer22_22Sep2023_JRV1_MC
        level: L1L2L3Res
      
      2022_postEE:
        json_path: ${cvmfs:Run3-22EFGSep23-Summer22EE-NanoAODv12,JME,jet_jerc.json.gz}
        jec_mc: Summer22EE_22Sep2023_V3_MC
        jec_data:
          E: Summer22EE_22Sep2023_RunE_V3_DATA
          F: Summer22EE_22Sep2023_RunF_V3_DATA
          G: Summer22EE_22Sep2023_RunG_V3_DATA
        jer: Summer22EE_22Sep2023_JRV1_MC
        level: L1L2L3Res

```

##### Collection Mapping
Maps jet types to actual NanoAOD collections for each data-taking period:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK8PFPuppi: "FatJet"
      # AK4PFPuppiPNetRegression: "Jet"
```

The jet type is just an internal labels used in the PocketCoffea configuration to link various pieces of the jets configuration together. 

:::{warning}
All the collections defined in the `jets_calibration.collection` entry will be calibrated by the configured JetsCalibrator if included in the calibrators sequence. It is not allowed to match the same jet collection to multiple jet types: an error will be raised.
:::

##### Collection Name Aliases
To allow to use the same calibration settings on different ket collections, it is possible to define aliases for the collection names with the `collection_name_alias` key. For example, if you have a jet collection called `JetCustom` that you want to calibrate in the same way as you do for the `Jet` collection, which is mapped to the `AK4PFPuppi` jet type, your configuration would look like this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK4PFPuppiCustom: "JetCustom"
  collection_name_alias:
      2022_preEE:
        AK4PFPuppiCustom : "AK4PFPuppi"
```

:::{warning}
In order to merge the variations of the `JetCustom` collection, you need to define a `merge_collections_for_variations` entry as specified in section [Merging Systematic Variations](#merging-systematic-variations).
:::

##### Calibration Control Flags
Enable/disable different correction types per jet type and period:

```yaml
  apply_jec_MC:           # Apply JEC to MC
    2022_preEE:
      AK4PFPuppi: True
      AK4PFPuppiPNetRegression: True

  apply_jer_MC:           # Apply JER to MC
    2022_preEE:
      AK4PFPuppi: True
      AK4PFPuppiPNetRegression: True
      
  apply_jec_Data:         # Apply JEC to Data
    2022_preEE:
      AK4PFPuppi: True
      
  apply_pt_regr_MC:       # Apply pT regression to MC
    2022_preEE:
      AK4PFPuppi: False
      AK4PFPuppiPNetRegression: True

  sort_by_pt:
    2016_PreVFP:
      AK4PFchs: True
      AK8PFPuppi: True
    2016_PostVFP:
      AK4PFchs: True
      AK8PFPuppi: True  
```

##### Systematic Variations
Different sets of systematic variations are available in the default parameter set. 

```yaml
default_jets_calibration:
  variations:
    full_variations:      # All individual JES sources
      AK4PFPuppi:
        2022_preEE:
          - JES_Regrouped_Absolute
          - JES_Regrouped_FlavorQCD
          - JER
    total_variation:      # Total JES uncertainty
      AK4PFPuppi:
        2022_preEE:
          - JES_Total
          - JER
```

The set of variations to be used has to be setup in the `jets_calibration.variation` key. For example: 

```yaml
jets_calibration:
  variations: "${default_jets_calibration.variations.total_variation}"

```

The user can also enable only some of the variations for different data taking periods

```yaml
jets_calibration:
  variations:
    2022_preEE:
      AK4PFPuppi:
        - JES_Total  # Only total JES uncertainty
        - JER        # JER variations
```

##### Merging Systematic Variations
By default, each systematic variation produces a separate collection of calibrated jets. To reduce the number of collections, variations can be merged into a single collection per jet type. This is configured using the `merge_collections_for_variations` key, which specifies which jet types should have their variations merged and under which new name:, e.g.:

```yaml
jets_calibration:
  merge_collections_for_variations:
    2022_preEE:
      AK4Jet: 
        - AK4PFPuppi
        - AK4PFPuppiPNetRegression
        - AK4PFPuppiCustom
```

This will create variation with the name `AK4Jet_{variation}_[up|down]` that merges the variations from the specified jet types.

#### MET Recalibration
Configure MET corrections that propagate jet calibration changes:

```yaml
  rescale_MET_config:
    2022_preEE:
      apply: True
      MET_collection: "PuppiMET"
      Jet_collection: "Jet"
```

#### Usage in Analysis

The jet calibration is typically applied automatically when using the standard calibrator sequence:

```python
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

# Default sequence includes: JetsCalibrator, METCalibrator, ElectronsScaleCalibrator
calibrators = default_calibrators_sequence
```

For custom configurations, modify the `jets_calibration` section in your parameters:

```yaml
jets_calibration:
  # Use a different variation set
  variations: "${default_jets_calibration.variations.full_variations}"
  
  # Enable specific jet types
  collection:
    2022_preEE:
      AK4PFPuppiCustomSetOfCorrections: "Jet"
```


### Jet energy regression
Starting from Run3 datasetes the ParticleNet jet energy regression corrections are part of the `Jet` object in NanoAOD. But they are not applied by default. In PocketCoffea the regression can be turned On/Off via configuration by using the `JetPtRegressionCalibrator` in the calibration sequence and by activating the pt regression in the jets calibration configuration

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppiPNetRegression: "Jet"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "Jet"
      
    2022_postEE:
      AK4PFPuppiPNetRegression: "Jet"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "Jet"

    2023_preBPix:
      AK4PFPuppiPNetRegression: "Jet"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "Jet"

    2023_postBPix:
      AK4PFPuppiPNetRegression: "Jet"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "Jet"

  apply_pt_regr_MC:
	2022_preEE:
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2022_postEE: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2023_preBPix: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2023_postBPix: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True

  apply_pt_regr_Data:
    2022_preEE: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2022_postEE:
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2023_preBPix: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
    2023_postBPix: 
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
```

The default jets configuration is overwritten to assign the `Jet` collection to the `AK4PFPuppiPNetRegression` tag, and to activate the pt regression for data and MC for that tag. 


If the user needs to apply regression only to a subset of Jets, then the best strategy is to define a copy of the Jet collection and calibrate that. 

An example configuration for this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"
      
    2022_postEE:
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

    2023_preBPix:
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

    2023_postBPix:
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

object_preselections:
    Jet:
        pt: 20
        eta..

    JetPtReg:
        pt: 20
        eta: ...
        btag:
          wp:
            M
```

This clone of the Jet collection needs to be defined in the `process_extra_after_skim` function of the user's workflow

```python
from pocket_coffea.workflows.base import BaseProcessorABC


class PtRegrProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def process_extra_after_skim(self):
        # Create extra Jet collections for testing
        self.events["JetPtReg"] = ak.copy(self.events["Jet"])
        # self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])
```

Further references:  
* The analysis note: [AN-2022/094](https://cms.cern.ch/iCMS/jsp/db_notes/noteInfo.jsp?cmsnoteid=CMS%20AN-2022/094)
* Measuring response in Z+b events: [presentation](https://indico.cern.ch/event/1451196/contributions/6181213/attachments/2949253/5183620/cooperstein_HH4b_oct162024.pdf)

#### Merge regressed and standard jet pT
In some cases, e.g. PNet regression in NanoAODv12, the regression can be applied only to a subset of jets (e.g. cutting on pT and eta of the jet). In this case, one may want to merge the regressed pT values with the standard pT values for jets failing the regression criteria. In order to do this, your configuration would look like this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

```

The merging of the pT values can be done in the user's workflow, e.g. in the `apply_object_preselection` section:

```python
from pocket_coffea.workflows.base import BaseProcessorABC


class PtRegrProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def process_extra_after_skim(self):
        # Create extra Jet collections for testing
        self.events["JetPtReg"] = ak.copy(self.events["Jet"])
        #self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])

    def apply_object_preselection(self, variation):
        # Use the regressed pt from PNet collection if available,
        # otherwise use the JEC corrected pt collection
        # This way we consider correctly all fields which change depending on
        # the pt definition, namely the pt, mass and the associated systematic variations
        self.events["Jet"] = ak.where(
            self.events["JetPtReg"].pt > 0,
            self.events["JetPtReg"],
            self.events.Jet,
        )
```

:::{warning}
In order to merge the variations of the `Jet` and `JetPtReg` collections, you need to define a `merge_collections_for_variations` entry as specified in section [Merging Systematic Variations](#merging-systematic-variations).
:::

:::{warning}
When merging the collections like this, make sure to set the `sort_by_pt` option to `False` for the jet typea in the jets calibration configuration, otherwise the jet ordering will be changed and the merging will fail.
:::


## Create a custom executor to use `onnxruntime`

This example shows running on CERN lxplus and assumes a prior understanding of how to load and use an ML model with onnxruntime. For more examples see the executors in the ttHbb analysis [here](https://github.com/PocketCoffea/AnalysisConfigs/tree/main/configs/ttHbb/semileptonic/common/executors)

At the time of writing, `onnxruntime` is not installed in the singularity container, which means that you will need to run with a custom environment. Instructions for this are given in [Running the analysis](./running.md)

The following code is a custom executor which is meant to be filled in with details such as the path to the `model.onnx` file and options used in the `InferenceSession`.
```python
from pocket_coffea.executors.executors_lxplus import DaskExecutorFactory
from dask.distributed import WorkerPlugin, Worker, Client


class WorkerInferenceSessionPlugin(WorkerPlugin):
    def __init__(self, model_path, session_name):
        super().__init__()
        self.model_path = model_path
        self.session_name = session_name

    async def setup(self, worker: Worker):
        import onnxruntime as ort

        session = ort.InferenceSession(
            self.model_path,
            # Whatever other options you use
            providers=["CPUExecutionProvider"],
        )
        worker.data["model_session_" + self.session_name] = session


class OnnxExecutorFactory(DaskExecutorFactory):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def setup(self):
        super().setup()
        self.Model = "absolute/path/to/your/model.onnx"
        inference_session_plugin = WorkerInferenceSessionPlugin(self.Model, "ModelName")
        self.dask_client.register_plugin(inference_session_plugin)

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()


def get_executor_factory(executor_name, **kwargs):
    return OnnxExecutorFactory(**kwargs)
```

To use the model to process events in the `workflow.py` file, one would do something like this. See [here](https://github.com/PocketCoffea/AnalysisConfigs/blob/main/configs/ttHbb/semileptonic/sig_bkg_classifier/workflow_test_spanet.py) for another example.
```python
# import the get_worker function
from dask.distributed import get_worker

# Rest of workflow


# Suppose you want to apply your model after preselection. You would do e.g.
def process_extra_after_presel():
    try:
        worker = get_worker()
    except ValueError:
        worker = None

    # Whatever needs to be done to prepare the inputs to the model

    if worker is None:
        # make it work running locally too
        import onnxruntime as ort

        session = ort.InferenceSession(
            self.model_path,
            # Whatever other options you use
            providers=["CPUExecutionProvider"],
        )
    else:
        session = worker.data["model_session_ModelName"]

    # Continue as you normally would when using an ML model with onnxruntime, e.g.
    model_output = session.run(
        # inputs and options
    )
```
To run with the custom executor, assuming the file is called `custom_executor.py`, one replaces `--executor dask@lxplus` with `--executor-custom-setup custom_executor.py` for example:
```
pocket-coffea run --cfg myConfig.py -o outputDir -ro run_options.yaml --executor-custom-setup custom_executor.py
```

Lastly, the custom executor will print a lot of `INFO` level log messages which are suppressed when running with the built-in pocket-coffea executors. To suppress these for all executors, create a file `~/.config/dask/logging.yaml` and add the following:
```yaml
logging:
  distributed: warning
  distributed.client: warning
  distributed.worker: warning
  distributed.nanny: warning
  distributed.scheduler: warning
```

## Split large outputs into categories

If your configuration contains a large number of categories, variables, and systematics, the number of histograms can grow very large. Although on disk, the size of the *merged* `output_all.coffea` is usually less than O(10 GB), the full accumulation process can consume around O(100 GB) of RAM. This seems unavoidable as coffea accumulation necessarily happens on memory. This can be addressed in one of two ways:

- The `merge-output` script dumps partial `.coffea` outputs whenever memory usage exceeds 50% of the available RAM on the machine. However, this means one still has to use a different large-memory machine to merge them into one `.coffea` file and/or read them all into memory during plotting. The fragmented `.coffea` dumps consume less space on disk and are fewer in number, so it is easier to `scp` them to other machines using this approach.

- A more efficient solution is to split outputs into "category groups" (i.e. channels or regions of the analysis) and merge/process only one group of categories at one time. Since plots are typically made per channel, this lets one do everything without loading multiple caetegory-grouped files into the memory.

Currently, the second solution is implemented only for the `condor@lxplus` executor. It can be utilized as follows:
  * `runner`: Pass `--split-by-category` to `runner` (actually gets passed to the executor parameters). The output from each job is then further split to contain 8 categories per output file, so each job produces `n_groups = n_categories/8` output files. This is handled through the `split-output` command, which in turn calls `utils.filter_output.filter_output_by_category`.
  * `merge-outputs`: Handles the merging per category automatically (no extra flag needed) if `--split-by-category` was passed to `runner`. This will produce `n_groups` merged outputs, containing mutually exclusive groups of categories.
  * `make-plots`: Pass `--split-by-category` flag to handle only the category-wise merged outputs.
