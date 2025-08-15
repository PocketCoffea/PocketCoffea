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

:::{warning}
The system uses a pickled factory file (`jets_calibrator_JES_JER_Syst.pkl.gz`) that contains pre-compiled correction functions. If you modify the factory configuration, you may need to regenerate this file using the factory building utilities in PocketCoffea.
:::

#### Available Calibrators

**JetsCalibrator**: Standard JEC/JER calibrator for regular jet collections
- Applies JEC (L1FastJet, L2Relative, etc.) and JER corrections
- Handles systematic variations (JES uncertainties, JER variations)
- Works with both MC and Data

**JetsPtRegressionCalibrator**: Calibrator that applies ML-based pT regression before JEC
- Supports ParticleNet (`PNet`) and UParTAK4 regression algorithms
- Applied to jet types with "Regression" in their name
- Regression applied selectively based on b-tagging scores
- Followed by standard JEC corrections

**METCalibrator**: Propagates jet corrections to MET
- Recalculates MET based on differences between uncalibrated and calibrated jets
- Must be applied after jet calibration

#### Configuration Structure

The configuration is organized in several sections:

##### Factory Configuration
Defines the correction files for each jet type, data-taking period, and correction level:

```yaml
default_jets_calibration:
  factory_configuration_MC:
    AK4PFchs:
      JES_JER_Syst:  # Full corrections with systematics
        2016_PreVFP:
          - "path/to/L1FastJet_file.jec.txt.gz"
          - "path/to/L2Relative_file.jec.txt.gz"
          - "path/to/UncertaintySources_file.junc.txt.gz"
          - "path/to/JER_SF_file.jersf.txt.gz"
      JES_JER_noJESSyst:  # JEC+JER without JES systematics
      JES_noJER:          # JEC only, no JER
```

##### Collection Mapping
Maps jet types to actual NanoAOD collections for each data-taking period:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK8PFPuppi: "FatJet"
      AK4PFPuppiPNetRegression: "Jet"
```

##### Calibration Control Flags
Enable/disable different correction types per jet type and period:

```yaml
  apply_jec_MC:           # Apply JEC to MC
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
```

##### Systematic Variations
Define which systematic variations to include:

```yaml
  variations:
    full_variations:      # All individual JES sources
      2022_preEE:
        AK4PFPuppi:
          - JES_Absolute
          - JES_FlavorQCD
          - JER
    total_variation:      # Total JES uncertainty
      2022_preEE:
        AK4PFPuppi:
          - JES_Total
          - JER
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
from pocket_coffea.lib.calibrators import default_calibrators_sequence

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
      AK4PFPuppiCustomSetOfCorrections: "Jet"  # Enable PNet regression
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

  apply_pt_regr_Data:
    apply_pt_regr_MC:
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

The implementation is based on [this presentation](https://indico.cern.ch/event/1476286/contributions/6220149/subcontributions/514978/attachments/2965734/5217706/PNetRegDiscussion_MKolosova_12Nov2024.pdf) from HH4b folks.

If the use need to apply the regression only on a subset of the Jets, the best strategy is to defined a copy of the Jet collection and calibrate that. 

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
        #self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])


```


Further references:  
* The analysis note: [AN-2022/094](https://cms.cern.ch/iCMS/jsp/db_notes/noteInfo.jsp?cmsnoteid=CMS%20AN-2022/094)
* Measuring response in Z+b events: [presenation](https://indico.cern.ch/event/1451196/contributions/6181213/attachments/2949253/5183620/cooperstein_HH4b_oct162024.pdf)


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
            #Whatever other options you use
            providers=["CPUExecutionProvider"]
        ) 
        worker.data["model_session_"+self.session_name] = session

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
#import the get_worker function
from dask.distributed import get_worker

#Rest of workflow 

#Suppose you want to apply your model after preselection. You would do e.g.
def process_extra_after_presel()
    try:
        worker = get_worker()
    except ValueError:
        worker = None

    #Whatever needs to be done to prepare the inputs to the model

    if worker is None:
        #make it work running locally too
        import onnxruntime as ort
        session = ort.InferenceSession(
            self.model_path,
            #Whatever other options you use
            providers=["CPUExecutionProvider"]
        )
    else:
        session = worker.data["model_session_ModelName"]
		
    #Continue as you normally would when using an ML model with onnxruntime, e.g.
    model_output = session.run(
        #inputs and options   
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