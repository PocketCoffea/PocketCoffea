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

### Jet energy regression
Starting from Run3 datasetes the ParticleNet jet energy regression corrections are part of the `Jet` object in NanoAOD. But they are not applied by default. In PocketCoffea the regression can be turned On/Off via configuration in the `Jet` object of `object_preselection.yaml` as follows:

```yaml
object_preselection:
  ...
  Jet:
	pt: 20
    ...
    regression:
      do: True
      cut_btagB: 0.12
      cut_btagCvL: 0.12
```

The corrections are applied in `jet_correction()` method in `lib/jets.py`. The implementation is based on [this presentation](https://indico.cern.ch/event/1476286/contributions/6220149/subcontributions/514978/attachments/2965734/5217706/PNetRegDiscussion_MKolosova_12Nov2024.pdf) from HH4b folks.

In principle this regression was designed to be applied to all jets (heavy flavor and light flavor), but according to the JME, there are some issues with its application to light flavor jets. This is why the cuts on b/c-tagging scores are introduced above. The regression is applied to the jets that pass an **OR** of these cuts: `(j.btagPNetB>cut_btagB) | (j.btagPNetCvL>cut_btagCvL)`  

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