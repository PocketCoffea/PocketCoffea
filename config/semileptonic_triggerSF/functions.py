import numpy as np
from PocketCoffea.lib.cut_definition import Cut

def trigger_mask(events, params, **kwargs):
	mask = np.zeros(len(events), dtype='bool')
	for trigger in params["triggers"]:
		mask = mask | events.HLT[trigger]
	assert (params["category"] in ["pass", "fail"]), "The allowed categories for the trigger selection are 'pass' and 'fail'"
	if params["category"] == "fail":
		mask = ~mask
	return mask

def get_trigger_passfail(triggers, category):
	return Cut(
		name=f"{'_'.join(triggers)}_{category}",
		params={"triggers": triggers, "category": category},
		function=trigger_mask
	)
