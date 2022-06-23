from PocketCoffea.lib.cut_definition import Cut

def trigger_mask(events, params, **kwargs):
	mask = events.HLT[params["trigger"]] == {'pass': True, 'fail': False}[params["category"]]
	return mask

def get_trigger_passfail(trigger, category):
	return Cut(
		name=f"{trigger}_{category}",
		params={"trigger": trigger, "category": category},
		function=trigger_mask
	)
