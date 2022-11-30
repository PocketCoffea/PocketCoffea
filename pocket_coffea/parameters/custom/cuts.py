# Per-event cuts applied to each event
import awkward as ak
import pocket_coffea.lib.cut_functions as cuts_f
from pocket_coffea.lib.cut_definition import Cut
from config.fatjet_base.custom.functions import mutag, ptbin, ptmsd, ptmsdtau, min_nObj_minmsd, flavor_mask

mutag_presel = Cut(
	name="mutag",
	params={
		"nmusj1" : 1,
		"nmusj2" : 1,
		#"dimuon_pt_ratio" : 0.6
	},
	function=mutag
)

def get_ptbin(pt_low, pt_high, name=None):
	if name == None:
		name = f"Pt-{pt_low}to{pt_high}"
	return Cut(
		name=name,
		params= {"pt_low" : pt_low, "pt_high" : pt_high},
		function=ptbin
	)

def get_ptmsd(pt, msd, name=None):
	if name == None:
		name = f"pt{pt}msd{msd}"
	return Cut(
		name=name,
		params= {"pt" : pt, "msd" : msd},
		function=ptmsd
	)

def get_ptmsdtau(pt, msd, tau21, name=None):
	if name == None:
		name = f"msd{msd}tau{tau21}"
	return Cut(
		name=name,
		params= {"pt" : pt, "msd" : msd, "tau21" : tau21},
		function=ptmsdtau
	)

def get_nObj_minmsd(N, minmsd=None, coll="JetGood", name=None):
    '''
    Factory function which creates a cut for minimum number of objects.
    Optionally a minimum msd is requested.
    :param N: request >= N objects
    :param coll: collection to use
    :param minmsd: minimum msd
    :param name: name for the cut, by defaul it is built as n{coll}_min{N}_msd{minmsd}
    :returns: a Cut object
    '''
    if name == None:
        if minmsd:
            name = f"n{coll}_min{N}_msd{minmsd}"
        else:
            name = f"n{coll}_min{N}"
    if minmsd:
        return Cut(
            name=name,
            params={"N": N, "coll": coll, "minmsd": minmsd},
            function=min_nObj_minmsd,
        )
    else:
    	raise NotImplementedError
        #return Cut(name=name, params={"N": N, "coll": coll}, function=min_nObj)

def get_flavor(flavor):
    return Cut(
        name=flavor,
        params={"flavor": flavor},
        function=flavor_mask
    )
