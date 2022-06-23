# Per-event cuts applied to each event
import awkward as ak
import PocketCoffea.lib.cut_functions as cuts_f
from PocketCoffea.lib.cut_definition import Cut

passthrough = Cut(
    name="passthrough",
    params = {},
    function= cuts_f.passthrough
)

dilepton_presel = Cut(
    name ="dilepton",
    params = {
		"METbranch" : {
			'2016' : "MET",
			'2017' : "METFixEE2017",
			'2018' : "MET",
		},
		"njet"  : 2,
		"nbjet" : 1,
		"pt_leading_lepton" : 25,
		"met" : 40,
		"mll" : 20,
		"mll_SFOS" : {'low' : 76, 'high' : 106}
	},
    function = cuts_f.dilepton
 )

semileptonic_presel = Cut(
    name ="semileptonic",
    params = {
		"METbranch" : {
			'2016' : "MET",
			'2017' : "METFixEE2017",
			'2018' : "MET",
		},
		"njet"  : 4,
		"nbjet" : 3,
		"pt_leading_electron" : {
			'2016' : 29,
			'2017' : 30,
			'2018' : 30,
		},
		"pt_leading_muon" : {
			'2016' : 26,
			'2017' : 29,
			'2018' : 26,
		},
		"met" : 20,
	},
    function = cuts_f.semileptonic
 )

semileptonic_triggerSF_presel = Cut(
    name ="semileptonic_triggerSF",
    params = {
		"njet"  : 4,
		"pt_leading_electron" : {
			'2016' : 29,
			'2017' : 30,
			'2018' : 30,
		},
		"pt_leading_muon" : {
			'2016' : 26,
			'2017' : 29,
			'2018' : 26,
		},
	},
    function = cuts_f.semileptonic_triggerSF
 )
