# Per-event cuts applied to each event
import awkward as ak
import lib.cut_functions as cuts_f
from parameters.cuts.cut_definition import Cut

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


