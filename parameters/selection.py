# Per-event cuts applied to each event

event_selection = {
	"dilepton" : {
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
}