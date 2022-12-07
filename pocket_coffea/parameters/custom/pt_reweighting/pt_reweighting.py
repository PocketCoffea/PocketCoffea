import pocket_coffea

basefolder = pocket_coffea.__path__[0]

pt_corrections = {
	'2018' : f"{basefolder}/parameters/custom/pt_reweighting/pt_reweighting_2018UL/pt_corr_1D_2018.json",
	'2017' : f"{basefolder}/parameters/custom/pt_reweighting/pt_reweighting_2017UL/pt_corr_1D_2017.json",
	'2016_PostVFP' : f"{basefolder}/parameters/custom/pt_reweighting/pt_reweighting_2016UL_PostVFP/pt_corr_1D_2016_PostVFP.json",
	#'2016_PreVFP' : f"{basefolder}/parameters/custom/pt_reweighting/pt_reweighting_2016UL_PreVFP_v04/pt_corr_2016_PreVFP.coffea",
}
