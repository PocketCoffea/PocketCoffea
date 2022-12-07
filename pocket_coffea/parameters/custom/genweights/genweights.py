import pocket_coffea

basefolder = pocket_coffea.__path__[0]

genweights_files = {
	'2018' : f"{basefolder}"+"/parameters/custom/genweights/genweights_2018UL/output_{dataset}.coffea",
	'2017' : f"{basefolder}"+"/parameters/custom/genweights/genweights_2017UL/output_{dataset}.coffea",
	'2016_PostVFP' : f"{basefolder}"+"/parameters/custom/genweights/genweights_2016UL_PostVFP/output_{dataset}.coffea",
	'2016_PreVFP' : f"{basefolder}"+"/parameters/custom/genweights/genweights_2016UL_PreVFP/output_{dataset}.coffea",
}

