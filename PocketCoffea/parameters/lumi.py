import os

# Function to convert and round the integrated luminosity from picobarn to femtobarn
def femtobarn(picobarn, digits=None):
    if round:
        return round((picobarn)/1000., digits)
    else:
        return picobarn/1000.

# Integrated luminosity [pb^-1] for each data-taking year

lumi = {
    "2016" : 36773.0,
    "2017" : 41529.0,
    "2018" : 58830.0,
}

goldenJSON = {
    "2016" : "/work/mmarcheg/BTVNanoCommissioning/PocketCoffea/PocketCoffea/parameters/datacert/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
    "2017" : "/work/mmarcheg/BTVNanoCommissioning/PocketCoffea/PocketCoffea/parameters/datacert/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",
    "2018" : "/work/mmarcheg/BTVNanoCommissioning/PocketCoffea/PocketCoffea/parameters/datacert/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
}
