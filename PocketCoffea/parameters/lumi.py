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
