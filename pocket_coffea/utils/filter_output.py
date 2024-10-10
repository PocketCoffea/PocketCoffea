# Functions to filter the output dictionary by year

def filter_dictionary(d, string):
    return {k : val for k,val in d.items() if k.endswith(string)}

def filter_output_by_year(o, year):
    o_filtered = {key : {} for key in o.keys()}
    keys_1d = ["sum_genweights", "sum_signOf_genweights"]
    for key in keys_1d:
        for k, val in o[key].items():
            if not k.endswith(year): continue
            o_filtered[key][k] = val
    keys_2d = ["sumw", "sumw2", "cutflow"]
    for key in keys_2d:
        for k, val in o[key].items():
            o_filtered[key][k] = filter_dictionary(val, year)
    keys_3d = ["variables"]
    for key in keys_3d:
        for k, val in o[key].items():
            o_filtered[key][k] = {}
            for s, _dict in val.items():
                o_filtered[key][k][s] = filter_dictionary(_dict, year)
    
    o_filtered["datasets_metadata"]["by_datataking_period"] = {k : val for k, val in o["datasets_metadata"]["by_datataking_period"].items() if k == year}
    o_filtered["datasets_metadata"]["by_dataset"] = {k : val for k, val in o["datasets_metadata"]["by_dataset"].items() if k.endswith(year)}
    return o_filtered