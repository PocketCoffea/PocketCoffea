# Functions to filter the output dictionary by year

from collections import defaultdict

def filter_dictionary(d, string):
    d_filtered = {k : val for k,val in d.items() if string in k}
    if type(d) == defaultdict:
        return defaultdict(dict, d_filtered)
    elif type(d) == dict:
        return d_filtered
    else:
        raise TypeError("Input dictionary must be either a dict or defaultdict")

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
            o_filtered[key][k] = defaultdict(dict)
            for s, _dict in val.items():
                o_filtered[key][k][s] = filter_dictionary(_dict, year)
    
    o_filtered["datasets_metadata"]["by_datataking_period"] = {k : val for k, val in o["datasets_metadata"]["by_datataking_period"].items() if k == year}
    o_filtered["datasets_metadata"]["by_dataset"] = defaultdict(dict, {k : val for k, val in o["datasets_metadata"]["by_dataset"].items() if val["year"] == year})
    return o_filtered

def compare_dict_types(d1, d2, path=""):
    """
    Recursively compare the types of values between two dictionaries.

    Args:
        d1 (dict): The first dictionary to compare.
        d2 (dict): The second dictionary to compare.
        path (str): The current path of nested keys being checked.
    """

    type_mismatch_found = False
    # Get the combined set of keys from both dictionaries
    all_keys = set(d1.keys()).union(d2.keys())

    for key in all_keys:
        current_path = f"{path}.{key}" if path else key

        # Check if the key is missing in either dictionary
        if key not in d1:
            #print(f"Key '{current_path}' is missing in the first dictionary")
            continue
        if key not in d2:
            #print(f"Key '{current_path}' is missing in the second dictionary")
            continue

        type1 = type(d1[key])
        type2 = type(d2[key])

        # Print the type comparison for the current key
        if type1 != type2:
            print(f"Type mismatch at '{current_path}': {type1.__name__} vs {type2.__name__}")
            type_mismatch_found = True

        # Recursively check if the value is a dictionary
        if isinstance(d1[key], dict) and isinstance(d2[key], dict):
            type_mismatch_found = compare_dict_types(d1[key], d2[key], current_path)
        # Recursively check if the value is a list or tuple
        elif isinstance(d1[key], (list, tuple)) and isinstance(d2[key], (list, tuple)):
            for i, (item1, item2) in enumerate(zip(d1[key], d2[key])):
                item_path = f"{current_path}[{i}]"
                type_mismatch_found = compare_dict_types({item_path: item1}, {item_path: item2})
    return type_mismatch_found