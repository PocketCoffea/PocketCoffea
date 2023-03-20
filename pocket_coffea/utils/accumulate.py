from coffea.util import load

def initialize_accumulator(o, joint_dataset):
    '''This function properly initialize the joint accumulator.'''
    accumulator = {'sum_genweights' : {}, 'sumw' : {}, 'cutflow' : {}, 'variables' : {}}
    
    for key in ['sumw', 'cutflow']:
        for k, d in o[key].items():
            accumulator[key][k] = {}
            
    for key in ['variables']:
        for k, d in o[key].items():
            accumulator[key][k] = {joint_dataset : None}
    
    return accumulator

def join_accumulator(accumulator, joint_dataset):
    '''This function joins the total accumulator into the accumulator of a single dataset.'''
    for key in ['sumw', 'cutflow']:
        for k, d in accumulator[key].items():
            accumulator[key][k] = {joint_dataset : sum(d.values())}

    return accumulator

def get_joint_accumulator(files, joint_dataset):
    '''This function reads a list of Coffea files and returns the joint accumulator summing over datasets.'''
    h_tot = None
    
    for i, file in enumerate(files):
        o = load(file)
        if i == 0:
            accumulator = initialize_accumulator(o, joint_dataset)
        # Read dataset from first histogram
        h0 = list(o['variables'].keys())[0]
        dataset = list(o['variables'][h0].keys())[0]
        print(dataset)
        
        # Update sum_genweights dictionary
        for key in ['sum_genweights']:
            dict_only_dataset = {dataset : o[key][dataset]}
            accumulator[key].update(o[key])
        
        # Update sumw and cutflow dictionaries
        for key in ['sumw', 'cutflow']:
            for k, d in o[key].items():
                dict_only_dataset = {dataset : d[dataset]}
                if i == 0:
                    accumulator[key][k] = dict_only_dataset
                else:
                    accumulator[key][k].update(dict_only_dataset)
        
        # Sum all the histograms
        for histname, h_dict in o['variables'].items():
            if i == 0:
                accumulator['variables'][histname][joint_dataset] = h_dict[dataset]
            else:
                accumulator['variables'][histname][joint_dataset] = accumulator['variables'][histname][joint_dataset] + h_dict[dataset]
            
    return join_accumulator(accumulator, joint_dataset)
