import uproot
import awkward as ak
import vector
import uproot

'''
This code was ripped from the original notebook and the involved analysis
is broadly unchanged. For more information, consult the original notebook!
'''

# =======================================================================
def calc_weight(weight_variables, events):
    lumi = 36.6
    total_weight = lumi * 1000 / events["sum_of_weights"]
    for variable in weight_variables:
        total_weight = total_weight * abs(events[variable])
    return total_weight


# Cut lepton type (electron type is 11,  muon type is 13)
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

# Cut lepton charge
def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M # .M calculates the invariant mass
    return invariant_mass


def cut_trig_match(lep_trigmatch):
    trigmatch = lep_trigmatch
    cut1 = ak.sum(trigmatch, axis=1) >= 1
    return cut1

def cut_trig(trigE,trigM):
    return trigE | trigM


def ID_iso_cut(IDel,IDmu,isoel,isomu,pid):
    thispid = pid
    return (ak.sum(((thispid == 13) & IDmu & isomu) | ((thispid == 11) & IDel & isoel), axis=1) == 4)

def process_data(fileString, sample):
    variables = ['lep_pt','lep_eta','lep_phi','lep_e','lep_charge','lep_type','trigE','trigM','lep_isTrigMatched',
            'lep_isLooseID','lep_isMediumID','lep_isLooseIso','lep_type']
    weight_variables = ["filteff","kfac","xsec","mcWeight","ScaleFactor_PILEUP", "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]

    frames = []
    
    tree = uproot.open(fileString + ":analysis")
    
    sample_data = []

    fraction = 1.0

    # Loop over data in the tree
    for data in tree.iterate(variables + weight_variables + ["sum_of_weights", "lep_n"],
                                library="ak",
                                entry_stop=tree.num_entries*fraction):#, # process up to numevents*fraction
                            #  step_size = 10000000):

        # Number of events in this batch
        nIn = len(data)

        data = data[cut_trig(data.trigE, data.trigM)]
        data = data[cut_trig_match(data.lep_isTrigMatched)]

        # Record transverse momenta (see bonus activity for explanation)
        data['leading_lep_pt'] = data['lep_pt'][:,0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
        data['third_leading_lep_pt'] = data['lep_pt'][:,2]
        data['last_lep_pt'] = data['lep_pt'][:,3]

        # Cuts on transverse momentum
        data = data[data['leading_lep_pt'] > 20]
        data = data[data['sub_leading_lep_pt'] > 15]
        data = data[data['third_leading_lep_pt'] > 10]

        data = data[ID_iso_cut(data.lep_isLooseID,
                                data.lep_isMediumID,
                                data.lep_isLooseIso,
                                data.lep_isLooseIso,
                                data.lep_type)]

        # Number Cuts
        #data = data[data['lep_n'] == 4]

        # Lepton cuts

        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]

        # Invariant Mass
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_e'])

        # Store Monte Carlo weights in the data
        if 'data' not in sample: # Only calculates weights if the data is MC
            data['totalWeight'] = calc_weight(weight_variables, data)
            # data['totalWeight'] = calc_weight(data)

        # Append data to the whole sample data list
        sample_data.append(data)

        val = fileString # from the original code because I dont wan't to change too much

        if not 'data' in val:
            nOut = sum(data['totalWeight']) # sum of weights passing cuts in this batch
        else:
            nOut = len(data)
            
        #print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after

    return ak.concatenate(sample_data)

    


#=======================================================================