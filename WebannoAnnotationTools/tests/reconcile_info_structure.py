# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 11:07:48 2019

@author: kim.montalibet
"""

def is_row_structure(row, list_var_structure): 
    output = 0
    if any([row[x] for x in list_var_structure]):
        output = 1
    return output  



def add_structure_to_all_tokens(chunk_df, partie_var, sous_partie_var, victime_var): 

    df = chunk_df.copy(deep = True)
    df[("Evaluation", "is_structure")] = df.apply(lambda x:\
               is_row_structure(x, [partie_var, sous_partie_var, victime_var]), axis = 1) 
    df = df.sort_values(by = [("id_features", "start_chunk")]).reset_index(drop = True)

    partie = "_"
    sous_partie = "_"
    victime = "_"
    for index, row in df.iterrows(): 
        if row[("Evaluation", "is_structure")]==1: 
            if row[partie_var] not in ["_", "_|_", "_|_|_", "_|_|_|_"]: 
                partie = row[partie_var]
                if partie in ["motifs", "dispositif"]:
                    sous_partie = "décision_appel"
            if row[sous_partie_var] not in ["_", "_|_", "_|_|_", "_|_|_|_"]:
                sous_partie = row[sous_partie_var]
            if row[victime_var] not in ["_", "_|_", "_|_|_", "_|_|_|_"]: 
                victime = row[victime_var]
        df.loc[index, ("Structure", "partie_all")] = partie
        df.loc[index, ("Structure", "sous_partie_all")] = sous_partie
        df.loc[index, ("Structure", "victime_all")] = victime
        
    return df 