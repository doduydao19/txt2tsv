# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 17:59:18 2019

@author: kim.montalibet
"""

import pandas as pd
from typing import List
import re


pd.options.mode.chained_assignment = None


### functions
def get_chunk_id(row, list_features_flatten):
    reg_brackets = r"\[\d+\]"
    list_id= []
    for col in list_features_flatten: 
        regex_temp = [(m.start(0), m.end(0)) for m in re.finditer(reg_brackets, row[col])]

        if len(regex_temp)> 0: 
            to_append = [row[col][x[0]+1:x[1]-1] for x in regex_temp]
            list_id+= to_append  
    return list(set(list_id))


def dedup_row_webanno(row, list_features_flatten): 
    list_other_features = [("id_features", 'token_id'), 
                           ("id_features", 'par_id'),
                           ("id_features", 'id_characters'),
                           ("id_features", 'token')]
    list_id = row[("id_features", "list_id")]
    list_dict = []
    for id_chunk in list_id: 
        dico_temp = {}
        for col in list_features_flatten:
            regex_temp = re.search(r"\[{}\]".format(id_chunk), row[col])
            if regex_temp:
                dico_temp[col] = [x for x in row[col].split("|") if "[{}]".format(id_chunk) in x][0]
                #dico_temp[col] = "blabla"#row[col]
        for col2 in list_other_features: 
            dico_temp[col2] = row[col2]
        list_dict.append(dico_temp)
        
    return  list_dict


def is_dup_feature(row: str, list_features_flatten: List) -> List:
    for col in list_features_flatten: 
        regex_temp = re.search(r"\|",  row[col])
        if regex_temp:
            return 1
        else:
            return 0



# remove rows without tokens 
def get_number_of_fields(row, col_list): 
    nb_fields = len([row[col] for col in col_list if row[col]!="_"])
    return nb_fields


### main function 

def token_to_chunk_df(df, list_span_variables, list_relation_variables = []): 
    threshold_desamb_id = 5000
    list_features_flatten = list_span_variables + list_relation_variables
    df[("id_features", "nb_fields_filled")] = df.apply(lambda x: get_number_of_fields(x, list_features_flatten), axis = 1)
    df = df[df[("id_features", "nb_fields_filled")] >0]
    df = df.drop([("id_features", "nb_fields_filled")], axis = 1)
    
    
    # part 2 : get desambiguation features
    df[("id_features", "list_id")] = df.apply(lambda x: get_chunk_id(x, list_features_flatten), axis = 1)
    df[("id_features", "len_set_id")] = df[("id_features", "list_id")].apply(lambda x: len(x))
    df[("id_features", "is_dup_feature")] = df.apply(lambda x: is_dup_feature(x, list_features_flatten), axis = 1)
    
    # part 3: get df with rows which need deduplication 
    # and deduplicated rows 
    
    df2 = df[df[("id_features", "len_set_id")] >1]
    if len(df2)>0: 
        df2[("id_features", "list_dict")] = df2.apply(lambda x: dedup_row_webanno(x, list_features_flatten), axis = 1) 
        df_to_append = pd.DataFrame()
        for index, row in df2.iterrows(): 
            list_dict = row[("id_features", "list_dict")]
            for dico in list_dict: 
                df_to_append = df_to_append.append(pd.DataFrame([dico]), sort = False)
        df_to_append.fillna("_", inplace = True)
        list_features2 = [x for x in list_features_flatten if x in df_to_append.columns]
        df_to_append[("id_features", "list_id")] = df_to_append.apply(lambda x: get_chunk_id(x, list_features2), axis = 1)
        
    # part 4 : keep only rows which do not need deduplication 
    df = df[df[("id_features", "len_set_id")] <2]
    df.drop([("id_features", 'is_dup_feature'), ("id_features", "len_set_id")], axis = 1, inplace = True)
    
    # part 5 : concatenate deduplicated rows and non duplicated rows 
    if len(df2) >0: 
        df = pd.concat([df, df_to_append], axis = 0, sort = False)
    df.fillna("_", inplace = True)
    df = df.replace(to_replace=r'\*(\[\d+\]){0,1}', value='_', regex=True)
    #df = df.replace(to_replace=r'\*', value='_', regex=True)
    
    # part 6 : create sentence number and token number variable (used for sorting )
    df[("id_features", "desamb_id")] = df[("id_features", "list_id")].apply(lambda x: x[0] if len(x)>0 else None)
    #df[("id_features", "sentence_nb")] = df[("id_features", "token_id")].apply(lambda x: int(x.split("-")[0]))
    #df[("id_features", "token_nb")] = df[("id_features", "token_id")].apply(lambda x: int(x.split("-")[1]))
    
    
    # part 7 : fill desamb with an id (in order to group by later )
    def fill_desamb_id(row): 
        if pd.isna(row[("id_features", "desamb_id")])==True: 
            return row[("id_features", "par_id")]*threshold_desamb_id+row[("id_features", "token_id")]
        else: 
            return int(row[("id_features", "desamb_id")])
    df[("id_features", "desamb_id")] = df.apply(fill_desamb_id, axis = 1)
    
    # part 8: groupby after ordering 
    df = df.sort_values(by = [("id_features", "par_id"), ("id_features", "token_id")])
    
    
    dict_agg = {("id_features", "token"): list, ("id_features", "id_characters"): list, 
                ("id_features", "token_id"): "first", ("id_features", "par_id"): "first" }
    for feature in list_relation_variables : 
        dict_agg[feature] = "first" 


    dfg = df.groupby([("id_features", "desamb_id")]+list_span_variables).agg(dict_agg)
    dfg = dfg.reset_index()
    dfg[("id_features", "desamb_id")] = dfg[("id_features", "desamb_id")].map(lambda x: 0 if x>=threshold_desamb_id else x)

    dfg[("id_features", "start_chunk")] = dfg[("id_features", "id_characters")].apply(lambda x: int(x[0].split("-")[0]))
    dfg[("id_features", "end_chunk")] = dfg[("id_features", "id_characters")].apply(lambda x: int(x[-1].split("-")[1]))
    dfg = dfg.replace(to_replace=r'\[\d+\]', value='', regex=True)
    dfg[("id_features", "token_joined")] = dfg[("id_features", "token")].apply(lambda x: " ".join(x))
    dfg[("id_features", "token_par_id")] = dfg[("id_features", "par_id")].map(lambda x: str(int(x+1))) + "-" + dfg[("id_features", "token_id")].map(lambda x: str(int(x+1)))
    
    return dfg

####
    


"""
def token_to_chunk_df_VO(df, list_features_flatten): 


    df[("id_features", "nb_fields_filled")] = df.apply(lambda x: get_number_of_fields(x, list_features_flatten), axis = 1)
    df = df[df[("id_features", "nb_fields_filled")] >0]
    df = df.drop([("id_features", "nb_fields_filled")], axis = 1)
    
    
    # part 2 : get desambiguation features
    df[("id_features", "list_id")] = df.apply(lambda x: get_chunk_id(x, list_features_flatten), axis = 1)
    df[("id_features", "len_set_id")] = df[("id_features", "list_id")].apply(lambda x: len(x))
    df[("id_features", "is_dup_feature")] = df.apply(lambda x: is_dup_feature(x, list_features_flatten), axis = 1)
    
    # part 3: get df with rows which need deduplication 
    # and deduplicated rows 
    
    df2 = df[df[("id_features", "len_set_id")] >1]
    df2[("id_features", "list_dict")] = df2.apply(lambda x: dedup_row_webanno(x, list_features_flatten), axis = 1) 
    df_to_append = pd.DataFrame()
    for index, row in df2.iterrows(): 
        list_dict = row[("id_features", "list_dict")]
        for dico in list_dict: 
            df_to_append = df_to_append.append(pd.DataFrame([dico]), sort = False)
    df_to_append.fillna("_", inplace = True)
    list_features2 = [x for x in list_features_flatten if x in df_to_append.columns]
    df_to_append[("id_features", "list_id")] = df_to_append.apply(lambda x: get_chunk_id(x, list_features2), axis = 1)
    
    # part 4 : keep only rows which do not need deduplication 
    df = df[df[("id_features", "len_set_id")] <2]
    df.drop([("id_features", 'is_dup_feature'), ("id_features", "len_set_id")], axis = 1, inplace = True)
    
    # part 5 : concatenate deduplicated rows and non duplicated rows 
    df = pd.concat([df, df_to_append], axis = 0, sort = False)
    df.fillna("_", inplace = True)
    df = df.replace(to_replace=r'\*\[\d+\]', value='_', regex=True)
    df = df.replace(to_replace=r'\*', value='_', regex=True)
    
    # part 6 : create sentence number and token number variable (used for sorting )
    df[("id_features", "desamb_id")] = df[("id_features", "list_id")].apply(lambda x: x[0] if len(x)>0 else None)
    #df[("id_features", "sentence_nb")] = df[("id_features", "token_id")].apply(lambda x: int(x.split("-")[0]))
    #df[("id_features", "token_nb")] = df[("id_features", "token_id")].apply(lambda x: int(x.split("-")[1]))
    
    
    # part 7 : fill desamb with an id (in order to group by later )
    def fill_desamb_id(row): 
        if pd.isna(row[("id_features", "desamb_id")])==True: 
            return row[("id_features", "par_id")]*100+row[("id_features", "token_id")]
        else: 
            return int(row[("id_features", "desamb_id")])
    df[("id_features", "desamb_id")] = df.apply(fill_desamb_id, axis = 1)
    
    # part 8: groupby after ordering 
    df = df.sort_values(by = [("id_features", "par_id"), ("id_features", "token_id")])
    
    dfg = df.groupby([("id_features", "desamb_id")]+list_features_flatten).agg({("id_features", "token"): list, ("id_features", "id_characters"): list})
    dfg = dfg.reset_index()
    dfg[("id_features", "start_chunk")] = dfg[("id_features", "id_characters")].apply(lambda x: int(x[0].split("-")[0]))
    dfg[("id_features", "end_chunk")] = dfg[("id_features", "id_characters")].apply(lambda x: int(x[-1].split("-")[1]))
    dfg = dfg.replace(to_replace=r'\[\d+\]', value='', regex=True)
    dfg[("id_features", "token_joined")] = dfg[("id_features", "token")].apply(lambda x: " ".join(x))


    return dfg"""

