# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 15:09:25 2019

@author: kim.montalibet
"""

from Process.WebannoAnnotationTools.classes.text_to_tsv_class import unlist_df
import pandas as pd
from typing import List, Tuple





def flatten_colnames(df): 
    df2 = df.copy(deep = True)
    df2.columns = ['|'.join(col).strip() for col in df2.columns.values]
    return df2

def merge_df_multi_index(df_left: pd.DataFrame, df_right: pd.DataFrame, 
          on: List[Tuple], how: str)-> pd.DataFrame: 
    df_left2 = flatten_colnames(df_left)
    df_right2 = flatten_colnames(df_right)
    on2 = ["|".join(x) for x in on]
    df_merge = pd.merge(df_left2, df_right2, on = on2, how = how)
    
    tuples_colnames = [tuple(x.split("|")) for x in df_merge.columns]
    multi_index = pd.MultiIndex.from_tuples(tuples_colnames)
    df_merge.columns = multi_index
    return df_merge 


def join_relations(df, target_variable, source_variable, 
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias = None, 
                   source_variable_alias = None,
                   list_other_var = [],
                   list_id_features = [("id_features", "token_par_id"), 
                                       ("id_features", "token_joined")]):

    if target_variable_alias == None: 
        target_variable_alias = target_variable[1]
    if source_variable_alias == None: 
        source_variable_alias = source_variable[1]
    
    df_copy = df.copy(deep = True)
    layer = target_variable[0]
    layer_relation = id_relation_variable[0]
    if len(list_other_var) >0: 
        list_layers_other_var = list(set([x[0] for x in list_other_var])-set([layer]))
        list_other_var_single = [x[1] for x in list_other_var]
    else: 
        list_layers_other_var = []
        list_other_var_single = []
    sub_df = df_copy[["id_features", layer, layer_relation] + list_layers_other_var]
    
    sub_df.columns = sub_df.columns.droplevel()
    target_variable = target_variable[1]
    source_variable = source_variable[1]
    id_relation_variable = id_relation_variable[1]
    relation_variable = relation_variable[1]
    list_id_features = [x[1] for x in list_id_features]
    
    
    sub_df[id_relation_variable] = sub_df[id_relation_variable].map(lambda x: x.split("|"))
    df_long = unlist_df(sub_df, id_relation_variable, [target_variable, source_variable] + list_id_features + list_other_var_single)
    df_long[source_variable_alias + "_id"] =  df_long[id_relation_variable]\
                                .map(lambda x: x.split("[")[0])
                                
    df_left = df_long[(df_long[id_relation_variable]!="_") & (df_long[target_variable]!="_")]
    df_left = df_left[list_id_features + list_other_var_single + [
                       id_relation_variable,
                       target_variable, 
                       source_variable_alias + "_id"]]
    
    df_right = df_long[list_id_features + [source_variable]]
    df_right = df_right[df_right[source_variable]!="_"]
    dict_rename_right = {'token_joined': source_variable_alias + "_token", 
                   source_variable : source_variable_alias + "_tag", 
                   'token_par_id': source_variable_alias + "_id"}
    df_right = df_right.rename(columns=dict_rename_right)
    
    dict_rename_left = {'token_joined': target_variable_alias + "_token", 
                   target_variable : target_variable_alias + "_tag", 
                   'token_par_id': target_variable_alias + "_id"}
    df_left = df_left.rename(columns=dict_rename_left)

    df_merge = pd.merge(df_left, df_right, on = source_variable_alias + "_id", how = "left")
    df_merge = df_merge.drop([id_relation_variable], axis = 1)
    
    df_merge = df_merge.dropna(subset = [source_variable_alias + "_token"])

    return df_merge
