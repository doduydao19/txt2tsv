# -*- coding: utf-8 -*-

import re



### auxiliary functions 

def desamb_id(x):
    output = 0
    reg_brackets = r"\[\d+\]"
    regex_temp = [(m.start(0), m.end(0)) for m in re.finditer(reg_brackets, x)]

    if len(regex_temp)> 0: 
        output = x[regex_temp[0][0]+1:regex_temp[0][1]-1] 
    return output

def groupby_chunk(df_doublons, layer, source_var, target_var, var): 
    df_doublons[("id_features", "desamb_id")] = df_doublons[(layer, var)].map(desamb_id)
    
    df_doublons[("id_features", "index_row")] = df_doublons.index
    
    dict_agg = {("id_features", "token_id"): "first", 
                ("id_features", "par_id"): "first", 
                ("id_features", "index_row"): "first"}
    df_doublons[("id_features", "desamb_id_bis")] = \
        df_doublons.apply(lambda row: 1000 + row[("id_features", "start_index")]\
           if row[("id_features", "desamb_id")] == 0\
           else row[("id_features", "desamb_id")], axis = 1)
    df_doublons = df_doublons.groupby([(layer, source_var), 
                                       ("id_features", "desamb_id"), 
                                       ("id_features", "desamb_id_bis"),
                                       (layer, target_var)]).agg(dict_agg).reset_index(drop = False)
    df_doublons[("id_features", "token_par_id")] =  df_doublons[("id_features", "par_id")].map(lambda x: str(int(x+1))) + "-" + df_doublons[("id_features", "token_id")].map(lambda x: str(int(x+1)))
    df_doublons.columns = df_doublons.columns.droplevel()
    df_doublons = df_doublons.drop(["desamb_id_bis"], axis = 1)

    return df_doublons 



def relation_webanno(token_par_id_source, desamb_id_source, desamb_id_target):
    """
    build the relation string (to be put in the relation variable on the first 
    token of the target chunk)
    """
    relation = token_par_id_source + '[' + str(desamb_id_source) + "_" +\
                     str(desamb_id_target) + "]"
    return relation 



def replace_or_append_relation(df, relation_value, index_row_target, 
                               layer_relation, relation_var):
    df_temp = df.copy(deep = True)
    # replace or append this value
    if df_temp.loc[index_row_target, (layer_relation, relation_var)] in ["_", "_|_", "_|_|_"]: 
        df_temp.loc[index_row_target, (layer_relation, relation_var)] = relation_value
    else: 
        df_temp.loc[index_row_target, (layer_relation, relation_var)] = \
            df_temp.loc[index_row_target, (layer_relation, relation_var)]\
            + "|" + relation_value    
    return df_temp






def replace_tag_by_relation(token_df, layer, layer_relation, source_var, 
                            target_var, relation_var, windows = 30): 

    df_temp = token_df.copy(deep = True)
    df_doublons = df_temp[~((df_temp[(layer, target_var)].\
                    isin(["_", "_|_", "_|_|_", "_|_|_|_"]))|(df_temp[(layer, source_var)].\
                    isin(["_", "_|_", "_|_|_", "_|_|_|_"])))]
    df_nd = df_temp[(~(df_temp[(layer, target_var)].\
                    isin(["_", "_|_", "_|_|_", "_|_|_|_"])))&(df_temp[(layer, source_var)].\
                    isin(["_", "_|_", "_|_|_", "_|_|_|_"]))]
    if len(df_doublons)>0: 
        df_doublons = groupby_chunk(df_doublons, layer, source_var, target_var, source_var)
        df_nd = groupby_chunk(df_nd, layer, source_var, target_var, target_var)
        df_nd[target_var] = df_nd[target_var].map(lambda x: x.split('[')[0])
        
        for index, row in df_doublons.iterrows(): 
            tag_target = row[target_var]
            tag_target = tag_target.split('[')[0]
            token_par_id_source = row["token_par_id"]
            desamb_id_source = row["desamb_id"]
            index_row_source = row["index_row"]
        
        
        
            # a changer : pour prendre en compte les entités apres la source entité     
            # build the value of the relation variable 
            df_nd["distance"] = df_nd.apply(lambda row: \
                 (index_row_source - row["index_row"]) if row["index_row"] <= \
                      index_row_source else 2*(row["index_row"]-index_row_source), axis = 1)
            df_nd = df_nd.sort_values(by = "distance")
    
    
            try: 
                desamb_id_target = df_nd[df_nd[target_var] == tag_target]["desamb_id"].values[0]
                index_row_target = df_nd[df_nd[target_var] == tag_target]["index_row"].values[0]
                relation_value = relation_webanno(token_par_id_source, desamb_id_source, desamb_id_target)
                
                
    
                df_temp = replace_or_append_relation(df_temp, relation_value, index_row_target, 
                                   layer_relation, relation_var)
                # remove the double tag on values :  
                if desamb_id_source == 0: 
                    df_temp.loc[index_row_source, (layer, target_var)] = "_" #df_temp.loc[index_row_source, (layer, source_var)].replace(tag_target, "_")
                else: 
                    df_temp[(layer, target_var)] = df_temp[(layer, target_var)].map(lambda x: x.replace(tag_target + "[" + desamb_id_source + "]", "_"))
                    
            # in case there is no tag found to be the target of the value, 
            # join the chunk with itself 
            except: 
    
                index_row_target = index_row_source
                desamb_id_target = desamb_id_source
                relation_value = relation_webanno(token_par_id_source, desamb_id_source, desamb_id_target)
                df_temp = replace_or_append_relation(df_temp, relation_value, index_row_target, 
                       layer_relation, relation_var)
    

    return df_temp
