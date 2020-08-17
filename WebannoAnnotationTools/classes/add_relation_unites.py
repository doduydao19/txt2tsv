# -*- coding: utf-8 -*-

# if valeur (target has no link with unite ou unite expertise)
# add link with the next token using shift +1 


def add_link(row, var_unite, var_valeur, var_relation): 
    if (row[var_valeur]!="_") and \
    (row[var_relation ]=="_") and \
    (row[(var_unite[0], var_unite[1]+"_lag")]!="_"): 
        return str(row[("id_features", "token_par_id_lag")]) + '[' \
                    + str(int(row[("id_features", "desamb_id_lag")])) + '_'+ \
                    str(int(row[("id_features", "desamb_id_lag")])) + ']'
    else: 
        return row[var_relation]



def add_relation_valeur_unite(df, var_unite, var_valeur, var_relation): 
    chunk_df = df.copy(deep = True)
    chunk_df = chunk_df.sort_values(by = [("id_features", "par_id"), ("id_features", "token_id")])
    chunk_df[(var_unite[0], var_unite[1]+"_lag")] = chunk_df[var_unite].shift(periods=-1).fillna("_")
    chunk_df[("id_features", "token_par_id_lag")] = chunk_df[("id_features", "token_par_id")].shift(periods=-1).fillna("0_0")
    
    chunk_df[("id_features", "desamb_id_lag")] = chunk_df[("id_features", "desamb_id")].shift(periods=-1)
 
    chunk_df[var_relation] = chunk_df.\
            apply(lambda x: add_link(x, var_unite, var_valeur, var_relation), axis = 1)
    return chunk_df
