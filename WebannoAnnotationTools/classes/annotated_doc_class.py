
import itertools
import pandas as pd
from typing import List, Dict
import csv
import re
pd.options.mode.chained_assignment = None
#from token_to_chunk_df import token_to_chunk_df

#### class definition for annotated docs 

from Process.WebannoAnnotationTools.classes.text_to_tsv_class import create_list_header,  text_to_tsv3, number_of_features, convert_to_webanno_tsv3
from Process.WebannoAnnotationTools.classes.token_to_chunk_df import token_to_chunk_df

#def token_to_chunk_df(df, list_features_flatten):
    
    
    

class annotated_doc(text_to_tsv3):
    
    def __init__(self, file, nb_desamb_id = 200):
        self.file = file
        #self.mapping_features = mapping_features
        #self.mapping_layers = mapping_layers
        #self.revert_mapping_features = dict(zip(mapping_features.values(), mapping_features.keys()))
        #self.revert_mapping_layers = dict(zip(mapping_layers.values(), mapping_layers.keys()))
        self.nb_desamb_id = nb_desamb_id
        self.webanno_tsv3 = wtsv3_to_token_df(self.file)[4]
        self.list_paragraphs = wtsv3_to_token_df(self.file)[5]
        self.token_dataframe = wtsv3_to_token_df(self.file)[0]
        self.text = reconstruct_text(self.token_dataframe)
        self.list_layers = wtsv3_to_token_df(self.file)[2] 
        self.dict_features = wtsv3_to_token_df(self.file)[3] 
        self.dict_features_by_type = wtsv3_to_token_df(self.file)[1] 
        self.nb_features = number_of_features(self.dict_features_by_type)
        self.list_header = create_list_header(self.dict_features_by_type)
        self.list_relation_variables_tuples = [ tuple(x) for x in list(self.dict_features_by_type['relation_variables'].values())]
        self.list_relation_variables = list(itertools.chain.from_iterable(self.list_relation_variables_tuples))
        self.list_span_variables_list = list(self.dict_features_by_type['span_variables'].values())
        self.list_span_variables = list(itertools.chain.from_iterable(self.list_span_variables_list))
        # 1st: transform the relation variables
        #self.token_df_conll = conll_relation_token_df(self.token_dataframe, self.list_relation_variables_tuples)
        # 2nd: transform the span relations 
        #self.token_df_iob = iob_token_df(self.token_df_conll, self.list_span_variables)
        
        # annotated chunk df: 
        # to add: take into account the relation variables
        #self.annotated_chunk_df = token_to_chunk_df(self.token_dataframe,  self.list_span_variables)

    ## method to change mapping from one scheme to another 
    def convert_scheme(self, mapping_dict): 
        self.token_dataframe = rename_token_dataframe(self.token_dataframe, mapping_dict)
        self.dict_features_by_type = new_dict_features_by_type(self.dict_features_by_type, mapping_dict)
        self.list_header = create_list_header(self.dict_features_by_type)
        self.nb_features = number_of_features(self.dict_features_by_type)
        self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe, self.list_paragraphs, 
                            self.dict_features_by_type,
                            self.list_header)


    def modify_token_dataframe(self, change_token_df):
         self.token_dataframe = change_token_df(self.token_dataframe)
         self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe, self.list_paragraphs, 
                    self.dict_features_by_type,
                    self.list_header)


    #def chunk_df(self, self.token_data_frame, self.list_span_variables, self.list_relation_variables):
    #    return None
         
    def chunk_df(self):
        return token_to_chunk_df(self.token_dataframe, 
                                 self.list_span_variables, 
                                 self.list_relation_variables)
         
    ## methods to add tag: inherited from text to tsv3
    
    ## methods to save back to tsv : inherited 



####################################################
#### auxiliary functions for annotated #############
####################################################


"""
def reconstruct_text(list_par): 
    text = ""
    for index, par in enumerate(list_par[:-1]):
        if list_par[index+1]=="\n": 
            text = text + par
        else:
            text = text + par + "\n"
    text += par[-1]
    return text
"""

def reconstruct_text(token_df): 
    text = token_df.loc[0, ("id_features", "token")]
    for item, row in token_df.loc[1:,:].iterrows(): 
        start_index = row[("id_features", "start_index")]
        #end_index = row[("id_features", "end_index")]
        end_index = token_df.loc[item-1, ("id_features", "end_index")]
        token = row[("id_features", "token")]
        
        text += " "*(start_index - end_index) + token
    return text



def test_reconstruct_text(token_df, text): 

    for item, row in token_df.iterrows(): 
        start_index = row[("id_features", "start_index")]
        end_index = row[("id_features", "end_index")]
        token = row[("id_features", "token")]
        if token != text[start_index:end_index]: 
            print("######")
            print(token)
            print(text[start_index:end_index])
    return None


    
def rename_token_dataframe(token_dataframe, mapping_dict): 
    mapping_dict = {k:v for k,v in mapping_dict.items() if (v != (None, None)) and (k in token_dataframe.columns) }
    
    mapping_concat = dict(zip(["|".join(x) for x in mapping_dict.keys()], ["|".join(x) for x in mapping_dict.values()]))
    
    token_dataframe.columns = ['|'.join((col[0], str(col[1]))) for col in token_dataframe.columns]
    token_dataframe = token_dataframe.rename(columns = mapping_concat)
    token_dataframe = token_dataframe[[x for x in token_dataframe.columns if "id_features" in x] + list(mapping_concat.values())]
    
    list_level_0 = ["id_features"] + list(set([x[0] for x in mapping_dict.values()]))

    order_columns = list(itertools.chain.from_iterable([[x for x in token_dataframe.columns if layer in x] for layer in list_level_0]))

    token_dataframe = token_dataframe[order_columns]
    tuple_columns = [tuple(x.split("|")) for x in order_columns]
    token_dataframe.columns = pd.MultiIndex.from_tuples(tuple_columns)
    return token_dataframe
    
    

def unlist_df(df: pd.DataFrame, col_with_list: str, id_cols: List[str]) -> pd.DataFrame:
    df_list_len_0 = df[df[col_with_list].map(len)==0][[col_with_list]+id_cols]
    df_list_len_0[col_with_list] = None
    
    df_long = (df[col_with_list].apply(lambda x: pd.Series(x))
         .stack()
         .reset_index(level=1, drop=True)
         .to_frame(col_with_list)
         .join(df[id_cols], how='left')
         )
    df_res = pd.concat([df_long, df_list_len_0])
    return df_res        
        
def wtsv3_to_token_df(file: str)-> pd.DataFrame: 
    """ takes the path of the file in webanno tsv3 format and creates a pandas 
    dataframe with one row per token and columns corresponding to the 
    annotation variables
    """
    list_par = []
    list_line_tsv3 = []
    dict_features = {}
    dict_features_by_type = {"span_variables": {}, 
                             "relation_variables": {}}

    list_df = []
    with open(file, encoding = "utf-8") as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter="\t")
        for line in tsvreader:
            list_line_tsv3.append(line)
            if len(line)==1:
                if line[0][:6] == "#Text=":
                       list_par.append(line[0][6:])
            if (len(line)==1) and (line[0][5:20]=="=webanno.custom"):
                list_per_feature = line[0].split("|")[1:]
                layer_name = line[0].split("|")[0][21:]
                
                if line[0][0:5]=="#T_SP": 
                    dict_features_by_type["span_variables"][layer_name] = [(layer_name, x) for x in list_per_feature]
                if line[0][0:5]=="#T_RL": 
                    #list_per_feature = [list_per_feature[ind-1] + "_id" if "BT_webanno.custom." in x else x for ind, x in enumerate(list_per_feature)]
                    dict_features_by_type["relation_variables"][layer_name] = [(layer_name, x) for x in list_per_feature]
                dict_features[layer_name] = list_per_feature
            if (len(line) not in [0, 1]):
                list_df.append(line[:-1])
    list_layers = ["".join([x[0].upper() for x in mot.split("_")]) for mot in list(dict_features.keys())]

    
    tuples_index_features = [[(layer, x) for x in dict_features[layer]] for layer in dict_features.keys()]
    tuples_index_features = list(itertools.chain.from_iterable(tuples_index_features))
    
    id_features = [("id_features", "token_par_id"), ("id_features", 'id_characters'), ("id_features", "token")]
    tuples_index_features = id_features + tuples_index_features
    multi_index = pd.MultiIndex.from_tuples(tuples_index_features, names=['layer', 'feature'])
    
    
    df= pd.DataFrame.from_records(list_df, columns = multi_index)
    df = df.replace(to_replace=r'\*\[\d+\]', value='_', regex=True)
    df = df.replace(to_replace=r'\*', value='_', regex=True)
    df[[x for x in df.columns if x not in ["token"]]] = df[[x for x in df.columns if x not in ["token"]]].replace(to_replace=r'\\', value='', regex=True)
    df[("id_features", "par_id")] = df[("id_features", "token_par_id")].apply(lambda x: int(x.split("-")[0])-1)
    df[("id_features", "token_id")] = df[("id_features", "token_par_id")].apply(lambda x: int(x.split("-")[1])-1)
    
    df[("id_features", "start_index")] = df[("id_features", "id_characters")].apply(lambda x: int(x.split("-")[0]))
    df[("id_features", "end_index")] = df[("id_features", "id_characters")].apply(lambda x: int(x.split("-")[1]))
    
    return df, dict_features_by_type, list_layers, dict_features, list_line_tsv3, list_par   
        


def iob_relation_sub_df(token_df, tuple_var): 
    """for a given tuple of relation variables, computes from token_df the format 
    of relation used in IOB"""
    id_var = [("id_features", "token_par_id")]
    relation_df = token_df[id_var + [tuple_var[0], tuple_var[1]]]
    relation_df = relation_df[relation_df[tuple_var[1]]!="_"]
    relation_df[[tuple_var[0], tuple_var[1]]] = relation_df[[tuple_var[0], tuple_var[1]]].applymap(lambda x: x.split("|"))
    
    def get_tuple(row, list_var): 
        return list(zip(*[row[x] for x in list_var]))#tuple([row[x] for x in list_var])
    
    relation_df[("id_features", "list_tuple")] = relation_df.apply(lambda x : get_tuple(x, [tuple_var[0], tuple_var[1]]), axis = 1) 
    relation_df_long = unlist_df(relation_df, ("id_features", "list_tuple"), id_var)
    
    relation_df_long = relation_df_long.rename(columns = {"token_par_id": "token_par_id_target"})
    relation_df_long[("id_features","id_relation")] = relation_df_long[("id_features", "list_tuple")].map(lambda x: x[1])
    relation_df_long[("id_features","id_relation")] = relation_df_long[("id_features", "id_relation")].map(lambda x: x if "[" in x else x + '[0_0]')
    relation_df_long[("id_features","token_par_id_origin")] = relation_df_long[("id_features", "list_tuple")].map(lambda x: x[1].split("[")[0])
    relation_df_long[("id_features","span_id_origin")] = relation_df_long[("id_features", "id_relation")].map(lambda x: x.split("[")[1].split("_")[0])
    relation_df_long[("id_features","span_id_target")] = relation_df_long[("id_features", "id_relation")].map(lambda x: x.split("[")[1].split("_")[1][:-1])
    relation_df_long[("id_features","type_relation")] = relation_df_long[("id_features", "list_tuple")].map(lambda x: x[0])
    
    relation_df_long_grouped = relation_df_long\
                                .groupby([("id_features","token_par_id_origin"), ("id_features", "span_id_origin")])\
                                .agg({("id_features", "type_relation"): list, 
                                      ("id_features","token_par_id_target"): list, 
                                      ("id_features","span_id_target"): list}).reset_index()
    
    def concat_span_id_relation(row): 
        return [row[("id_features","span_id_origin")] + "_" + x for x in row[("id_features","span_id_target")]]
        
    relation_df_long_grouped[("id_features", "span_id_relation")] = \
                relation_df_long_grouped.apply(concat_span_id_relation, axis = 1)
    
    
    relation_df_long_grouped = relation_df_long_grouped\
                                .rename(columns = {"type_relation": tuple_var[0][1], 
                                         "token_par_id_target": tuple_var[1][1], 
                                         "span_id_relation": tuple_var[0][1] + "span_id", 
                                         "token_par_id_origin": "token_par_id"})
    
    relation_df_long_grouped = relation_df_long_grouped\
                            .drop([("id_features", "span_id_origin"), ("id_features","span_id_target")], axis = 1)    
    return relation_df_long_grouped




def conll_relation_token_df(token_df, list_relation_variables): 
    """taking as input the token_df in the webanno tsv2 format, 
    changes the relation format to the conll format format
    but keeps the span annotations in the webanno tsv2 format"""
    conll_token_df = token_df.copy(deep = True)
    for tuple_var in list_relation_variables: 
        relation_df_temp = iob_relation_sub_df(conll_token_df, tuple_var)
        conll_token_df = conll_token_df.drop(list(tuple_var), axis = 1)
        conll_token_df = pd.merge(conll_token_df, relation_df_temp, on = [("id_features", "token_par_id")], how = "left")
        conll_token_df = conll_token_df.fillna("_")
    return conll_token_df



def span_annotation_id(x):
    """intermediate helper function  to get the 
    id of the annotated token 
    Returns ST if the token has no desambiguation id 
    Returns None if the token is not annotated"""
    if x== "_": 
        return None
    reg_brackets = r"\[\d+\]"
    res = re.search(reg_brackets, x)
    if res:
        span = res.span()
        return x[span[0]+1:span[1]-1]
    else: 
        return "ST" # which stands for single token 


def aux_iob_tag(row): 
    """intermediate helper function to find IOB tag"""
    if pd.isna(row[("id_features", "id")]): 
        return "O"
    elif (pd.isna(row[("id_features", "id")])==False) and (pd.isna(row[("id_features", "lag")])==True): 
        return "B"
    else: 
        return "I"

def iob_token_df(token_df: pd.DataFrame, variables: List[str]) -> pd.DataFrame: 
    """ From the token_df (pandas dataframe with the webanno tsv3 format)
    and the list of span variables, returns a df with the IOB format for each 
    of the span variable
    """
    iob_token_df = token_df.copy(deep = True)
    for variable in variables: 
        iob_token_df[("id_features", "id")] = iob_token_df[variable].apply(span_annotation_id)
        iob_token_df[("id_features", "lag")] =  iob_token_df[("id_features", "id")].shift(periods=1)
        iob_token_df[("id_features", "IOB")] = iob_token_df.apply(aux_iob_tag, axis = 1)
        iob_token_df[("id_features", "variable_no_id")] = iob_token_df[variable].apply(lambda x: re.sub(r"\[\d+\]$", "", x))
        
        def concat_iob_var(row): 
            if row[("id_features", "IOB")]=="O":
                return "O"
            else: 
                return row[("id_features", "IOB")] + "-" + row[("id_features", "variable_no_id")]
        
        iob_token_df[variable] = iob_token_df.apply(concat_iob_var, axis = 1)
        iob_token_df = iob_token_df.drop([("id_features", "id"), ("id_features", "lag"), 
                                  ("id_features", "IOB"), 
                                  ("id_features", "variable_no_id")], axis = 1)
    return iob_token_df 
    
    
def new_dict_features_by_type(dict_features_by_type: Dict, mapping_dict: Dict):
    """generates the dict feature by type corresponding to the new schema """
    df_mapping = pd.DataFrame({"layer_old": [x[0] for x in mapping_dict.keys()], 
                                         "layer_new": [x[0] for x in mapping_dict.values()],
                                         "feature_old": [x[1] for x in mapping_dict.keys()],
                                         "feature_new": [x[1] for x in mapping_dict.values()]})
    dict_layer_type = {}
    for key in dict_features_by_type.keys():
        dict_layer_type[key] = list(dict_features_by_type[key].keys())
    list_tuples = list(itertools.chain.from_iterable([[(key, x) for x in dict_layer_type[key]] for key in dict_layer_type.keys()])) 
    df_layer = pd.DataFrame(list_tuples, columns =["layer_type", "layer_old"])
    df_mapping = pd.merge(df_mapping, df_layer, on = "layer_old", how = "left")
    
    df_mapping["tuple_var"] = df_mapping.apply(lambda x: (x["layer_new"], x["feature_new"]), axis = 1)
    
    dfg = df_mapping.groupby(["layer_type", "layer_new"]).agg({"tuple_var": list}).reset_index()
    
    new_dict = {"span_variables": {}, "relation_variables": {}}
    for row, item in dfg.iterrows(): 
        new_dict[item["layer_type"]][item["layer_new"]] = item["tuple_var"]
    
    return new_dict 

    
   

