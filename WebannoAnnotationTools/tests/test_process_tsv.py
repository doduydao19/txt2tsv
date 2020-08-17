# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 14:35:29 2019

@author: kim.montalibet
"""



### to do 
# rajouter les relations quand tag poste 
# si on ne trouve pas l'entité avec le tag -> le rajouter sur le tag lui meme


##### TO DO : remplace "_" par une regex
#regex_nul = r"_(\|_){0,5}"
#if re.match(regex_nul, "_|_|_|_"): 
#    print("yes")


# import configs
# dir_path = configs.load("C:\\Users\\Mr.SpAm-PC\\PycharmProjects\\STM\\Data")

import os 
from Process.WebannoAnnotationTools.classes.annotated_doc_class import annotated_doc


# to decide 
from Process.WebannoAnnotationTools.classes.join_entities_with_relations import join_relations

# spefific to my project -> to put in Annotation and not in Webanno annotation tools 
from Process.WebannoAnnotationTools.classes.replace_tag_by_relation import replace_tag_by_relation
from Process.WebannoAnnotationTools.classes.add_relation_unites import add_relation_valeur_unite
from Process.WebannoAnnotationTools.tests.reconcile_info_structure import add_structure_to_all_tokens


#from WebannoAnnotationTools.token_to_chunk_df import token_to_chunk_df
import pandas as pd
import numpy as np 
from typing import List, Tuple
import re
from Process.WebannoAnnotationTools.classes.text_to_tsv_class import unlist_df


list_files = os.listdir( "C:\\Users\Mr.SpAm-PC\PycharmProjects\STM\Data\\1000Doc2" )
file = list_files[0]
#file = "1594890.tsv"
#file = ""



regex_chiffre = r"([^\d]*)(\d+[\.\d+]{0,1})([^\d]*)"
get_nb_from_string = lambda x: re.sub(regex_chiffre, r"\2", x)




# load tsv file and convert into annotated doc class 
anno_doc = annotated_doc(dir_path + "gold_annotation/gold_dataset2/"+ file)
token_df = anno_doc.token_dataframe


## replace the double tag by relation for valeur / poste
layer = "Evaluation"
layer_relation = "Relation_evaluation"
source_var = "A_Valeur"
target_var = "liste_postes"
relation_var = "BT_webanno.custom.Evaluation"
window = 30
anno_doc.modify_token_dataframe(lambda x: replace_tag_by_relation(x, layer, layer_relation, 
                                        source_var, target_var, relation_var))

## replace the double tag by relation for valeur

token_df.Evaluation.columns
layer = "Evaluation"
layer_relation = "Relation_evaluation"
source_var = "A_Valeur"
target_var = "liste_postes"
relation_var = "BT_webanno.custom.Evaluation"
window = 30
anno_doc.modify_token_dataframe(lambda x: replace_tag_by_relation(x, layer, layer_relation, 
                                        source_var, target_var, relation_var))

token_df = anno_doc.token_dataframe

## replace the double tag by relation for autre sous partie

token_df.Evaluation.columns
layer = "Evaluation"
layer_relation = "Relation_evaluation"
source_var = "A_Valeur"
target_var = "C_autre_sous_partie"
relation_var = "BT_webanno.custom.Evaluation"
window = 30
anno_doc.modify_token_dataframe(lambda x: replace_tag_by_relation(x, layer, layer_relation, 
                                        source_var, target_var, relation_var))








chunk_df = anno_doc.chunk_df()

var_unite = ("Evaluation", "type_information")
var_valeur = ("Evaluation", "A_Valeur")
var_relation = ("Relation_evaluation", "BT_webanno.custom.Evaluation")


chunk_df = add_relation_valeur_unite(chunk_df, var_unite, var_valeur, var_relation)
# do the same for unité expertise 
var_unite = ("Evaluation", "A_Unite_expertise")
chunk_df = add_relation_valeur_unite(chunk_df, var_unite, var_valeur, var_relation)




            
partie_var = ("Structure", "zonage_macro")
sous_partie_var = ("Structure", "sous_partie")
victime_var = ("Structure", "victime_concernee")          
            
chunk_df = add_structure_to_all_tokens(chunk_df, partie_var, sous_partie_var, victime_var)  

action_deboutee = chunk_df[chunk_df[("Evaluation", "A_Valeur")]=="action_deboutee"]
action_deboutee = action_deboutee[[("Structure", "sous_partie_all"), 
                                   ("Structure", "victime_all"),
                                   ("Evaluation", "A_Valeur")]]
action_deboutee.columns = action_deboutee.columns.droplevel()
action_deboutee = action_deboutee.rename(columns = {"A_Valeur": "valeur_tag"})

### test with dates 
token_df.Relation_structure.columns
target_variable = ("Structure", "E_evenement")
source_variable = ("Structure", "C_Date")
target_variable_alias = "evenement"
source_variable_alias = "date"

id_relation_variable = ("Relation_structure", "BT_webanno.custom.Structure")
relation_variable = ("Relation_structure", "relation_date") 

layer = target_variable[0]
layer_relation = id_relation_variable[0]



df_chrono = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 


#df_copy = chunk_df.copy(deep = True)
#sub_df = df_copy[["id_features", layer, layer_relation]]



#### test with amounts and postes 

target_variable = ("Evaluation", "liste_postes")
source_variable = ("Evaluation", "A_Valeur")
list_other_var = [("Structure", "partie_all"), 
                  ("Structure", "sous_partie_all"), 
                  ("Structure", "victime_all")]

target_variable_alias = "poste"
source_variable_alias = "valeur"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")



df_montant = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias, 
                   list_other_var) 


#df_montant = pd.merge(df_montant, action_deboutee, on = "sous_partie_all", how = "left")

df_append = df_montant[['poste_id', 'poste_token', 'poste_tag']]
df_append = df_append.drop_duplicates(subset = ['poste_tag'])
action_deboutee["merge"] = 1
df_append["merge"] = 1
df_append = pd.merge(df_append, action_deboutee, on = "merge")
df_append.drop(["merge"], axis = 1, inplace = True)

df_montant = pd.concat([df_montant, df_append], axis = 0, sort = False)


#### test with amounts and autre sous partie 

token_df.Evaluation.columns
target_variable = ("Evaluation", "C_autre_sous_partie")
source_variable = ("Evaluation", "A_Valeur")
target_variable_alias = "autre_sp"
source_variable_alias = "valeur"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")

df_montant_sp = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 




#### test with amounts and unite


target_variable = ("Evaluation", "A_Valeur")
target_variable_alias = "valeur"
source_variable = ("Evaluation", "type_information")
source_variable_alias = "unite"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")

df_montant_unite = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 



target_variable = ("Evaluation", "A_Valeur")
target_variable_alias = "valeur"
source_variable = ("Evaluation", "A_Unite_expertise")
source_variable_alias = "unite_exp"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")

df_montant_unite_exp = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 




#### test with amounts and victime concernée 


target_variable = ("Evaluation", "E_victime_concernee")
target_variable_alias = "victime_evaluation"
source_variable = ("Evaluation", "A_Valeur")
source_variable_alias = "valeur"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")

df_montant_victime = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 



target_variable = ("Evaluation", "A_Valeur")
target_variable_alias = "valeur"
source_variable = ("Evaluation", "A_Unite_expertise")
source_variable_alias = "unite_exp"
id_relation_variable = ("Relation_evaluation", "BT_webanno.custom.Evaluation")
relation_variable = ("Relation_evaluation", "relation_evaluation")

df_montant_unite_exp = join_relations(chunk_df, target_variable, source_variable,
                   id_relation_variable, 
                   relation_variable, 
                   target_variable_alias, 
                   source_variable_alias) 





### join the amount variable 

df_res = pd.merge(df_montant, df_montant_unite, 
                  on = ["valeur_id", "valeur_token", "valeur_tag"], 
                  how = "left")

df_res = pd.merge(df_res, df_montant_unite_exp, 
                  on = ["valeur_id", "valeur_token", "valeur_tag"], 
                  how = "left")

df_res = pd.merge(df_res, df_montant_sp, 
                  on = ["valeur_id", "valeur_token", "valeur_tag"], 
                  how = "left")



df_res = pd.merge(df_res, df_montant_victime, 
                  on = ["valeur_id", "valeur_token", "valeur_tag"], 
                  how = "left")




# reconcil sous partie and autre sous partie 
df_res = df_res.applymap(lambda x: np.nan if x=="_" else x)
df_res["sous_partie_rec"] = df_res.apply(lambda x: 
    x["autre_sp_tag"] if pd.isnull(x["autre_sp_tag"])==False else x["sous_partie_all"], axis = 1)

    
# reconcil victime structure and victime evaluation 
df_res["victime_rec"] = df_res.apply(lambda x: 
    x["victime_evaluation_tag"] if pd.isnull(x["victime_evaluation_tag"])==False else x["victime_all"], axis = 1)
    
df_res["victime_rec"] = df_res["victime_rec"].fillna("victime_1")   







# replace demande deboutée par 0 and valeur nulle par 0 
set(df_res.valeur_tag)
#mapping_valeur = {"demande_deboutee": 0, "valeur_nulle": 0}
df_res["valeur_token"] = df_res.apply(lambda x: 
    "0" if x["valeur_tag"] in ["demande_deboutee", "valeur_nulle", "action_deboutee"] 
        else (-1 if x["valeur_tag"]=="confirmation_1ere_instance" else  x["valeur_token"]),
    axis = 1)

df_res["unite_exp_tag"] = df_res.apply(lambda x: 
    "echelle_1_7" if "/7" in x["valeur_token"] else x["unite_exp_tag"], 
    axis = 1)
 
df_res["valeur_token"] = df_res["valeur_token"].map(lambda x: x.replace('/7', ""))



 # to do : extract number only when there are number + letters    
#df_res["valeur_token"] = df_res["valeur_token"].apply(lambda x: re.search(r"\d+[,\d+]{0,1}", ))



# fill missing unite with "description" when valeur == description else with euros 

df_res["unite_exp_tag"]= df_res.apply(lambda x: 
    "description" if (x['valeur_tag']=="description")\
    else x["unite_exp_tag"], axis = 1)

df_res["unite_tag"]= df_res.apply(lambda x: 
    "euros" if ((pd.isnull(x["unite_tag"])==True) and (pd.isnull(x["unite_exp_tag"])==True))\
    else x["unite_tag"], axis = 1)


### filter and reshape df res in order to have : one row par poste de préjudice, 
# avec les montants en euros uniquement 

df_euro = df_res[df_res['unite_tag']=="euros"]
df_euro["valeur_token"] = df_euro["valeur_token"].map(lambda x: x.replace(',', ".") if type(x)==str else x)
df_euro["valeur_token"] = df_euro["valeur_token"].map(lambda x: re.sub(regex_chiffre, r"\2", x))
df_euro["valeur_token"] = df_euro["valeur_token"].astype(float)
    
df_euro = df_euro[["poste_tag", "valeur_token", "sous_partie_rec", "victime_rec"]]
print(len(df_euro))
df_euro = df_euro.drop_duplicates()
print(len(df_euro))

#### TO DO : check if there are some inconsistencies here 



## reshape df euro in order to have one row per poste (pivot table)

df_pv = pd.pivot_table(df_euro, values='valeur_token', index=['poste_tag', "victime_rec"],
                       columns=['sous_partie_rec'], 
                       aggfunc="first").reset_index(drop = False)



## create table with expertise 
df_exp = df_res[df_res['unite_exp_tag'].isin(["echelle_1_7", "pourcentage"])]
df_exp["valeur_token"] = df_exp["valeur_token"].map(get_nb_from_string)
df_exp["valeur_token"] = df_exp["valeur_token"].map(lambda x: re.sub(regex_chiffre, r"\2", x))
df_exp["valeur_token"] = df_exp["valeur_token"].astype(float)

# si plusieurs valeurs d'expertise, on retient la dernière 
# à modifier plus tard et créer une colonne avec une liste / un dico avec 
# les différentes valeurs 
df_exp = df_exp[["poste_tag", "valeur_token", "valeur_tag", 
                 "unite_exp_tag", "valeur_id", "victime_rec"]]
df_exp = df_exp.drop_duplicates()
df_exp["position"] = df_exp["valeur_id"].map(lambda x: 
    int(x.split("-")[0])*1000 + int(x.split("-")[1]))
df_exp = df_exp.sort_values(by = "position", ascending = False)
df_exp = df_exp.drop_duplicates(subset = ["poste_tag", "victime_rec"], keep = "first")
df_exp = df_exp.drop(["valeur_id", "position", "valeur_tag"], axis = 1)
df_exp = df_exp.rename(columns = {"valeur_token": "cotation"})


## create table with description  
df_desc = df_res[df_res['unite_exp_tag'].isin(["description"])]
df_desc = df_desc[["poste_tag", "valeur_token", "victime_rec"]]
df_desc = df_desc.drop_duplicates(subset = ["poste_tag", "victime_rec"], keep = "first")
df_desc = df_desc.rename(columns = {"valeur_token": "description_expertise"})




# join df_pv and df_exp 
df_all = pd.merge(df_pv, df_exp, on = ["poste_tag", "victime_rec"], how = "left")
df_all = pd.merge(df_all, df_desc, on = ["poste_tag", "victime_rec"], how = "left")


      
################################################################################
#### infos victimes 
################################################################################

df_vict = chunk_df[["Victimes", "id_features"]]
df_vict.columns = df_vict.columns.droplevel()
df_vict = df_vict.rename(columns = {'A_id_victime': "id", 'B_partie_proces': "partie_proces", 
                                    'D_date_naissance': "date_naissance",
                                   'H_etat_anterieur': "etat_anterieur", 'Sexe_victime': "genre", 
                                   'age_victime': "age", 'profession_victime': "professsion"})


df_vict["filter"] = df_vict.apply(lambda x : 0 if ((x["id"]=="_") and (x["partie_proces"]!="_")) else 1, axis = 1)
df_vict = df_vict[df_vict["filter"] ==1]
df_vict.drop(["filter"], axis = 1, inplace = True)


df_vict["id"] = df_vict["id"].apply(lambda x: x if x!="_" else "victime_1")

def get_filled_fields(row, list_col): 
    list_val = [(x, row[x], row["token_joined"]) for x in list_col if row[x]!="_"]
    return list_val

col_list = ['partie_proces', 'date_naissance', 'etat_anterieur', 'genre',
       'age', 'autre_role_litige', 'deces', 'lien_parente_vind',
       'partiesducorps', 'professsion']

df_vict["col_value_list"] = df_vict.apply(lambda x: get_filled_fields(x, col_list), axis = 1)
df_vict = df_vict[df_vict.col_value_list.apply(len) >0]

df2 = unlist_df(df_vict, "col_value_list", ["id"])
df2["variable"] = df2["col_value_list"].apply(lambda x : x[0])
df2["tag"] = df2["col_value_list"].apply(lambda x : x[1])
df2["token"] = df2["col_value_list"].apply(lambda x : x[2])

df2.drop(["col_value_list"], axis = 1, inplace = True)



## pour les valeurs tokens sans unités, si /7 l'enlever et mettre l'unité en échelle 1_7
### pour les valeurs qui ne sont pas des chiffres: faire un script qui rajoute les unités 


# valeur nulle 

# rajouter : la victime concernée, perte de chance, part de resp







# rajouter autre sous partie 

# rajouter sous partie 


# retraiter les valeurs sans les liens 
    # questions: fait-on 







### here we do not take into account the desambiguation id
# to imporve: 
   # - pb in the processing of the token to chunk df: we create desamb id 
   # for single token in order to group by :
    #    - maybe transform these after the group by in the function in order to replace them with 0 
     #   (replace the desamb greater than 500 by 0)
        
