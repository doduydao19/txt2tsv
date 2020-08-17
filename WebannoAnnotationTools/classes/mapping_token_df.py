# -*- coding: utf-8 -*-


mapping_features = {'BT_webanno.custom.Evaluation': "relation_evaluation_id", 
                    'BT_webanno.custom.Structure': "relation_date_id", 
                    'BT_webanno.custom.Victimes': "relation_victime_id"}

revert_mapping_features = dict(zip(mapping_features.values(), mapping_features.keys()))

token_df = token_df.rename(columns = mapping_features, level = 1)
token_df = token_df.rename(columns = revert_mapping_features, level = 1)

token_df = token_df.rename(columns = {}, level = 1)

token_df = token_df.rename(columns = {}, level = 0)
