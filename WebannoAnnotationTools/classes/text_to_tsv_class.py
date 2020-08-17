# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 17:19:29 2019
@author: kim.montalibet
"""

import csv
import pandas as pd
from typing import List, Dict
import string
from nltk.tokenize import TreebankWordTokenizer as twt
import itertools
from Process.WebannoAnnotationTools.classes import taggers
import re


class text_to_tsv3:
    list_header_no_tag = [['#FORMAT=WebAnno TSV 3.2'], [''], ['']]

    def __init__(self, text, file_name,
                 dict_features_by_type={"span_variables": {}, "relation_variables": {}},
                 nb_desamb_id=200):
        self.text = text
        self.file_name = file_name
        # self.dict_features = dict_features

        self.dict_features_by_type = dict_features_by_type
        self.list_header = create_list_header(dict_features_by_type)  # to modify with the dict_feature_by_type

        self.nb_features = number_of_features(self.dict_features_by_type)  # idem
        self.nb_desamb_id = nb_desamb_id
        # self.mapping_order_variables = mapping_order_variables
        # self.mapping_dict = mapping_dict
        self.list_paragraphs = split_text_into_paragraphs(self.text)
        self.list_paragraph_spans = paragraph_spans(self.list_paragraphs)
        self.text_titles = text_titles_only(self.text, self.list_paragraphs, self.list_paragraph_spans)
        self.list_paragraph_tokens = split_par_into_tokens(self.list_paragraphs)
        self.list_paragraph_token_spans = split_par_into_token_spans(self.list_paragraphs)

        # returns a df with one row per paragraph
        self.paragraph_dataframe = create_paragraph_dataframe(self.list_paragraphs,
                                                              self.list_paragraph_spans,
                                                              self.list_paragraph_tokens,
                                                              self.list_paragraph_token_spans)

        # a df with one row per token
        self.token_dataframe = create_token_dataframe(self.paragraph_dataframe)
        self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe,
                                                    self.list_paragraphs,
                                                    self.dict_features_by_type,
                                                    self.list_header)

    def add_tag(self, layer, variable, dict_regex, file=None,
                single_match=False, function=None, limit_text=None):
        """
        variable: name of the variable in token_df
        variable_mapping: tuple of the type (layer, feature_name) with a
        correspondance with the annotation schema
        list_tuples_span_tag: a list of tuples of the type (span, tag)
        each corresponding to an annotation selection
        span = (start_index, end_index) of the selection
        tag: the value of the tag
        """

        # if instead of using regex, you use function
        # function must take as argument the text and must have as output a
        # list of tuples span tags

        if file:
            try:
                dict_regex = dict_regex[file]
            except:
                dict_regex = {}

        # def à modifier
        if limit_text:
            limit_text = int(limit_text * len(self.text)) if limit_text < 1 else limit_text
        text = self.text[:limit_text] if limit_text else self.text
        if (single_match == False) and (function == None):
            list_tuples_span_tag = taggers.find_all_with_dict_regex(text, dict_regex)
        elif single_match == True:
            list_tuples_span_tag = taggers.find_first_match(text, dict_regex)
        elif function:
            list_tuples_span_tag = function(self.text)
        self.token_dataframe[(layer, variable)] = self.token_dataframe.apply(
            lambda x: match_token_with_tag(x, list_tuples_span_tag, layer, variable, self.nb_desamb_id), axis=1)
        self.nb_desamb_id += len(list_tuples_span_tag)
        self.dict_features_by_type = update_dict_features_by_type(self.dict_features_by_type,
                                                                  "span_variables", layer, variable)
        self.nb_features = number_of_features(self.dict_features_by_type)
        self.list_header = create_list_header(self.dict_features_by_type)
        self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe,
                                                    self.list_paragraphs,
                                                    self.dict_features_by_type,
                                                    self.list_header)

    def add_single_token_tag(self, layer, variable, dict_regex, token_lag=None):
        if token_lag:
            self.token_dataframe[("id_features", "token_lag")] = self.token_dataframe[("id_features", "token")].shift(
                periods=token_lag).fillna("")

        self.token_dataframe[(layer, variable)] = self.token_dataframe.apply(
            lambda x: match_single_token(x, dict_regex, token_lag, layer, variable), axis=1)

        self.dict_features_by_type = update_dict_features_by_type(self.dict_features_by_type,
                                                                  "span_variables", layer, variable)
        self.nb_features = number_of_features(self.dict_features_by_type)
        self.list_header = create_list_header(self.dict_features_by_type)
        self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe,
                                                    self.list_paragraphs,
                                                    self.dict_features_by_type,
                                                    self.list_header)

    def apply_to_token_df(self, layer, variable, row_function):
        self.token_dataframe[(layer, variable)] = self.token_dataframe.apply(row_function, axis=1)

        self.dict_features_by_type = update_dict_features_by_type(self.dict_features_by_type,
                                                                  "span_variables", layer, variable)
        self.nb_features = number_of_features(self.dict_features_by_type)
        self.list_header = create_list_header(self.dict_features_by_type)
        self.webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe,
                                                    self.list_paragraphs,
                                                    self.dict_features_by_type,
                                                    self.list_header)

    # def add_relation(self, layer, variable):

    #   return None

    def save_webanno_tsv3(self, file_name, dict_features_by_type=None):
        """saves the webanno_tsv3 to a tsv file"""

        if dict_features_by_type:
            list_header = create_list_header(dict_features_by_type)

            webanno_tsv3 = convert_to_webanno_tsv3(self.token_dataframe,
                                                        self.list_paragraphs,
                                                        dict_features_by_type,
                                                        list_header)

        with open(file_name, 'w', encoding='utf-8', newline='') as out_file:
            tsv_writer = csv.writer(out_file, delimiter='\t', lineterminator='\n')
            for line in webanno_tsv3:
                tsv_writer.writerow(line)


######### auxiliary functions for the class annotated_text


### for titres zonage
def remove_punctuation(s: str) -> str:
    return s.translate(str.maketrans(string.punctuation, " " * len(string.punctuation)))


def is_capital_letter(par: str) -> bool:
    """check if a paragraph is all in capital letters"""
    return all(word.isupper() for word in par if word not in [" ", "\n"])


def text_titles_only(text, list_par, list_spans):
    for ind_par, par in enumerate(list_par):
        par_no_punct = remove_punctuation(par)
        if is_capital_letter(par_no_punct) == False:
            spans = list_spans[ind_par]
            text = text[:spans[0]] + "." * (spans[1] - spans[0]) + text[spans[1]:]
    return text


def update_dict_features_by_type(dict_features_by_type: Dict,
                                 variable_type: str,
                                 layer: str,
                                 variable: str) -> Dict:
    if layer not in dict_features_by_type[variable_type].keys():
        dict_features_by_type[variable_type][layer] = []

    if (layer, variable) not in dict_features_by_type[variable_type][layer]:
        dict_features_by_type[variable_type][layer].append((layer, variable))
    return dict_features_by_type


def split_text_into_paragraphs(text: str) -> List[str]:
    """when there are more than one \n, returns a paragraph containing
    \n instead of an empty "" """
    return [par if len(par) > 0 else "\n" for par in text.split("\n")]


def paragraph_spans(text_par: List[str]) -> List[List[tuple]]:
    """ returns spans of paragraphs"""
    list_span = [(0, len(text_par[0]))]
    start_span = list_span[0][0]
    end_span = list_span[0][1]
    for i, par in enumerate(text_par[1:]):
        increment = 0 if text_par[i] == "\n" else 1
        start_span = end_span + increment
        end_span = start_span + len(par)
        list_span.append((start_span, end_span))
    return list_span


def split_par_into_tokens(text_par: List[str]) -> List[List[str]]:
    "returns list of tokens for each paragraph"
    par_tokens = [[x for x in list(twt().tokenize(s))] for s in text_par]
    par_tokens = [[x.replace('``', '"').replace("''", '"') for x in tokens] for tokens in par_tokens]
    return [par if len(par) > 0 else ["\n"] for par in par_tokens]


def split_par_into_token_spans(text_par: List[str]) -> List[List[tuple]]:
    """for each paragraph in the list, returns a list of the token spans"""
    par_tokens = [[x for x in list(twt().span_tokenize(s))] for s in text_par]
    return [par if len(par) > 0 else [(0, 1)] for par in par_tokens]


def create_paragraph_dataframe(text_par: List[str], par_spans: List[tuple], par_tokens: List[List[str]],
                               par_token_spans: List[List[tuple]]) -> pd.DataFrame:
    par_ids = [list(range(len(tokens))) for tokens in par_tokens]
    par_tuples = [list(zip(l1, l2, l3)) for (l1, l2, l3) in list(zip(par_ids, par_tokens, par_token_spans))]

    df_par = pd.DataFrame({"text": text_par, "par_tuples": par_tuples, "par_id": list(range(len(par_tokens))),
                           "par_spans": par_spans, "tokens": par_tokens})

    return df_par


def create_token_dataframe(df_par: pd.DataFrame) -> pd.DataFrame:
    df_token = unlist_df(df_par, 'par_tuples', ['par_id', 'par_spans'])

    df_token["token_id"] = df_token["par_tuples"].apply(lambda x: x[0])
    df_token["token"] = df_token["par_tuples"].apply(lambda x: x[1])
    df_token["token_span"] = df_token["par_tuples"].apply(lambda x: x[2])
    df_token.drop(["par_tuples"], axis=1, inplace=True)
    df_token["start_index"] = df_token["token_span"].apply(lambda x: x[0]) + df_token["par_spans"].apply(lambda x: x[0])
    df_token["end_index"] = df_token["token_span"].apply(lambda x: x[1]) + df_token["par_spans"].apply(lambda x: x[0])

    df_token.columns = pd.MultiIndex.from_product([["id_features"], df_token.columns])
    df_token = df_token.reset_index()
    return df_token


# to modify with dict_features_by_type
def number_of_features(dict_features_by_type: Dict) -> int:
    """computes the number of features from the dict of features"""
    nb_features = sum(
        [sum([len(x) for x in dict_features_by_type[key].values()]) for key in dict_features_by_type.keys()])
    return nb_features


def create_list_header(dict_features_by_type: Dict) -> List[List[str]]:
    list_header = [['#FORMAT=WebAnno TSV 3.2']]
    if dict_features_by_type != {"span_variables": {}, "relation_variables": {}}:
        mapping_var_type = {"span_variables": "SP", "relation_variables": "RL"}
        for feature_type in dict_features_by_type.keys():
            start_header = '#T_{}=webanno.custom.'.format(mapping_var_type[feature_type])
            dict_features = dict_features_by_type[feature_type]
            if dict_features != {}:
                for key in dict_features.keys():
                    line = [start_header + key + "|" + "|".join([x[1] for x in dict_features[key]])]
                    list_header.append(line)
    list_header += [[""], [""]]
    return list_header


def convert_to_webanno_tsv3(df: pd.DataFrame, text_par: List[str],
                            dict_features_by_type: Dict,
                            list_header: List) -> List[List[str]]:
    for type_var in dict_features_by_type.keys():
        dict_features = dict_features_by_type[type_var]
        for layer in dict_features.keys():
            list_var = dict_features[layer]
            for var in list_var:
                if var not in df.columns:
                    df[var] = "_"

    replacement_lb = " "
    text_par = [replacement_lb if x == "\n" else x for x in text_par]
    # text_par = [x if len(x)>0 else " " for x in text_par]
    list_lines_tsv = []

    df = df.replace("\n", replacement_lb)

    dfg = df.groupby([("id_features", "par_id")])

    for i, group in dfg:
        sent = text_par[i]
        text_sent = ["#Text=" + sent]
        list_lines_tsv.append(text_sent)
        for row_index, row in group.iterrows():
            col_1 = '{0}-{1}'.format(row[("id_features", "par_id")] + 1, row[("id_features", "token_id")] + 1)
            col_2 = '{0}-{1}'.format(row[("id_features", "start_index")], row[("id_features", "end_index")])
            list_tuple_var = [
                [[tuple_var for tuple_var in list_tuple_var] for list_tuple_var in dict_features_by_type[key].values()]
                for key in dict_features_by_type.keys()]
            list_tuple_var = list(itertools.chain.from_iterable(list(itertools.chain.from_iterable(list_tuple_var))))
            feature_row = [row[tuple_var] for tuple_var in list_tuple_var] + ["_"]

            list_lines_tsv.append([col_1, col_2, row[("id_features", "token")]] + feature_row)
        list_lines_tsv.append([])

    list_lines_tsv = list_header + list_lines_tsv[:-1]

    return list_lines_tsv


def match_token_with_tag(row, list_tuples_span_tag, layer, variable, nb_desamb_id):
    """ function that applies to the rows of the token dataframe and test
    whether the token in the row belongs to one the span in the list
    Returns tag + tag desambiguation id if test is positive
    None otherwise
    """
    try:
        filled_variable = row[(layer, variable)]
    except:
        filled_variable = "_"

    if filled_variable == "_":
        token_start_span = row[("id_features", "start_index")]
        token_end_span = row[("id_features", "end_index")]
        if len(row[("id_features", "token")]) > 1:
            if row[("id_features", "token")][1] == "'":
                token_start_span += 2
            if row[("id_features", "token")][-1] in string.punctuation:
                token_end_span += -1
        values = [((token_start_span >= x[0][0]) and (token_end_span <= x[0][1])) for x in list_tuples_span_tag]
        if any(values):
            index_match = values.index(max(values))
            return list_tuples_span_tag[index_match][1] + "[{}]".format(index_match + nb_desamb_id)

    return filled_variable


def match_single_token(row, dict_regex, token_lag, layer, variable):
    try:
        filled_variable = row[(layer, variable)]
    except:
        filled_variable = "_"

    if filled_variable == "_":
        if token_lag == -1:
            two_tokens = row[("id_features", "token")] + " " + row[("id_features", "token_lag")]
        elif token_lag == 1:
            two_tokens = row[("id_features", "token_lag")] + " " + row[("id_features", "token")]

        else:
            two_tokens = row[("id_features", "token")]

    for key in dict_regex.keys():
        list_regex = dict_regex[key]
        for regex in list_regex:
            if re.match(regex, two_tokens):
                filled_variable = key

    return filled_variable


def unlist_df(df: pd.DataFrame, col_with_list: str, id_cols: List[str]) -> pd.DataFrame:
    df_list_len_0 = df[df[col_with_list].map(len) == 0][[col_with_list] + id_cols]
    df_list_len_0[col_with_list] = None

    df_long = (df[col_with_list].apply(lambda x: pd.Series(x))
               .stack()
               .reset_index(level=1, drop=True)
               .to_frame(col_with_list)
               .join(df[id_cols], how='left')
               )
    df_res = pd.concat([df_long, df_list_len_0])
    return df_res
