# -*- coding: utf-8 -*-

import re
from typing import Tuple, Dict, List


## functions used in the pre annotation 

def reformat_text(text): 
    text = text.replace('"', "''")
    text_par = text.split("\n")
    text_par = [x if ((len(x)> 100) or  (len(x)==0)) else  x + "     " + "."*(95-len(x))  for x in text_par]
    text = "\n".join(text_par)
    return text



def dedup_date_label_row(row): 
    if (row[("Victimes", "D_date_naissance")]!="_") or (row[("Victimes", "deces")]!="_"):
        return "_"
    else: 
        return row[("Structure", "C_Date")]


### functions used in the class



def deduplicate_non_disjoint_spans(list_span): 
    """ auxiliary function for find_list_regex_matches
    given the input list of spans, when there are two spans that span on one another, 
    eliminate the smallest from the list
    """
    list_span = sorted(list_span, key=lambda x: x[1], reverse = True)
    list_span = sorted(list_span, key=lambda x: x[0])
    for index, item in enumerate(list_span[:-1]):
        if item[1]> list_span[index+1][0]: 
            del list_span[index+1]
    return list_span 

def deduplicate_non_disjoint_tuple_spans(list_tuple_span): 
    """ auxiliary function for find_all_with_dict_regex
    given the input list of tuple spans, when there are two spans that span on one another, 
    eliminate the smallest from the list
    """
    list_tuple_span = sorted(list_tuple_span, key=lambda x: x[0][1], reverse = True)
    list_tuple_span = sorted(list_tuple_span, key=lambda x: x[0][0])
    to_drop = []
    for index, item in enumerate(list_tuple_span[:-1]):
        if item[0][1]> list_tuple_span[index+1][0][0]: 
            to_drop.append(index + 1)
    return [x for ind, x in enumerate(list_tuple_span) if ind not in to_drop]


def remove_start_end_blanks(regex, list_span):
    """ if regex starts with \n and end with multiple spaces 
    shortens the spans of the corresponing length"""
    if regex[0:2]==r"\n": 
        list_span = [(x[0]+1, x[1]) for x in list_span]
    if regex[-2:] == "  ": 
        list_span = [(x[0], x[1]-2) for x in list_span]
    return list_span





def find_list_regex_matches(text, regex):
    """ functions that returns the list of the input regular expression 
    (regex) found in the input text"""
    regex_test = re.compile(regex, flags=re.IGNORECASE)
    re_finditer = regex_test.finditer(text)
    list_span = [x.span() for x in re_finditer]
    list_span = deduplicate_non_disjoint_spans(list_span)
    return remove_start_end_blanks(regex, list_span)


def find_first_match(text, dict_regex): 
    list_tuple_match = []
    output = []
    for key in dict_regex: 
        list_regex = dict_regex[key]
        for regex_temp in list_regex: 
            test_search = re.search(regex_temp, text)
            if test_search: 
                list_tuple_match.append((test_search.span(), key))
    list_start_span = [x[0][0] for x in list_tuple_match]
    if len(list_start_span)>0: 
        index_min = list_start_span.index(min(list_start_span))
        output.append(list_tuple_match[index_min])
    return output



def create_list_tuples_span_tag(list_span, tag): 
    """ takes a list of span and returns a list of tuples of the form 
    [((start_index, end_index), tag), ..., (start_index, end_index), tag)]"""
    list_tuples = [(x, tag) for x in list_span]
    return list_tuples


def find_all_with_dict_regex(text: str, dict_regex: Dict) -> List[Tuple[Tuple, str]]: 
    """returns a list of (spans, tag) of matches strored in a dict 
    with keys = tags and values : list of regex """
    
    list_match = []
    for key in dict_regex.keys():
        list_regex = dict_regex[key]
        for regex in list_regex:
            list_match_temp = find_list_regex_matches(text, regex)
            list_match += create_list_tuples_span_tag(list_match_temp, key)

    return deduplicate_non_disjoint_tuple_spans(list_match)

def remove_wrong_positioned_titles(text, list_span_tag, cut = {"faits et procédure": (0.01,0.28), "moyens_pretentions": (0.10, 0.5), 
                              "motifs": (0.5,0.80), "dispositif": (0.75,1)}): 
    len_text = len(text)
    for element in list_span_tag: 
        key = element[1]
        if (element[0][0]/len_text < cut[key][0]) or (element[0][0]/len_text> cut[key][1]):
            list_span_tag.remove(element)
    return list_span_tag
    


