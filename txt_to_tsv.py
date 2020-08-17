# -*- coding: utf-8 -*-
import csv
import re
from nltk.tokenize import word_tokenize as w_token
from Process.WebannoAnnotationTools.classes import text_to_tsv_class as tc
pathIn = "C:\\Users\Mr.SpAm-PC\PycharmProjects\STM\ouput\sent_tokened\\"
pathOut = "C:\\Users\Mr.SpAm-PC\PycharmProjects\STM\ouput\TSV\\"
num = 1
patternNum = '[0-9]+'
# patternChar = '[:|/|-]'
while num < 1000:
    fileIn = open(pathIn+ str(num)+".txt","r+",encoding="utf-8")
    fileOut = pathOut+str(num)+".tsv"

    # create text
    # Process text
    text = ""
    for line in fileIn:
        words = w_token(line)
        for word in words:
            if "|" in word and len(word) > 1:
                #print(word)
                word = word.replace("|", "| ")
            if ":" in word and len(word) > 1:
                #print(word)
                word = word.replace(":", " : ")
            if "/" in word and len(word) > 1:
                #print(word)
                word = word.replace("/", " / ")
            if "-" in word and len(word) > 1:
                #print(word)
                word = word.replace("-", " - ")
            if re.match(patternNum,word) and len(word) > 1:
                # print("before "+word)
                if "/" in word and len(word) > 1:
                    # print(word)
                    word = word.replace("/", "/")
                else:
                    word = re.sub(patternNum,re.match(patternNum,word).group()+" ",word)
                # print("after " + word)
            text += word +" "
        text += "\n"
    # print(text)
    # convert
    list_header_no_tag = [['#FORMAT=WebAnno TSV 3.2'], [''], ['']]
    dict_features_by_type ={"span_variables": {}, "relation_variables": {}}

    list_header = tc.create_list_header(dict_features_by_type= dict_features_by_type)

    nb_features = tc.number_of_features(dict_features_by_type)  # idem
    nb_desamb_id=200

    list_paragraphs = tc.split_text_into_paragraphs(text= text)
    list_paragraph_spans = tc.paragraph_spans(list_paragraphs)
    text_titles = tc.text_titles_only(text= text
                                      ,list_par= list_paragraphs
                                      ,list_spans= list_paragraph_spans)
    list_paragraph_tokens = tc.split_par_into_tokens(text_par= list_paragraphs)
    list_paragraph_token_spans = tc.split_par_into_token_spans(text_par= list_paragraphs)

    paragraph_dataframe = tc.create_paragraph_dataframe(text_par=list_paragraphs
                                                        ,par_spans=list_paragraph_spans
                                                        ,par_tokens=list_paragraph_tokens
                                                        ,par_token_spans=list_paragraph_token_spans)
    token_dataframe = tc.create_token_dataframe(df_par=paragraph_dataframe)
    webanno_tsv3 = tc.convert_to_webanno_tsv3(df=token_dataframe
                                            ,text_par=list_paragraphs
                                            ,dict_features_by_type= dict_features_by_type
                                            ,list_header= list_header)
    # for data in webanno_tsv3:
    #     print(data,end="\n")
    # print(type(webanno_tsv3))\
    # write into file:

    with open(fileOut, 'w', encoding='utf-8', newline='') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t', lineterminator='\n')
        for line in webanno_tsv3:
            tsv_writer.writerow(line)
    print("writed: " + str(num) + ".tsv")
    num +=1
    out_file.close()
    fileIn.close()
print("write DONE !")