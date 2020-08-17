# WebannoAnnotationTools

This package enables to create pre annotated documents in the Webanno custom format (webannoTsv3) and to convert annotated documents from the webbing tsv3 to a pandas data frame 


# Class text_to_tsv
Enables to convert a txt file to a webanno Tsv3 file. 
Several methods enable to add tags. 

# Class annotated_doc : 
Enables to read tsv3 files and to convert them into pandas dataframe
Attribute "token_dataframe" is a pandas df with multiindex columns with: 

- one line per token 
- column names: 
  - level 0 correspond to the layer 
  - level 1 to the variables of the layer 

## to do in further developments 

    convert the webanno tsv3 format to other standard annotated format (IOB for tags, CONLL for relations) 
    add functionalities of pre tagging using SpaCy
