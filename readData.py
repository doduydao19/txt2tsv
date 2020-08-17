# -*- coding: utf-8 -*-
from nltk.tokenize import sent_tokenize
path = "C:\\Users\\Mr.SpAm-PC\\PycharmProjects\\STM\\"
num =1
while num < 1000 :
    inPath = path +"Data\\1000Doc\\"+str(num)+".txt"
    outPath = path+ "ouput\\sent_tokened\\"+str(num)+".txt"

    # readFile -)TXT -) line by line:
    id = 0;
    inFile = open(inPath, "r",encoding= "utf-8")
    text = ""
    for line in inFile:
        text += line
    # print(text)
    text = text.replace("\n", " ")
    # write to file:
    outFile = open(outPath,"w",encoding="utf-8")
    # print(type(sent_tokenize(text)))
    id = 1
    for sen in sent_tokenize(text):
        outFile.write(sen + "\n")
        # print(str(id) + " " + sen + "\n")
        id+=1
    num += 1
    inFile.close()
    outFile.close()
print("done!")