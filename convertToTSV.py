# -*- coding: utf-8 -*-
from nltk.tokenize import word_tokenize as w_token

path = "C:\\Users\\Mr.SpAm-PC\\PycharmProjects\\STM\\"
numTxt = 1
while numTxt < 1000:
    inPathProcess = path + "ouput\\sent_tokened\\" + str(numTxt) + ".txt"
    outPath = path + "ouput\\TSV\\" + str(numTxt) + ".tsv"
    inFileP = open(inPathProcess, "r", encoding="utf-8")
    outFile = open(outPath, "w", encoding="utf-8")

    numOfSent = 1
    outFile.write("#FORMAT=WebAnno TSV 3.2" + "\n" + "#T_SP=webanno.custom.NER|value" + "\n\n")
    for sent in inFileP:
        outFile.write("#Text=" + sent.replace("\n", "") + ' ')
        words = w_token(sent)
        numOfWord = 1
        id_word = 0;
        for word in words:
            if "|" in word and len(word) > 1:
                newWord = word.replace("|", "")
                oldWord = "|"
                outFile.write(str(numOfSent) + "-" + str(numOfWord) + "    " + str(id_word) + "-" + str(
                    id_word + 1) + "    " + oldWord + "    _\n")
                numOfWord += 1
                id_word += len(word) + 1
                outFile.write(str(numOfSent) + "-" + str(numOfWord) + "    " + str(id_word) + "-" + str(
                    id_word + len(newWord)) + "    " + newWord + "    _\n")
            else:
                outFile.write(str(numOfSent) + "-" + str(numOfWord) + "    " + str(id_word) + "-" + str(
                    id_word + len(word)) + "    " + word + "    _\n")

            numOfWord += 1
            id_word += len(word) + 1
        numOfWord = 1
        numOfSent += 1
        outFile.write("\n")
    numTxt += 1
    inFileP.close()
    # inFileR.close()
    outFile.close()
print("done!")
