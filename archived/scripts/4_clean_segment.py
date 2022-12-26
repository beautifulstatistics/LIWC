# alternate:
import dask.dataframe as dd
import re
import preprocessor as tpp
import spacy

we = dd.read_csv("./data/sample.csv")
nlp = spacy.load("zh_core_web_lg")
def clean(text):
    text = tpp.clean(text)
    text = re.sub("\w+"," ",text)
    for t in nlp(text):
        yield t.text


we['text_seg'] = we.text.apply(clean)
we = we.drop('text',axis=1)
we.to_csv("./data/segmented.csv",index=False)