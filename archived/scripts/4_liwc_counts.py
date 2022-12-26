from collections import Counter
import numpy as np
import pandas as pd
import spacy

import liwc
import preprocessor as tpp

import time
import sys

global progressg
progressg = 0

def logged_apply(g, func, *args, **kwargs):
    step_percentage = 100. / len(g)
    lne = format(0, '3.0f')
    sys.stdout.write(lne + '%\n' )
    sys.stdout.flush()

    def logging_decorator(func):
        def wrapper(*args, **kwargs):
            global progressg

            progress = int(wrapper.count * step_percentage)
            if progress > progressg:
                lne = format(progress, '3.0f')
                sys.stdout.write(lne + '%\n' )
                sys.stdout.flush()
                progressg = progress
            wrapper.count += 1
            return func(*args, **kwargs)
        wrapper.count = 0
        return wrapper

    logged_func = logging_decorator(func)
    res = g.apply(logged_func, *args, **kwargs)
    return res

parse, category_names = liwc.load_token_parser(
        './data/dictionaries/LIWC2015 Dictionary - Chinese (Simplified)(adjusted).dic')

we = pd.read_csv("./data/sample.csv",low_memory=False,lineterminator='\n')
print("Data read in.")
t1 = time.time()
nlp = spacy.load("zh_core_web_lg")
def tokenize(text):
    text = tpp.clean(text)
    for t in nlp(text):
        yield t.text

def count(weibo):
    weibo = tokenize(weibo)
    return Counter(category for token in weibo for category in parse(token))

counts = logged_apply(we.text,count)
counts = pd.DataFrame.from_records(counts.values).fillna(0).astype(np.int16)

we = we.drop("text",axis=1)
we = pd.concat([we,counts],axis=1)

we.to_csv("./data/ready_for_vif.csv",index=False)
print("Finished")
print((time.time()-t1)/60)