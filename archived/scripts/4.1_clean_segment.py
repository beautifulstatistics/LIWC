import pandas as pd
import re
import preprocessor as tpp
import os

we = pd.read_csv("./data/sample.csv")

we['text_preprocess'] = we.text.apply(lambda x: tpp.clean(x))
we['text_preprocess'] = we.text_preprocess.apply(lambda x: re.sub("(\n+|,+)"," ",x))

we = we.drop('text',axis=1)
we.to_csv("./data/sample_clean.csv",index=False,header=False)
we['text_preprocess'].to_csv("./data/to_segment.txt",index=False,header=False)

os.system("./stanford-segmenter-2020-11-17/segment.sh ctb ./data/to_segment.txt utf-8 0 1> ./data/segmented.csv 2> ./logs/4_clean_segment_standford.log")
