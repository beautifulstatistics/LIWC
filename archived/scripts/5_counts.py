from collections import Counter
import numpy as np
import dask
import dask.dataframe as dd
import pandas as pd
import liwc
import time
from dask.diagnostics import ProgressBar

from functools import partial
print = partial(print,flush=True)

if __name__ == '__main__':
    print(time.strftime('%H:%M', time.localtime()))
    ProgressBar(dt=60*15).register()
    dask.config.set(scheduler='processes',num_workers=7)
    parse, category_names = liwc.load_token_parser(
        './data/dictionaries/LIWC2015 Dictionary - Chinese (Simplified)(adjusted).dic')

    we = dd.read_csv("./data/segmented.csv")
    print(we)
    print("nrows:",we.shape[0].compute())

    we = we.head(n=5000,compute=False)

    print(we)

    def count(weibo):
        counts = Counter()
        counts['totallen'] = len(weibo)
        for token in weibo.split():
            counts.update(['tokencount'])
            for category in parse(token):
                counts.update([category])
            else:
                counts.update(['notdict'])
        return counts

    counts = we.text.apply(count,meta=('x','object')).compute()
    print("Apply finished.")
    counts = pd.DataFrame.from_records(counts.values,columns=category_names)
    counts = counts.fillna(0).astype(np.int16)

    print('Counts made.')
    we = we.drop('text',axis=1)
    we = we.compute()
    we = pd.concat([we,counts],axis=1)

    print("Writing to disk.")
    we.to_csv("./data/counts.csv")
