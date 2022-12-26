import dask.dataframe as dd
import dask
from dask.diagnostics import ProgressBar
import time

import pandas as pd

from tqdm import tqdm
tqdm.pandas()
    

from functools import partial
print = partial(print,flush=True)

def return_time():
    return time.strftime('%H:%M', time.localtime())

if __name__ == "__main__":
    print(return_time())
    ProgressBar(dt=5).register()
    dask.config.set(scheduler='processes',num_workers=7)
        
    X = dd.read_csv("./data/counts.csv/*.part",engine='python')
    X.columns = [x.replace("(","").replace(")","") for x in X.columns]

    X = X.drop(['totallen','tokencount','notdict'],axis=1)
    X = X.compute()
    cols = list(X.columns)
    cols.remove('permission_denied')

    print(return_time())
    
    Xg = X.groupby(cols, as_index = False).permission_denied.agg(['sum','size'])

    print(return_time())





    Xg.to_csv('./data/consolidated_sum_size.csv',index=True)
    print(return_time())