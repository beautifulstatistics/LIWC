import os
import dask.dataframe as dd
import dask
from dask.diagnostics import ProgressBar
import pandas as pd

pd.set_option('display.float_format', str)
pd.options.display.max_rows = 999

import time

from tqdm import tqdm
tqdm.pandas()

from hypothesises import *
    
def return_time():
    return time.strftime('%H:%M', time.localtime())

if __name__ == "__main__":
    print(return_time())
    ProgressBar(dt=60).register()
    dask.config.set(scheduler='processes',num_workers=7)
    
    counts_path = os.path.join('data','counts','*.part')
    X = dd.read_csv(counts_path,engine='python')

    cols = [item for sublist in HYPOTHESISES for item in sublist]
    cols = list(set(cols))
    
    X = X.loc[:,cols + ['permission_denied']]
    X = X.compute()

    print(return_time())

    for name in cols:
        if name != 'totallen':
            print(name, (X[name] == 0).mean())
            X[name] = X[name] == 0

    Xg = X.groupby(cols, as_index = False).permission_denied.agg(['sum','size'])
    Xg['size'] = Xg['size'] - Xg['sum']
    Xg.rename({'sum':'censored','size':'not_censored'}, inplace=True,axis=1)

    Xg = Xg.loc[((Xg.censored != 0) & (Xg.not_censored != 0)),:]

    print(return_time())

    consolidated_path = os.path.join('data','consolidated_binned.csv')
    Xg.to_csv(consolidated_path,index=True)
    print(return_time())
