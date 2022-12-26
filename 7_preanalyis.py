import dask.dataframe as dd
import dask
from dask.diagnostics import ProgressBar
import time

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

    X.columns = [x.split("(")[0] for x in X.columns]

    X = X.compute()
    cols = list(X.columns)
    cols.remove('permission_denied')

    print(return_time())
    
    Xg = X.groupby(cols, as_index = False).permission_denied.agg(['sum','size'])
    Xg['size'] = Xg['size'] - Xg['sum']
    Xg.rename({'sum':'censored','size':'not_censored'}, inplace=True,axis=1)

    print(return_time())

    Xg.to_csv('./data/consolidated_full.csv',index=True)
    print(return_time())
