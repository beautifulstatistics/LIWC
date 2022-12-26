import dask.dataframe as dd
import dask
from dask.diagnostics import ProgressBar
import time

from functools import partial
print = partial(print,flush=True)

def return_time():
    return time.strftime('%H:%M', time.localtime())

if __name__ == "__main__":
    ProgressBar(dt=60).register()
    dask.config.set(scheduler='processes',num_workers=7)
        
    X = dd.read_csv("./data/counts.csv/*.part",engine='python')
    X.columns = [x.replace("(","").replace(")","") for x in X.columns]

    print("Computing sums: ", return_time())
    sums = X.sum().compute()
    to_exclude = sums == 0
    print(sums)
    print("Exclude: ", sums.index[to_exclude])

    with open("./data/single_models.formulas",'w') as f:
        lf = [f'permission_denied ~ {x}' for x in sums.index[~to_exclude]]
        lf_string = "\n".join(lf)
        f.write(lf_string)

    X = X.astype(int)
    print("Writing to disk. ", return_time())
    X.to_csv("./data/ready_for_models.csv",index=False)
    print("Finshed. ", return_time())