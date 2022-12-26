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

    with open("./data/dictionaries/linear_factors.txt",'r') as f:
        lf = f.read()
        lf = lf.replace("(","").replace(")","")
        lf = [x.split('/')[-1] for x in lf.split('\n')] + ['image']
        lf = [x for x in lf if x not in sums.index[to_exclude]]

        lf_string = " + ".join(lf)

        formula = f"permission_denied ~ {lf_string}\n"

    print(formula)
    with open("./data/linear_model.formula",'w') as f:
        f.write(formula)

    print(lf)
    lf.append("permission_denied")
    X = X.loc[:,lf]
    X = X.astype(int)
    print("Writing to disk. ", return_time())
    X.to_csv("./data/ready_for_models.csv",index=False)
    print("Finshed. ", return_time())