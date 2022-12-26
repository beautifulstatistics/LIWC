from statsmodels.tools.tools import add_constant
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

import pandas as pd
import time

from functools import partial

import multiprocessing as mp
import numpy as np

np.seterr(divide='ignore')

def return_time():
    return time.strftime('%H:%M', time.localtime())

def vif_core(i, X):
    return X.columns[i], vif(X.values, i)

def make_vif(X):
    with mp.Pool(processes=7) as pool:
        vif_data = {}
        for col_name, vif_value in pool.imap_unordered(partial(vif_core,X=X),list(range(X.shape[1]))):
            vif_data[col_name] = vif_value
    
    vif_data = pd.Series(vif_data)
    return vif_data.sort_values(ascending=False)

def main(X):
    print(return_time())
    vif_each = make_vif(X)
    while vif_each.iloc[0] > 5:
        col = vif_each.index[0]
        X = X.drop(col,axis=1)
        print("Col Dropped",col,vif_each.iloc[0])
        vif_each = make_vif(X)
    
    return X.columns

if __name__ == "__main__":
    ProgressBar(dt=30).register()

    print(return_time())
    X = dd.read_csv("./data/ready_for_models2.csv/*.part")

    # permission_denied = X['permission_denied']
    X = X.drop("permission_denied",axis=1)
    X = X.sample(frac=.001).compute().astype(float)
    X = add_constant(X)

    print("starting vif",return_time())
    print(make_vif(X))

    # n = 1
    # cols_to_keep = []
    # for _ in range(n):
    #     Xs = X.sample(n=250_000)
    #     cols_to_keep.append(main(Xs))
    
    # equal = []
    # for a,b in combinations(range(n),2):
        # equal.append(set(cols_to_keep[a]) == set(cols_to_keep[b]))

    # print(equal)
    # print(X.shape)

    # assert all(equal), 'Not all equal!'
    
    # X = X.loc[:,cols_to_keep[0]]

    # X['permission_denied'] = permission_denied
    # print("Writing to disk.")
    # X.to_csv("./data/ready_for_regression.csv",index=False)
    
    print("Finished.")