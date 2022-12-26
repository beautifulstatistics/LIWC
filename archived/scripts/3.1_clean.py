import dask.dataframe as dd
import preprocessor as tpp
import time
import math

from dask.distributed import Client, LocalCluster

from functools import partial
print = partial(print,flush=True)

print("\nProcess",__name__)

def main():
    we = dd.read_csv("./data/weibos.csv/0.part",engine='python')
    we = we.persist()
    nparts = int(math.sqrt(we.shape[0].compute()))
    we = we.repartition(npartitions=nparts)
    we['text'] = we.text.apply(lambda x: tpp.clean(x),meta=('x','str'))
    print(we)
    we = we.compute()
    we.to_csv("./data/clean.csv",index=False)

if __name__ == "__main__":
    cluster = LocalCluster(n_workers=7) 
    client = Client(cluster)
    main()