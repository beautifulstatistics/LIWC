import dask
import dask.dataframe as dd
import preprocessor as tpp
from dask.diagnostics import ProgressBar
import time

from functools import partial
print = partial(print,flush=True)

print("\nProcess",__name__)
ProgressBar(dt=5).register()
dask.config.set(scheduler='processes',num_workers=7)

def main():
    t1 = time.time()
    we = dd.read_csv("./data/weibos.csv/0.part",engine='python')
    we = we.persist()
    we = we.repartition(npartitions=int(we.shape[0].compute()/10**3))
    we['text'] = we.text.apply(lambda x: x,meta=('x','str'))#tpp.clean(x)
    print(we)
    we = we.compute()
    print(we)
    we.to_csv("./data/clean.csv",index=False)
    t2 = (time.time() - t1)/60/60
    print(t2,'hours')

if __name__ == "__main__":
    main()