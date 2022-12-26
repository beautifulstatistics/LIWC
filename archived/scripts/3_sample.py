import dask
from dask.diagnostics import ProgressBar
import dask.dataframe as dd

from functools import partial
print = partial(print,flush=True)


print("\nProcess",__name__)
if __name__ == '__main__':
    ProgressBar(dt=5).register()
    dask.config.set(scheduler='processes',num_workers=7)

    df = dd.read_parquet('./data/weibos.parquet',engine="pyarrow")
    df = df.sample(frac=1)
    df = df.compute()
    df = dd.from_pandas(df,npartitions=10)
    df.to_csv('./data/sample.csv',index=False)