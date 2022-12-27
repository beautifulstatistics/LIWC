import os
import dask
from dask.diagnostics import ProgressBar
import dask.dataframe as dd

ProgressBar(dt=60).register()
dask.config.set(scheduler='processes',num_workers=7)

def main():
    week_path = os.path.join('data','weeks','week*.csv')
    df = dd.read_csv(week_path,encoding='utf-8',dtype=str,engine='python',
                    on_bad_lines='skip',**{'encoding_errors':'replace'})

    df = df.reset_index(drop=True).persist()
    df['permission_denied'] = ~df.permission_denied.isna()
    df['image'] = (df.image == 1).astype(bool)
    
    df = df.drop(['mid','retweeted_status_mid','uid','geo','created_at','retweeted_uid','source','deleted_last_seen'],axis=1)
    df = df.loc[~df.text.isna(),:]
    df = df.sample(frac=1)
    df = df.compute()
    df = dd.from_pandas(df,npartitions=10)
    weibo_path = os.path.join('data','weibos')
    df.to_csv(weibo_path,index=False)


if __name__ == "__main__":
    main()