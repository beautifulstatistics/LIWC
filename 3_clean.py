import pandas as pd
import dask.dataframe as dd
import preprocessor as tpp
import time
import os
import shutil
import numpy as np

import multiprocessing as mp
import re

import signal

class TimeoutError(Exception):
    pass

def handler(signum, frame):
    raise TimeoutError()

def timeout(func, text):
    signal.signal(signal.SIGALRM, handler) 
    signal.alarm(20)
    try:
        result = func(text), False
    except TimeoutError as exc:
        result = '', True
    finally:
        signal.alarm(0)
    return result

def clean(text, part):
    text, timeouted = timeout(tpp.clean, text)
    text = re.sub("(//：|~|：+)","",text)
    if timeouted:
        with open(f"./logs/3_clean.py.error/{part}.timeout", 'a') as f:
            print(0,file=f,end='')
    return text

def clean_df(df):
    part, df = df
    df = pd.DataFrame(df,columns=['image','text','permission_denied'])
    df['text'] = df.text.astype(str)

    df['text'] = df.text.apply(clean,args=(part,))
    df.to_csv(f"./data/clean.csv/{part}.part",index=False)
    return part

def main():
    print("Preprocessing started.",time.strftime('%H:%M', time.localtime()))

    os.makedirs("./data/clean.csv",exist_ok=True)
    shutil.rmtree(f"./logs/3_clean.py.error/",ignore_errors=True)
    os.makedirs("./logs/3_clean.py.error",exist_ok=True)

    we = dd.read_csv(f"./data/weibos.csv/*.part",engine='python')
    we = we.compute()
    nparts = int(np.sqrt(we.shape[0]))
    print("This many total parts",nparts)

    we = np.array_split(we.values,nparts)
    already_processed = [int(x.split(".")[0]) for x in os.listdir("./data/clean.csv/")]
    we = [(i,part) for i, part in enumerate(we) if i not in already_processed]
    print(len(we),we[0][0])
    print("Processing Started.", time.strftime('%H:%M', time.localtime()))

    t1 = time.time()
    with mp.Pool(processes=7) as pool:
        for part in pool.imap_unordered(clean_df,we):
            t2 = time.time()
            print(f"{part+1}/{nparts} finished.")
            print((t2-t1)/(part+1)*(nparts-(part+1))/60/60,'hours left.')
    
    print((time.time()-t1)/60/60,"hours")

if __name__ == "__main__":
    main()