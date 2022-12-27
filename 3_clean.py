import os
import time
import signal
import multiprocessing as mp

import re
import numpy as np

import pandas as pd
import dask.dataframe as dd
import preprocessor as tpp

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
    return text

def clean_df(df):
    part, df = df
    df = pd.DataFrame(df,columns=['image','text','permission_denied'])
    
    df['text'] = df.text.astype(str)
    df['text'] = df.text.apply(clean,args=(part,))
    
    part_path = os.path.join('data','clean',f'{part}.part')
    df.to_csv(part_path,index=False)
    return part

def main():
    print("Preprocessing started.",time.strftime('%H:%M', time.localtime()))

    os.makedirs(os.path.join('data','clean'),exist_ok=True)

    part_path = os.path.join('data','weibos','*.part')
    we = dd.read_csv(part_path,engine='python')
    we = we.compute()

    nparts = int(np.sqrt(we.shape[0]))
    print("This many total parts",nparts)

    we = np.array_split(we.values,nparts)
    already_processed = [int(x.split(".")[0]) for x in os.listdir(os.path.join('data','clean'))]
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