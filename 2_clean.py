import os
import time
import signal
import multiprocessing as mp

import re
import pandas as pd
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

def clean(text):    
    text, _ = timeout(tpp.clean, text)
    text = re.sub("(//：|~|：+|\.+|\（via\）).+|【.+】", "", text)
    return text

def clean_df(part_path):
    df = pd.read_csv(part_path, encoding='utf-8',dtype=str,engine='python',
                    on_bad_lines='skip',**{'encoding_errors':'replace'})
    
    df['permission_denied'] = ~df.permission_denied.isna()
    df['image'] = (df.image == '1') + 0
    
    df = df.drop(['mid','retweeted_status_mid','uid','geo','created_at','retweeted_uid','source','deleted_last_seen'],axis=1)
    df = df.loc[~df.text.isna(),:]

    df['text'] = df.text.astype(str)
    df['text'] = df.text.apply(clean)

    clean_path = part_path.replace('weeks','clean')
    df.to_csv(clean_path, index = False)

def main():
    print("Processing Started.", time.strftime('%H:%M', time.localtime()))
    
    os.makedirs(os.path.join('data','clean'), exist_ok=True)
    
    weeks_dir = os.path.join('data','weeks')
    part_paths = [os.path.join(weeks_dir,file) for file in os.listdir(weeks_dir)]
    filel = len(part_paths)

    t1 = time.time()
    with mp.Pool(processes=6) as pool:
        for index, _ in enumerate(pool.imap_unordered(clean_df, part_paths)):           
            print(f'{(index+1)/filel*100:.2f}% Complete',end=": ")
            print(f'{(time.time()-t1)/(index+1)/60/60*(filel - index - 1):.2f} hours left',flush=True)

    print((time.time()-t1)/60/60,"hours")

if __name__ == "__main__":
    main()
