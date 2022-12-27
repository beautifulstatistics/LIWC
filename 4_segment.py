import os
import subprocess
import multiprocessing as mp
import time
import tempfile
import pandas as pd

def core(file):
    with tempfile.NamedTemporaryFile(delete=True) as tf:
        df = pd.read_csv(f"./data/clean/{file}")
        df['text'] = df.text.str.replace("(\s|,)+"," ",regex=True)
        df.text.to_csv(tf,index=True,header=None)

        proc = subprocess.Popen(["./stanford-segmenter/segment.sh","pku", str(tf.name), "UTF-8","0"],
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                            universal_newlines=True)

        lines = proc.communicate()
        ss = pd.Series(lines[0].split("\n")[1:])
        ss = ss.str.replace("^[0-9]+ , "," ",regex=True)
        df['text'] = ss
        df.to_csv(f"./data/segmented/{file}")
        return int(file.split(".")[0])

files = os.listdir("./data/clean")
files.sort(key=lambda x: int(x.split(".")[0]))
filel = len(files)
os.makedirs("./data/segmented",exist_ok=True)

t1 = time.time()
with mp.Pool(processes=7) as pool:
    for part in pool.imap_unordered(core,files):
        print((part+1)/filel,end=": ")
        print((time.time()-t1)/(part+1)/60/60*(filel - part-1),flush=True)

print((time.time()-t1)/60)
