import os
import subprocess
import multiprocessing as mp
import time
import tempfile
import pandas as pd

def core(file):
    with tempfile.NamedTemporaryFile(delete=True) as tf:
        file_path = os.path.join('data','clean',file)
        df = pd.read_csv(file_path)
        df['text'] = df.text.str.replace("(\s|,)+"," ",regex=True)
        df.text.to_csv(tf,index=True,header=None)

        segmenter_path = os.path.join('stanford-segmenter','segment.sh')
        proc = subprocess.Popen([segmenter_path,"pku", str(tf.name), "UTF-8","0"],
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                            universal_newlines=True)

        lines = proc.communicate()
        ss = pd.Series(lines[0].split("\n")[1:])
        ss = ss.str.replace("^[0-9]+ , "," ",regex=True)
        df['text'] = ss
        csv_path = os.path.join('data','segmented',str(file))
        df.to_csv(csv_path)
        return int(file.split(".")[0])

clean_path = os.path.join('data','clean')
files = os.listdir(clean_path)
files.sort(key=lambda x: int(x.split(".")[0]))
filel = len(files)
seg_path = os.path.join('data','segmented')
os.makedirs(seg_path,exist_ok=True)

t1 = time.time()
with mp.Pool(processes=7) as pool:
    for part in pool.imap_unordered(core,files):
        print((part+1)/filel,end=": ")
        print((time.time()-t1)/(part+1)/60/60*(filel - part-1),flush=True)

print((time.time()-t1)/60)
