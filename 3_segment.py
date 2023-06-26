import os
import subprocess
import multiprocessing as mp
import time
import tempfile
import pandas as pd

def core(file):
    with tempfile.NamedTemporaryFile(delete=True) as tf:
        df = pd.read_csv(file)
        df['text'] = df.text.str.replace("(\s|,)+"," ",regex = True)
        df.text.to_csv(tf,index=True,header=None)

        segmenter_path = os.path.join('stanford-segmenter','segment.sh')
        proc = subprocess.Popen([segmenter_path,"pku", str(tf.name), "UTF-8","0"],
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                            universal_newlines=True)

        lines = proc.communicate()
        ss = pd.Series(lines[0].split("\n")[1:])
        df['text'] = ss.str.replace("^[0-9]+ , "," ",regex=True)
        file = os.path.basename(file)
        csv_path = os.path.join('data','segmented',file)
        df.to_csv(csv_path, index = False)

def main():
    clean_path = os.path.join('data','clean')
    files = [os.path.join('data','clean',file) for file in os.listdir(clean_path)]
    filel = len(files)
    
    seg_path = os.path.join('data','segmented')
    os.makedirs(seg_path,exist_ok=True)
    
    t1 = time.time()
    with mp.Pool(processes=4) as pool:
        for index, _ in enumerate(pool.imap_unordered(core,files)):
            print(f'{(index+1)/filel*100:.2f}% Complete',end=": ")
            print(f'{(time.time()-t1)/(index+1)/60/60*(filel - index - 1):.2f} hours left',flush=True)

    print((time.time()-t1)/60)
    print('Finished')
    
if __name__ == "__main__":
    main()

