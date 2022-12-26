import dask.dataframe as dd
import os
import spacy
import time
import multiprocessing as mp

if __name__ == "__main__":
    def return_time():
        return time.strftime('%H:%M', time.localtime())

    def segment_core(weibo):
        weibo_tokens = []
        for token in nlp(weibo):
            weibo_tokens.append(token.text)
        return " ".join(weibo_tokens)
    
    def segment_df(part_df):
        part, df = part_df
        df = df.compute()
        df['text'] = df.text.astype(str)
        df['text'] = df.text.apply(segment_core)
        df.to_csv(f"./data/segmented2.csv/{part}.part",index=False)
        return None

    print("Preprocessing started", return_time())
    we = dd.read_csv("./data/clean.csv/*.part", engine= "python")
    npart = we.npartitions

    nlp = spacy.load("zh_core_web_lg")
    os.makedirs("./data/segmented2.csv",exist_ok=True)

    print("Processing started",return_time())
    t1 = time.time()
    with mp.Pool(processes=7) as pool:
        for part,_ in enumerate(pool.imap_unordered(segment_df,enumerate(we.partitions))):
            t2 = time.time()
            print(f"{part+1}/{npart} finished.")
            print((t2-t1)/(part+1)*(npart-(part+1))/60/60,'hours left.')
    
    print("Finished",(time.time()-t1)/60/60,'hours.', return_time())
