import os
import time
import numpy as np
import multiprocessing as mp

import pandas as pd

def process(file):       
    df = pd.read_csv(file, dtype = np.uint32)
    
    cols = list(df.columns)
    cols.remove('permission_denied')

    X = df.groupby(cols).permission_denied.agg(['sum', 'size'])
    X = X.reset_index().copy()
    
    X['size'] = X['size'] - X['sum']
    X.rename({'sum':'censored','size':'not_censored'}, inplace = True, axis = 1)
    
    X[cols] = X[cols].astype(np.uint8)
    X[['censored','not_censored']] = X[['censored','not_censored']].astype(np.uint32)
    
    file = os.path.basename(file)
    consolidated_path = os.path.join('data', 'consolidated', file)
    X.to_csv(consolidated_path, index = False)

def main():
    clean_path = os.path.join('data','counts')
    files = [os.path.join(clean_path,file) for file in os.listdir(clean_path)]
    filel = len(files)
    
    consolidate_path = os.path.join('data','consolidated')
    os.makedirs(consolidate_path, exist_ok = True)
    
    t1 = time.time()
    with mp.Pool(processes=5) as pool:
        for index, _ in enumerate(pool.imap_unordered(process, files)):
            print(f'{(index+1)/filel*100:.2f}% Complete',end=": ")
            print(f'{(time.time()-t1)/(index+1)/60/60*(filel - index - 1):.2f} hours left',flush=True)
        
    print((time.time()-t1)/60/60)
    print('Finished')


if __name__ == "__main__":
    main()