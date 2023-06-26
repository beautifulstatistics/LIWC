import os
import time
import numpy as np
import pandas as pd

def data_generator(files):    
    for file in files:
        X = pd.read_csv(file, engine='python', dtype=np.uint32)
        
        xcols = list(X.columns)
        
        remove = ['tokencount','totallen','not_censored','censored']
        
        for name in remove:
            xcols.remove(name)
        
        X[xcols] = X[xcols].apply(lambda x: x > 0).astype(np.uint8)
        
        xcols += ['tokencount']
        
        X = X.groupby(xcols).agg({'censored': 'sum', 'not_censored': 'sum'})
        X = X.reset_index()
        yield X

def main():
    path = os.path.join('data','consolidated')
    files = [os.path.join(path,file) for file in os.listdir(path)]
    filel = len(files)
    
    path = os.path.join('data','presence_data.csv')
    first_one = True
    t1 = time.time()
    for index, chunk in enumerate(data_generator(files)):
        if first_one:
            chunk.to_csv(path, mode='w', index = False)
            first_one = False
        else:
            chunk.to_csv(path, mode='a', header=False, index = False)
        
        print(f'{(index+1)/filel*100:.2f}% Complete',end=": ")
        print(f'{(time.time()-t1)/(index+1)/60/60*(filel - index - 1):.2f} hours left',flush=True)

    
if __name__ == "__main__":
    main()