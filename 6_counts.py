from collections import Counter
import numpy as np
import pandas as pd
import liwc
import time
import os
import multiprocessing as mp

from collections import defaultdict, OrderedDict

from functools import partial
print = partial(print,flush=True)

def return_time():
    return time.strftime('%H:%M', time.localtime())

if __name__ == '__main__':
    print("Prepreprocessing started.",return_time())
    parse, category_names = liwc.load_token_parser(
        './data/dictionaries/LIWC2015 Dictionary - Chinese (Simplified)(adjusted).dic')

    with open("./data/dictionaries/linear_factors.txt",'r') as f:
        lf = defaultdict(lambda:[])
        for line in f:
            line = line.split("/")
            line = [x.strip()+str(i+1) for i,x in enumerate(line)]
            for i, lineh in enumerate(line):
                if i < len(line)-1:
                    lf[lineh] += [line[i+1]]
            lfo = OrderedDict()
            lfko = list(lf.keys())
            lfko.sort(key=lambda x: int(x[-1]),reverse=True)
            for entry in lfko:
                lfl = list(set(lf[entry]))
                lfl.sort(key=lambda x: int(x[-1]),reverse=True)
                lfo[entry] = lfl
    
    category_names += ['persconc(PersonalConcerns)','totallen','tokencount','notdict']
    category_names += ['o' + x[:-1] for x in lfo.keys()]

    def enforce_nesting(counts):
        for key in lfo.keys():
            category = key[:-1]
            sub_count = 0
            for sub_key in lfo[key]:
                sub_category = sub_key[:-1]
                if sub_category in counts:
                    sub_count += counts[sub_category]
            diff = counts[category] - sub_count
            if diff > 0:
                counts['o' + category] = diff
            elif diff < 0:
                counts[category] = sub_count
        return counts

    def fill_missing_category(category):
        if category in ['work(Work)',\
                        'leisure(Leisure)',\
                        'home(Home)',\
                        'money(Money)',\
                        'relig(Religion)',\
                        'death(Death)']:
            return 'persconc(PersonalConcerns)'
        return None

    def count(weibo):
        weibo = str(weibo)
        counts = Counter()
        counts['totallen'] = len(weibo)
        for token in weibo.split():
            counts.update(['tokencount'])
            for category in parse(token):
                counts.update([category])
                if missing := fill_missing_category(category):
                    counts.update([missing])
            else:
                counts.update(['notdict'])
        
        counts = enforce_nesting(counts)
        return counts
    
    def count_df(file):
        part = int(file.split(".")[0])
        file = os.path.join("./data/segmented.csv",file) 
        df = pd.read_csv(file,engine='python')

        counts = df.text.apply(count)
        counts = pd.DataFrame.from_records(counts.values,columns=category_names)
        counts = counts.fillna(0).astype(np.int16)

        df = df.drop('text',axis=1)
        df = pd.concat([df,counts],axis=1)
        df.to_csv(f"./data/counts.csv/{part}.part",index=False)
        return None

    os.makedirs("./data/counts.csv",exist_ok=True)
    files = os.listdir("./data/segmented2.csv")
    nparts = len(files)

    print("Processing Started.", return_time())
    t1 = time.time()
    with mp.Pool(processes=6) as pool:
        for i, _ in enumerate(pool.imap_unordered(count_df,files)):
            t2 = time.time()
            if i % int(nparts/50) == 0:
                print(f"{i+1}/{nparts} finished. ", end='')
                print((t2-t1)/(i+1)*(nparts-(i+1))/60,'mins left.')
    
    print("Finished.", return_time())
