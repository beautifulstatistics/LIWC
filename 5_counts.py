from collections import Counter
import numpy as np
import pandas as pd
import liwc
import time
import os
import multiprocessing as mp

from collections import defaultdict, OrderedDict

def return_time():
    return time.strftime('%H:%M', time.localtime())

if __name__ == '__main__':
    dictionary_path = os.path.join('dictionaries',
    'LIWC2015 Dictionary - Chinese (Simplified)(adjusted).dic')
    parse, category_names = liwc.load_token_parser(dictionary_path)

    linear_factor_path = os.path.join('dictionaries','restructured_factors.txt')
    with open(linear_factor_path,'r') as f:
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
    
    category_names += ['persconc','totallen','tokencount','notdict']
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
        if category in ['work',\
                        'leisure',\
                        'home',\
                        'money',\
                        'relig',\
                        'death']:
            return 'persconc'

    def count(weibo):
        weibo = str(weibo)
        counts = Counter()
        weibo_clean = weibo.replace(' ','').replace('\t','').replace('\n','')
        counts['totallen'] = len(weibo_clean)        
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
        df = pd.read_csv(file,engine='python')

        counts = df.text.apply(count)
        counts = pd.DataFrame.from_records(counts.values, columns=category_names)
        counts = counts.fillna(0).astype(np.int16)

        df = df.drop('text',axis=1)
        df = pd.concat([df,counts],axis=1)
        
        df = df.astype(np.uint8)
        file = os.path.basename(file)
        csv_path = os.path.join('data','counts',file)
        df.columns = [x.split('(')[0] for x in df.columns]
        df.to_csv(csv_path, index = False)

    count_path = os.path.join('data','counts')
    os.makedirs(count_path,exist_ok = True)
    segmented_path = os.path.join('data','segmented')
    files = [os.path.join(segmented_path,file) for file in os.listdir(segmented_path)]
    filel = len(files)

    print("Processing Started.", return_time())
    t1 = time.time()
    with mp.Pool(processes=6) as pool:
        for index, _ in enumerate(pool.imap_unordered(count_df,files)):
            print(f'{(index+1)/filel*100:.2f}% Complete',end=": ")
            print(f'{(time.time()-t1)/(index+1)/60/60*(filel - index - 1):.2f} hours left',flush=True)
    
    print("Finished.", return_time())
