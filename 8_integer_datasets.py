import os
import statsmodels.api as sm
import pandas as pd

from hypothesises import *

def consolidate(X, predictor_cols):
    X = X.loc[:,predictor_cols + ['censored','not_censored']]
    X = X.groupby(predictor_cols, as_index=False).sum()
    
    to_include = X.sum() != 0
    X = X.loc[:,to_include]
    
    X = sm.add_constant(X)
    return X.astype(float)

def main(hypothesis_cols):
    consolidated_path = os.path.join('data','consolidated_full.csv')
    X = pd.read_csv(consolidated_path)

    model_dir = os.path.join("data","model_datasets")
    os.makedirs(model_dir,exist_ok=True)
    for index, cols in enumerate(hypothesis_cols):
        X_sub = consolidate(X, cols)

        model_path = os.path.join("data",
        "model_datasets",f"{index}.data")
        
        X_sub.to_csv(model_path,index=False)

if __name__ == "__main__":
    main(HYPOTHESISES)