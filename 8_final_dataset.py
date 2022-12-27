import os
import statsmodels.api as sm
import pandas as pd

def consolidate(X, predictor_cols, response_cols):
    X = X.loc[:,predictor_cols + response_cols]
    X = X.groupby(predictor_cols, as_index=False).sum()
    
    to_include = X.sum() != 0
    X = X.loc[:,to_include]
    
    X = sm.add_constant(X)
    return X.astype(float)

def main(hypothesis_cols):
    predictor_cols = [item for sublist in hypothesis_cols for item in sublist]
    response_cols = ['censored','not_censored']

    consolidated_path = os.path.join('data','consolidated_full.csv')
    X = pd.read_csv(consolidated_path,
                usecols=predictor_cols + response_cols)

    for index, cols in enumerate(hypothesis_cols):
        X_sub = consolidate(X, cols, response_cols)

        model_path = os.path.join("data",
        "ready_for_models",f"{index}.model")
        
        X_sub.to_csv(model_path,index=False)

if __name__ == "__main__":
    hypothesis_cols = [['affect',
                        'social',
                        'cogproc',
                        'percept',
                        'drives',
                        'relativ',
                        'ppron',
                        'totallen',]]


    main(hypothesis_cols)