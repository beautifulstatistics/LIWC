import os
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd

class vf(sm.families.varfuncs.VarianceFunction):
    def __call__(self, mu):
        return mu ** 2 * (1 - mu) ** 2

    def deriv(self, mu):
        return 2 * mu - 6 * mu ** 2 + 4 * mu ** 3

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

    X = pd.read_csv("./data/consolidated_censored_not_censored.csv",
                usecols=predictor_cols + response_cols)

    for index, cols in enumerate(hypothesis_cols):
        X_sub = consolidate(X, cols, response_cols)
        
        X_sub.to_csv("test.csv",index=False)

        full_factorial = "*".join(cols)
        formula = f"censored + not_censored ~ {full_factorial}"
        family = sm.families.Binomial()
        family.variance = vf()

        bi = smf.glm(formula=formula, data=X_sub, family=family)
        bi = bi.fit(scale='X2')
        
        bi.y = X_sub.loc[:,response_cols]
        bi.X = X_sub.loc[:,predictor_cols]

        model_path = os.path.join("models",f"{index}.model")
        bi.save(model_path)

if __name__ == "__main__":
    hypothesis_cols = [['relativ',
                        'affect',
                        'ppron',
                        'drives']]


    main(hypothesis_cols)