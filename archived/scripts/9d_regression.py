import statsmodels.api as sm
import dask.dataframe as dd
import dask

if __name__ == '__main__':

    dask.config.set(scheduler='processes',num_workers=7)
    response_cols = ['permission_denied']
    predictor_cols = ['image']

    X = dd.read_csv("./data/counts.csv/*.part",engine='python')
    X.columns = [x.replace("(","").replace(")","") for x in X.columns]
    X = X.loc[:,response_cols + predictor_cols]

    X = X.compute()
    print('compute done')

    y = X.loc[:,response_cols]
    X = X.loc[:,predictor_cols]
    X = sm.add_constant(X)
    X = X.astype(float)

    print("start fit")

    bi = sm.Logit(y,X)
    bi = bi.fit()
    print(bi.summary())

"""
    ==============================================================================
Dep. Variable:      permission_denied   No. Observations:            226810201
Model:                          Logit   Df Residuals:                226810199
Method:                           MLE   Df Model:                            1
Date:                Mon, 12 Dec 2022   Pseudo R-squ.:                  0.8633
Time:                        17:54:14   Log-Likelihood:            -3.8913e+06
converged:                       True   LL-Null:                   -2.8471e+07
Covariance Type:            nonrobust   LLR p-value:                     0.000
==============================================================================
                 coef    std err          z      P>|z|      [0.025      0.975]
------------------------------------------------------------------------------
const         -5.9812      0.001  -4459.099      0.000      -5.984      -5.979
image         17.6578      0.144    122.331      0.000      17.375      17.941
==============================================================================
    """