import statsmodels.api as sm
import pandas as pd

input('start')

X = pd.read_csv("./data/consolidated_censored_not_censored.csv")
response_cols = ['censored','not_censored']
y = X.loc[:,response_cols]
X = X.loc[:,'image']
X = sm.add_constant(X)
X = X.astype(float)


input('check ram')

bi = sm.GLM(y, X, family=sm.families.Binomial())
bi = bi.fit()
print(bi.summary())
"""
========================================================================================
Dep. Variable:     ['censored', 'not_censored']   No. Observations:             83425219
Model:                                      GLM   Df Residuals:                 83425217
Model Family:                          Binomial   Df Model:                            1
Link Function:                            Logit   Scale:                          1.0000
Method:                                    IRLS   Log-Likelihood:            -2.0718e+06
Date:                          Sun, 11 Dec 2022   Deviance:                   3.8330e+06
Time:                                  13:19:18   Pearson chi2:                 9.15e+07
No. Iterations:                              18   Pseudo R-squ. (CS):             0.4453
Covariance Type:                      nonrobust                                         
==============================================================================
                 coef    std err          z      P>|z|      [0.025      0.975]
------------------------------------------------------------------------------
const         -5.9812      0.001  -4459.099      0.000      -5.984      -5.979
image         17.6578      0.144    122.331      0.000      17.375      17.941
==============================================================================
"""