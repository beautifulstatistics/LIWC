import pandas as pd
import statsmodels.discrete.count_model as reg_models
import statsmodels.api as sm

from hypothesises import *

df = pd.read_csv('./data/consolidated.csv')

df = sm.add_constant(df)

out = reg_models.ZeroInflatedPoisson(df.censored, df.loc[:,['const'] + HYPOTHESISES[2]])

fit_regularized = out.fit_regularized(maxiter = 100)
print(fit_regularized.summary())