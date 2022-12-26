import pandas as pd
import statsmodels as sm
import numpy as np

X = pd.read_csv("./data/consolidated_sum_size.csv",nrows=2000)

sf = np.hstack([X['sum'].values.reshape(-1,1),(X['size'] - X['sum']).values.reshape(-1,1)])
X = X.drop(['sum','size'],axis=1)
X = sm.add_constant(X)

model = sm.GLM(X, sf, family=sm.families.Binomial())