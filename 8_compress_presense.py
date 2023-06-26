import numpy as np
import pandas as pd
from scipy.special import gammaln, betaln

from square.experimental.decisiontreegm import DecisionTreeGM

X = pd.read_csv('data/presence_data.csv',nrows=1)

xcols = list(X.columns)
xcols.remove('censored')
xcols.remove('not_censored')

dtypes = {name:np.uint8 for name in xcols}
dtypes.update({'censored':np.uint32,'not_censored':np.uint32})

X = pd.read_csv('data/presence_data.csv',dtype=dtypes)
X = X.loc[:,X.sum() != 0]

y = X.loc[:,['censored','not_censored']].values
X = X.drop(['censored','not_censored'], axis = 1).values

jefferys_beta = betaln(.5, .5)

def model_evidence(point):
    success, failure = point
    N = success + failure
    log_evidence = gammaln(N + 1) - gammaln(success + 1) - gammaln(failure + 1) \
                   + betaln(success + .5, failure + .5) - jefferys_beta
    return log_evidence

def point_estimator(y):
    return [sum(y[:, 0]), sum(y[:,1])]

def test_statistic(left_y, right_y):
    together = np.vstack((left_y,right_y))
    left_model = model_evidence(point_estimator(left_y))
    right_model = model_evidence(point_estimator(right_y))
        
    log_split_like = - left_model - right_model

    return {'statistic': log_split_like, 'aux': together}

def stopping_test(aux, depth, ncols):
    main_model = model_evidence(point_estimator(aux['aux']))
    log_bf = main_model + aux['statistic']
    if log_bf >= 0:
        return False
    return True

dtg = DecisionTreeGM(point_estimator, test_statistic, stopping_test)   
dtg.fit(X, y)

compressed = dtg.get_dataframe(dtg, xcols, integers = True)
compressed.to_csv("data/ANDpartition.csv", index = False)