import os
from functools import partial
import multiprocessing as mp
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import \
    variance_inflation_factor as vif

class vf(sm.families.varfuncs.VarianceFunction):
    def __call__(self, mu):
        return mu ** 2 * (1 - mu) ** 2

    def deriv(self, mu):
        return 2 * mu - 6 * mu ** 2 + 4 * mu ** 3

def vif_core(i, X):
    return X.columns[i], vif(X.values, i)

def make_vif(X):
    if X.shape[1] == 1:
        return "Only one factor. No VIF data."

    with mp.Pool(processes=7) as pool:
        vif_data = {}
        for col_name, vif_value in pool.imap_unordered(
            partial(vif_core,X=X),list(range(X.shape[1]))):

            vif_data[col_name] = vif_value
    
    vif_data = pd.Series(vif_data)
    vif_data.name = "VIFs"
    vif_data = vif_data.sort_values(ascending=False)
    return vif_data.to_string()

def make_save_report(model_name, vifs, summary):
    text = f"     {model_name}" \
           f"\n===================" \
           f"\n       VIF" \
           f"\n===================" \
           f"\n{vifs}" \
           f"\n{summary}"
    
    model_num = model_name.split(".")[0]
    report_file = os.path.join('reports', f'{model_num}.txt')

    with open(report_file,'w') as f:
        f.write(text)

def main():
    head = "./models"
    for model in os.listdir(head):
        model_num = str(model.split('.')[0])
        file = os.path.join(head,model)
        bi = sm.load(file)

        make_save_report(model_name = model,
                         vifs = make_vif(bi.X),
                         summary = bi.summary().as_text())
        
        actual = bi.y.censored/(bi.y.censored + bi.y.not_censored)

        plt.scatter(actual,bi.predict())
        plt.title(f"Predicted vs Actual {model}")

        red_actual = os.path.join("reports",f"Pred_actual_{model_num}")
        plt.xlabel("Actual")
        plt.ylabel("Predicted")
        plt.savefig(red_actual)

        plt.plot(bi.predict(linear=True), bi.resid_pearson)
        plt.title(model)
        plt.xlabel("Linear Predictor")
        plt.ylabel("Pearson Residual")

        resids_file = os.path.join('reports', 
                                   f"Pearson_residual_{model_num}")
        plt.savefig(resids_file)

if __name__ == "__main__":
    main()