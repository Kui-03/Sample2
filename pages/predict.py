from pages.properties import *
from classes.predictor_models import Predictor
from classes.predictor_models import *
import json

import pandas as pd
import numpy as np
from sklearn.metrics import precision_score, recall_score, roc_auc_score, f1_score, matthews_corrcoef
from sklearn.metrics import make_scorer
import joblib as j

# Create Scorers
def recall_mcc_f1(y_true, y_pred):
        mcc = matthews_corrcoef(y_true, y_pred)
        recall = recall_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred)
        return np.array([mcc,recall,f1]).mean()
        
@app.route("/predict", methods=["POST"])
def predict():
    content = request.json
    values = content["values"]
    model = content["model"]
    method = content["method"]

    df = pd.read_json(values)
    print(df)
    if len(model) <= 1:
        model = "RF"
    if len(method) <= 1:
        method = "predict"

    # XGB, RF, SGD, LR
    p = Predictor(model)
    if method == "predict":
        result = p.predict(df)
    elif method == "predict_proba":
        result = p.predict_proba(df)

    return {"result": result.tolist()}
