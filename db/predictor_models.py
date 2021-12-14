# ============================================================== #
# Class Predictor
# Rename the GridSearchCV Classifier in DATA_clf directory to:
#   - [key]_clf_optimal.mdl
# -------------------------------------------------------------- #
# Directory
DATA_train_test = "db/train_test/"
DATA_clf = "db/clf_models/"

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV

from sklearn.metrics import precision_score, recall_score, roc_auc_score, f1_score, matthews_corrcoef
from sklearn.metrics import make_scorer
import pandas as pd
import joblib as j

# from rest_framework.parsers import JSONParser
# from django.http.response import JsonResponse

# Create Scorers
def recall_mcc_f1(y_true, y_pred):
        mcc = matthews_corrcoef(y_true, y_pred)
        recall = recall_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred)
        return np.array([mcc,recall,f1]).mean()

# Class Predictor
class Predictor():
    features = []
    model = None
    key=""

    # Initialize
    def __init__(self, key=""):
        if key=="": return
        self.key = key
        self.features = j.load(DATA_train_test+"X_train").columns
        self.model = self.get_model()       

    # Get Best Model
    def get_model(self):
        get = j.load(DATA_clf +"{0}_clf_optimal.mdl".format(self.key))
        model = get.best_estimator_
        
        # Assign features
        if isinstance(model, XGBClassifier):
            # print(model)
            # print(model.get_booster().feature_names)
            pass
        else:
            model.feature_names=self.features
            # print(model)
            # print(model.feature_names)

        return model
    
    # Create predict function, with columns arranged
    # Test: PASSED
    def predict(self, X):
        frames = X[self.features]
        return self.model.predict(frames)

    def predict_log_proba(self, X):
        if isinstance(self.model, XGBClassifier): return
        frames = X[self.features]
        return self.model.predict_log_proba(frames)

    def predict_proba(self, X):
        frames = X[self.features]
        return self.model.predict_proba(frames)

    def score(self, X, y):
        frames = X[self.features]
        return self.model.score(frames, y)
