#!/usr/bin/env py39
import pandas as pd
import numpy as np
from glob import glob
import joblib

# Directories
IN = "df/"
MODELS = "db/transformers/"
COLS = ['city', 'job', 'category', 'merchant', 'age_category', 'dur', 'dist',
'amt', 'proxim_ave', 'rate_ave']

class Transformer():
    excl = []

    def __init__(self, cols=None, excl=[]):
        if cols == None:
            cols = COLS
        self.models = self.read_models(cols)
        self.excl = excl

    # Read models
    def read_models(self, cols):
        # Models hash
        dls = {}
        # Read existing models for columns
        for col in cols:
            get = MODELS+col+".mdl"
            models = sorted(glob(get))
            if len(models) > 0:
                model = models[0]
                if model in self.excl: continue # Block EXCL models
                dls[col] = joblib.load(model)
        return dls

