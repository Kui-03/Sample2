

DATA = "db/components/"

from classes.merchant_models import Merchant
import pandas as pd
import numpy as np
from services.GETdata import *

class MerchantService():
    merchant = None

    def __init__(self):
        pass

    def getMerchantList(self):
        p = pd.read_csv("merchant_cat.csv")
        return p

    def getMerchant(self, name=""):
        df = self.GETdata(name, "merchant_cat.csv")
        merchant = Merchant()
        merchant.name = df.merchant
        merchant.category = df.categories
        
        df = self.GETdata(name, "merchant_coords.csv")
        merchant.merch_lat = df.merch_lat
        merchant.merch_long = df.merch_long

        self.merchant = merchant
        return merchant
