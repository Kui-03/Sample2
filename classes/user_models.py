# ============================================================== #
# Class User
# -------------------------------------------------------------- #



from os import name
import pandas as pd
import numpy as np
from classes.transaction_models import Transaction
from datetime import datetime
from geopy.distance import geodesic
from classes.transformer_models import Transformer
import json

class User():
    # User profile
    ssn             = ""
    name            = ""
    dob             = ""

    gender          = 0
    city            = 0
    job             = 0
    age_category    = 0

    lat             = 0.0
    long            = 0.0
    proxim_ave      = 0.0
    rate_ave        = 0.0

    # Transactions
    last_transac = ""
    new_transacs = []
    fraud_transacs = []

    # Merchant Profile
    merchant        = 0
    proxim          = 0.0

    # Transaction info
    category        = 0
    dur             = 0.0
    dist            = 0.0
    amt             = 0.0

    # Initialize
    def __init__(self, ssn):
        self.ssn = ssn
        pass

    # Add to new transactions
    def add_transac(self, transac):
        self.new_transacs.append(transac)

    # Update other params
    def update(self):
        if len(self.new_transacs) < 1:
            return print("No new transactions yet")

        b4 = self.last_transac
        afr = self.new_transacs[0]
        
        if b4 == afr:
            return print("This is the latest transaction")
        
        # Duration (sec)
        dur = pd.Series(afr.time).astype("datetime64[ns]") - pd.Series(b4.time).astype("datetime64[ns]")
        dur = dur.squeeze().seconds
        # Distance (km)
        x1 = [b4.merch_lat, b4.merch_long]
        x2 = [afr.merch_lat, afr.merch_long]
        dist = geodesic(x2, x1).km
        # Rate (km/hr)
        rate = dist / ((dur/60)/60)
        # Proxim (km)
        usr = [self.lat, self.long]
        proxim = geodesic(x2, usr).km

        # ASsign - the avg of these variabes i.e. (rate_ave) are unmoving to mimic the user's
        #   behaviour profile. The weights of new transactions are not added to these metrics.
        self.dur = dur
        self.dist = dist
        self.proxim = proxim
        self.rate = rate
        # Update last transaction to latest!
        self.last_transac = afr
        # Clean
        del proxim, dist, dur, rate, usr, b4, afr, x1, x2

    # Format params for prediction as JSON
    def format(self):
        # Create transformer: kee-ka-ka-ku!
        t = Transformer()
        # Update values
        self.update()
        last = self.last_transac
        # Convert params
        p = {}
        p["city"] = [self.city]
        p["job"] = [self.job]
        p["age_category"] = [self.age_category]
        p["dur"] = [self.dur]
        p["dist"] = [self.dist]
        p["proxim_ave"] = [self.proxim_ave]
        p["rate_ave"] = [self.rate_ave]

        p["category"] = [last.category]
        p["merchant"] = [last.merchant]
        p["amt"] = [last.amt]
        
        p = pd.DataFrame(p)
        if p.isna().sum().sum() > 0:
            raise ValueError("Cannot convert NaN values")


        excl=["age_category", "proxim_ave", "rate_ave"]
        cat = ["city", "job", "category", "merchant"]
        for col in p.columns:
            if col in excl: continue
            # Categorical
            if col in cat:
                val = p[col]
            # Numerical
            else:
                val = p[[col]]
            # Transform
            p[col] = t.models[col].transform(val)

        del t
        # print(p)
        return p.to_json()

    # Update profile
    # pd.read_json(df.to_json())

