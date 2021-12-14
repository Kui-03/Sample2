# Class UserService


DATA = "db/components/"
SPARK = False

if SPARK:
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.sql import functions as F

import joblib as j
import pandas as pd
import numpy as np

import os, resource
import subprocess as sp
from io import StringIO as SIO

from classes.user_models import User
from classes.transaction_models import Transaction
from services.transaction_service import TransactionService

class UserService():
    spark = None
    user = None
    # resource.RLIMIT_VMEM = 500
    # soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    # resource.setrlimit(resource.RLIMIT_AS, (500, hard))

    def __init__(self):
        if SPARK == True:
            self.spark = SparkSession.builder.appName("UserService").getOrCreate()

    # Load user profile
    def load_objects(self, User):
        user = User
        user = self.load_userdat(user)
        user = self.load_ave(user)
        user = self.load_coords(user)
        user = self.load_transac(user, "last")
        return user

    # Load ave data
    def load_ave(self, User):
        cwd = os.getcwd()+"/"
        csv = "user_avg.csv"
        file = cwd+DATA+csv

        if SPARK == True:
            df_avg = self.spark.read.csv(file, header=True, inferSchema=True)
            user_avg = df_avg.filter(F.col("ssn") == User.ssn).toPandas()
            
            User.proxim_ave      = user_avg.proxim_ave.squeeze()
            User.rate_ave        = user_avg.rate_ave.squeeze()
           
            # Clear Memory
            del df_avg, user_avg
        else:
            df = self.GETdata(User.ssn, csv)
            # assign
            User.proxim_ave      = df.proxim_ave.squeeze()
            User.rate_ave        = df.rate_ave.squeeze()
            # clear
            del df
        # return user
        return User
    
    # Load user data
    def load_userdat(self, User):
        df = self.GETdata(User.ssn, "userdat.csv")
        #return df
        User.name = df.name.squeeze()
        User.gender = df.gender.squeeze()
        User.job = df.job.squeeze()
        User.city = df.city.squeeze()

        User.dob = df.dob.squeeze()
        del df
        
        return User
    
    # Load user coords
    def load_coords(self, User):
        df = self.GETdata(User.city, "city_coords.csv")
        User.lat = df.lat.squeeze()
        User.long = df.long.squeeze()
        return User


    # Load transactions
    #   last: last non-fraud    fraud: all frauds    nfraud: all non-frauds
    def load_transac(self, User, mode="last"):
        if mode == "last":
            # ssn,trans_num,merchant,amt,is_fraud,time
            df = self.GETdata(User.ssn, "transac_nfraud.csv", mode = "last")

            # Create Transaction data
            transac = Transaction()
            transac.amt = df.amt.squeeze()
            transac.merchant = df.merchant.squeeze()
            transac.time = df.time.squeeze()
            transac.is_fraud = df.is_fraud.squeeze()

            # merchant,categories
            df = self.GETdata(transac.merchant, "merchant_cat.csv")
            transac.category = df.categories.squeeze()
            
            # merchant,merch_lat,merch_long
            df = self.GETdata(transac.merchant, "merchant_coords.csv")
            transac.merch_lat = df.merch_lat.squeeze()
            transac.merch_long = df.merch_long.squeeze()
            # Write to User
            User.last_transac = transac

        elif mode == "fraud":
            df = self.GETdata(User.ssn, "transac_fraud.csv")
            User.fraud_transacs = df.trans_num.tolist()
        elif mode == "nfraud":
            df = self.GETdata(User.ssn, "transac_nfraud.csv")
            User.transacs = df.trans_num.tolist()
        elif mode == "all":
            df = self.GETdata(User.ssn, "transac_nfraud.csv", mode = "last")
            User.last_transac = df.trans_num.squeeze()
            df = self.GETdata(User.ssn, "transac_fraud.csv")
            User.fraud_transacs = df.trans_num.tolist()
            df = self.GETdata(User.ssn, "transac_nfraud.csv")
            User.transacs = df.trans_num.tolist()
        
        del df
        return User



    # Getdata from CSV
    def GETdata(self, grep = "", csv="", mode="single"):
        if (csv == "") or (grep == ""):
            raise ValueError("No csv input filename!")
        # csv dir
        cwd = os.getcwd()+"/"
        file = cwd+DATA+csv

        # get header
        cmd = "head -n 1 {0}".format(file)
        head = sp.check_output(cmd, shell=True, text=True)

        # get values
        if mode == "single":
            cmd = "grep '{0}' {1} ".format(grep, file)
        elif mode == "last":
            cmd = "grep '{0}' {1} | tail -n 1 ".format(grep, file)

        res = head+sp.check_output(cmd, shell=True, text=True)
        # convert to pandas df
        dat = SIO(res)
        df = pd.read_csv(dat)
        # clear
        del head, res, dat
        return df

    # Retrieve User and Data
    def getUser(self, ssn):
        user = User(ssn)
        user = self.load_objects(user)
        self.user = user
        return user

    def setUser(self, User):
        self.user = User


    def listUsers(self, cols=["name", "gender", "ssn"]):
        users = pd.read_csv(DATA+"userdat.csv")
        return users[cols]
        pass


# cmd = "cat {0} | grep {1} | tail -n 1".format(file, User.ssn)