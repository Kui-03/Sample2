DATA = "db/components/"

import pandas as pd
import numpy as np
import os 
import subprocess as sp
from io import StringIO as SIO

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