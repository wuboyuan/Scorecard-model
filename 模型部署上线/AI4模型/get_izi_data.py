# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import pymysql
from get_app_data import *
import warnings

warnings.filterwarnings("ignore")


def izi_data_sql(db="dompet_second",loan_id=None,user_id="fc0185e95a354666bd995edfd8adfb00"):
    conn = pymysql.connect(
        host="",
 
        user="wuboyuan",
        password="",
        db=db,
        charset="utf8")

    sql = f"""
        SELECT  *  from master_risk.t_izidata_credit_feature_v2 where app_user_id='{user_id}' and loan_id='{loan_id}'and creditscore>0
           """

    df = pd.read_sql(sql, conn)
    df["creditscore"] = df["creditscore"].astype(float)
    conn.close()
    return df
# if __name__ == '__main__':
#     data=izi_data_sql()
#     print(data)
