# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import pymysql
import pymysql.cursors
# import docx
#import re
import datetime
#import toad
import warnings
warnings.filterwarnings("ignore")
import numpy as np

def sql_table(db = 'dompet_second',id_card_no='9271055712860003'):
    conn = pymysql.connect(
    host = '',
    user = '',
    password = '',
    db = db,
    charset = 'utf8')
    sql = f"""
       SELECT
        a.app_user_id,
        a.mobile,
        a.id_card_no,
        a.request_date,
        a.item_code,
        a.loan_order_status,
        CASE
        WHEN a.remaining_days < 0 
        AND a.repay_order_status = 0 THEN
        1 ELSE 0 
        END AS 'tar' 
        FROM
        t_loan_order a
        WHERE
        a.is_new = 1 
        AND a.request_date >= DATE_SUB(NOW(), INTERVAL 150 DAY)
        AND a.id_card_no='{id_card_no}'
        """
    df = pd.read_sql(sql,conn)
    conn.close()
    return df