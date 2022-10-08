#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-20 10:48
# Author     : wuboyuan
# File       : model_data_get.py
# Desc       : 此代码用于系统批量取数


import pandas as pd
import pymysql
from get_new_customer_feat import *
from get_old_customer_feat import *
from get_half_customer_feat import *
import tqdm

class GetModelData:
    def __init__(self, db=None,allplatfrom_user_type=None, star_date=None, end_date=None):
        self.db = db
        self.allplatfrom_user_type=allplatfrom_user_type
        self.star_date=star_date
        self.end_date = end_date


    def get_loan_id(self):
        conn = pymysql.connect(
            host="",
            port=,
            user="",
            password="",
            db=self.db,
            charset="utf8")
        sql = f"""
            select a.loan_id,
            a.request_date,
            a.repay_date,
            a.loan_order_status,
            a.item_code,
            a.allplatfrom_user_type,
            a.repay_order_status
            from (SELECT a.loan_id,
         a.request_date,
         b.repay_date,
         a.repay_order_status,
         a.allplatfrom_user_type,
         a.item_code,
         a.id_card_no,
         a.mobile,
         a.extend_times,
         a.loan_order_status,
         a.remaining_days,
         b.sell_accounts_date
        FROM
         t_loan_order a
         LEFT JOIN t_loan_order_operate b ON a.loan_id = b.loan_id
         WHERE 
         1=1
         UNION 
         SELECT 
         loan_id,
         request_date,
         repay_date,
          1 AS 'repay_order_status',
          CAST(JSON_EXTRACT(loan_order_json,'$.allplatfromUserType') AS SIGNED ) AS 'allplatfrom_user_type',
          item_code,
          id_card_no,
          mobile,
          CAST(JSON_EXTRACT(loan_order_json,'$.extendTimes') AS SIGNED ) AS 'extend_times',
          5 AS 'loan_order_status',
          postpone_day AS 'remaining_days',
          create_date AS 'sell_accounts_date'
        FROM t_loan_order_postpone_log) a
            where 1=1
            AND loan_order_status = 5
            AND extend_times =0
            #AND a.item_code != 'CAH'
            AND repay_date>='{self.star_date}' 
            AND repay_date<='{self.end_date}' 
            AND allplatfrom_user_type='{self.allplatfrom_user_type}'
                """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    def get_model_feat(self,merchant_code=None,id_star=None,id_end=None):
        data_loan=self.get_loan_id()
        data_loan.to_csv('C:/Users/Administrator/Desktop/test_data.csv')
        data_id = list(data_loan['loan_id'].values)
        if id_star:
            id_star=id_star
        else:
            id_star=0
        if id_end:
            id_end=id_end
        else:
            id_end=len(data_id)

        if self.allplatfrom_user_type == 1:
            ind=0
            for i in tqdm.tqdm(data_id[id_star:id_end]):
                try:
                    data = GetNewCustomerFeat(self.db,i).get_new_data_feat()
                    if ind==0:
                        data_new=data
                        ind=1
                    else:
                        data_new=data_new.append(data)
                except:
                    print(i)
                    continue
            return data_new
        elif self.allplatfrom_user_type == 3:
            ind = 0
            for i in tqdm.tqdm(data_id[id_star:id_end]):
                try:
                    data = GetOldCustomerFeat(self.db, i).get_old_data_feat(merchant_code=merchant_code)
                    if ind == 0:
                        data_old = data
                        ind = 1
                    else:
                        data_old = data_old.append(data)
                except:
                    print(i)
                    continue
        elif self.allplatfrom_user_type == 2:
            ind = 0
            for i in tqdm.tqdm(data_id[id_star:id_end]):
                try:
                    data = GetHalfCustomerFeat(self.db, i).get_half_data_feat(merchant_code=merchant_code)
                    if ind == 0:
                        data_half = data
                        ind = 1
                    else:
                        data_half = data_half.append(data)
                except:
                    print(i)
                    continue
            return data_half








if __name__ == '__main__':
    data = GetModelData('api_loansuper_x4',allplatfrom_user_type=1,star_date='2021-05-23 00:00:00',end_date='2021-08-23 23:59:59').get_model_feat(merchant_code='x4_system',id_star=0)
    #data.to_csv('C:/Users/Administrator/Desktop/test_data.csv')
    print(data)

    # BETWEEN DATE_SUB( DATE_FORMAT( NOW(), '%Y-%m-%d 00:00:00'),INTERVAL 5 DAY) AND
    #     DATE_SUB( DATE_FORMAT( NOW(), '%Y-%m-%d 00:00:00'),INTERVAL 5 DAY);
