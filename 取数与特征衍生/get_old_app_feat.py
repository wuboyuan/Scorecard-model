#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-18 15:55
# Author     : wuboyuan
# File       : get_old_app_feat.py
# Desc       : 此代码用于老客app变化


import requests
import re
import joblib
from get_old_iziv4_feat import *


class GetOldAppFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def get_requests(self,url, encod=True):
        url = url
        headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; \
                           WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        if encod:
            response.encoding = 'utf-8'
        else:
            response.encoding = 'gbk'
        rs = response.text
        return rs

    def count_app_change(self):
        last_loan = GetOldIziV4feat(self.db,self.loan_id).get_last_loan()
        new_loan_id=self.loan_id
        old_loan_id=last_loan['loan_id_b'].iloc[0]
        app_new = GetDataAll(self.db,loan_id=new_loan_id).get_applist()
        app_new_json = self.get_requests(app_new['applist_json'].iloc[0])
        app_new_df = pd.DataFrame(eval(app_new_json))

        app_old = GetDataAll(self.db, loan_id=old_loan_id).get_applist()

        app_old_json = self.get_requests(app_old['applist_json'].iloc[0])

        app_old_df = pd.DataFrame(eval(app_old_json))
        if len(app_new_df) != 0 and len(app_old_df) != 0:
            minus = set(app_old_df['packageName'].unique()) - set(app_new_df['packageName'].unique())

            minus_data = app_old_df[app_old_df['packageName'].isin(minus)]

            minus_data['status_app'] = -1

            add = set(app_new_df['packageName'].unique()) - set(app_old_df['packageName'].unique())

            add_data = app_new_df[app_new_df['packageName'].isin(add)]

            add_data['status_app'] = 1
            diff_data = minus_data.append(add_data)
            diff_data['loan_id'] = new_loan_id
            diff_data['create_date'] = app_new['request_date'].iloc[0]
        return diff_data

    def count_app_fun(self):
        diff_test = self.count_app_change()
        data_app = {}
        category_dict = joblib.load('file/calss_category_dict0731.pkl')
        diff_test["category"] = diff_test['packageName'].map(category_dict)
        diff_test["category"] = diff_test["category"].fillna("others")
        diff_test1 = diff_test[diff_test['status_app'] == 1]
        diff_test2 = diff_test[diff_test['status_app'] == -1]
        data_app['AddWordsFinance'] = diff_test1['packageName'].apply(lambda x: 1 if re.search(
            r'dana|cash|kredit|pinjaman|pinjam|dompet|ksp|ojk|darurat|tunai|modal|finance|cepat|keuangan|uang|loan|saku|tagihan|lender|money|uangku|kami|darurat|emas|loan|kantong|duit|rupiah|uangme|ekpressuang|pinjaman|kredi|uang|dan',
            x, re.I) else 0).sum()
        data_app['MinusWordsFinance'] = diff_test2['packageName'].apply(lambda x: 1 if re.search(
            r'dana|cash|kredit|pinjaman|pinjam|dompet|ksp|ojk|darurat|tunai|modal|finance|cepat|keuangan|uang|loan|saku|tagihan|lender|money|uangku|kami|darurat|emas|loan|kantong|duit|rupiah|uangme|ekpressuang|pinjaman|kredi|uang|dan',
            x, re.I) else 0).sum()
        data_app['add_app'] = len(diff_test[diff_test['status_app'] == 1])
        data_app['minus_app'] = len(diff_test[diff_test['status_app'] == 1])
        category_dict_list = ['Finance', 'others', 'Game', 'Office', 'Shopping', 'Entertainment', 'Photography',
                              'Communications', 'Lifestyle', 'Education']
        for i in category_dict_list:
            data_app['Add' + i] = len(diff_test1[diff_test1["category"] == i])
            data_app['minus' + i] = len(diff_test2[diff_test2["category"] == i])
        data_app = pd.DataFrame(data_app, index=[diff_test['loan_id'].iloc[0]])
        return data_app

if __name__ == '__main__':
    data = GetOldAppFeat('api_loansuper_x4', 986222).count_app_fun()
    # data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    print(data)