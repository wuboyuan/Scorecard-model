#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-17 19:43
# Author     : wuboyuan
# File       : add_connect_feat.py
# Desc       : 此代码用于 通讯录字段


import re
import joblib
from get_data_all import *
import requests


class AddConnectFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def add_phone_feat(self):
        company_phone = joblib.load('file/company_phone.pkl')
        person_phone = joblib.load('file/person_phone.pkl')
        data_phone =GetDataAll(db=self.db, loan_id=self.loan_id).get_connections().iloc[-1:]
        data_phone_1 = pd.DataFrame(eval(data_phone['connect_json'].iloc[0]))
        data_phone_1['loan_id'] = self.loan_id
        data_phone_1['phone'] = data_phone_1['phone'].apply(lambda x: x.replace("-", "").replace(" ", ""))
        data_phone_1['phone_1'] = data_phone_1['phone'].apply(
            lambda x: 1 if re.compile(r'((^\+628)|(^08)|(^628)|(^\+08))+([0-9]{8,11})$').match(x) else 0)
        data_phone_2 = data_phone_1.drop_duplicates(subset=['loan_id', 'name'], keep='first')
        data_phone_3 = data_phone_1.drop_duplicates(subset=['loan_id', 'phone'], keep='first')
        data_phone_feat = {}
        data_phone_feat['phone_count_t'] = len(data_phone_1)
        data_phone_feat['phone_count_valid_t'] = data_phone_1['phone_1'].sum()
        data_phone_feat['phone_count_t_drop'] = len(data_phone_2)
        data_phone_feat['phone_count_valid_drop'] = data_phone_2['phone_1'].sum()
        data_phone_feat['phone_count_name_drop'] = len(data_phone_2[['loan_id', 'name']].groupby('name').count())
        data_test_p = data_phone_1[['loan_id', 'name']].groupby('name').count()
        data_phone_feat['phone_count_name_duplicates_drop'] = len(data_test_p[data_test_p['loan_id'] >= 2])
        data_phone_feat['phone_count_name_duplicates_max_drop'] = data_test_p[data_test_p['loan_id'] >= 2].max().iloc[0]
        data_phone_feat['phone_count_t_p_drop'] = len(data_phone_3)
        data_phone_feat['phone_count_valid_p_drop'] = data_phone_3['phone_1'].sum()
        data_test_n = data_phone_1[['loan_id', 'phone']].groupby('phone').count()
        data_phone_feat['phone_count_name_duplicates_p_drop'] = len(data_test_n[data_test_n['loan_id'] >= 2])
        data_phone_feat['phone_count_name_duplicates_max_p_drop'] = data_test_n[data_test_n['loan_id'] >= 2].max().iloc[0]
        data_phone_feat['phone_count_person_phone_drop'] = len(data_phone_2[data_phone_2['name'].isin(person_phone)])
        data_phone_feat['phone_count_company_phone_drop'] = len(data_phone_2[data_phone_2['name'].isin(company_phone)])
        data_phone_feat['phone_count_person_phone_p_drop'] = len(data_phone_3[data_phone_3['name'].isin(person_phone)])
        data_phone_feat['phone_count_company_phone_p_drop'] = len(data_phone_3[data_phone_3['name'].isin(company_phone)])
        data_phone_feat_data = pd.DataFrame(data_phone_feat, index=[self.loan_id])
        return data_phone_feat_data


if __name__ == '__main__':
    data = AddConnectFeat('api_loansuper_x4', 986821).add_phone_feat()
    # data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    print(data)