#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-17 20:18
# Author     : wuboyuan
# File       : full_link_feat.py
# Desc       : 此代码用于full_link里的字段处理问题

from app_add_feat import *




class FullLinkFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def get_all_full_feat(self,data1=None):
        #data1=GetDataAll(self.db, self.loan_id).get_fullinfo_data().iloc[-1:]
        data1.index = data1['loan_id']
        json_list=['app_user_device_json','app_user_applist_json','app_user_hidden_data_json', 'izidata_credit_feature_v2_json']
        cl=list(set(data1.columns)-set(json_list))
        data = data1[cl]
        data = data.drop_duplicates(subset='loan_id', keep='first')
        data = data[data['loan_id'] == self.loan_id]
        data['loanId'] = data['loan_id']
        return data

    def get_device_feat(self,data1=None):
        data_device = pd.DataFrame(eval(data1.loc[self.loan_id]['app_user_device_json']))
        data_device = data_device[data_device['loanId'] == self.loan_id].iloc[-1:]
        data_device = data_device[['deciveBrand', 'loanId']]
        return  data_device

    def get_applist_json(self,data1=None):
        data_applist_json = pd.DataFrame(eval(data1.loc[self.loan_id]['app_user_applist_json']))
        data_applist_json = data_applist_json[data_applist_json['loanId'] == self.loan_id].iloc[-1:]
        return  data_applist_json

    def get_hidden_json(self,data1=None):
        data_hidden_data_json = pd.DataFrame(eval(data1.loc[self.loan_id]['app_user_hidden_data_json']))
        data_hidden_data_json=data_hidden_data_json[data_hidden_data_json['loanId'] == self.loan_id].iloc[-1:]
        data_hidden_data_json = data_hidden_data_json[['bootTime', 'faceCheckIp' , 'screenRateLong', 'screenRateWidth', 'storageTotalSize', 'loanId']]
        return  data_hidden_data_json



if __name__ == '__main__':
    data1 = GetDataAll('api_loansuper_x4', 978847).get_fullinfo_data().iloc[-1:]
    data1.index = data1['loan_id']
    data = FullLinkFeat('api_loansuper_x4', 978847).get_applist_json(data1)
    data_a = pd.DataFrame(eval(AppAddFeat().get_requests(url=data['applistJson'].iloc[0])))
    data_a["create_date"] = data1['request_date'].iloc[0]
    data_a["loan_id"] = 986821
    llit = joblib.load('file/llit.pkl')
    app_add_feats = AppAddFeat().app_feats(data_a, llit)
    data_hidden = FullLinkFeat('api_loansuper_x4', 978847).get_hidden_json(data1)
    #data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    print(data_hidden)

