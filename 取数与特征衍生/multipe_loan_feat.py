#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-17 17:12
# Author     : wuboyuan
# File       : multipe_loan_feat.py
# Desc       : 此代码用于内部多头衍生变量

from get_data_all import *

class MultipeLoanFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def multipe_data(self, id_card_no=None):
        ind = 0
        for db in ['api_loansuper', 'api_loansuper_x2', 'api_loansuper_x4', 'api_loansuper_x5']:
            if ind == 0:
                multipe_loan_set1 =GetDataAll(db=db).get_multipe_loan(id_card_no)
                ind = 1
            else:
                multipe_loan_set1 = multipe_loan_set1.append(GetDataAll(db=db).get_multipe_loan(id_card_no))
        return multipe_loan_set1
    def count_multipe(self,multipeloan=None,requests_date_y=None,loan_id=None):
        multipeloan_result={}
        multipeloan1=multipeloan[~multipeloan['push_cash_date'].isnull()]
        multipeloan1.loc[:,'push_cash_date']=pd.to_datetime(multipeloan1['push_cash_date'])
        for day_y in [0.1,1,3,5,7,10,365]:
            data_set=multipeloan[(multipeloan['request_date']>requests_date_y+pd.Timedelta(days=-day_y)) &(multipeloan['request_date']<requests_date_y)]
            data_set2=multipeloan1[(multipeloan1['push_cash_date']>requests_date_y+pd.Timedelta(days=-day_y)) &(multipeloan1['push_cash_date']<requests_date_y)]

            multipeloan_result['loan_request_'+str(day_y)]=len(data_set)
            multipeloan_result['loan_push_'+str(day_y)]=len(data_set2[data_set2['loan_order_status']==5])
            multipeloan_result['remain_loan_amount_'+str(day_y)]=data_set2[data_set2['loan_order_status']==5]['loan_amount'].sum()
            multipeloan_loanid=pd.DataFrame(multipeloan_result,index=[loan_id])
        return multipeloan_loanid

    def multipe_result(self):
        data_id = GetDataAll(db=self.db, loan_id=self.loan_id).get_id_data().iloc[-1:]
        data_id_test = data_id[data_id['loan_id'] == self.loan_id]
        requests_date_y =data_id['request_date'] .iloc[0]
        loan_id = data_id['loan_id'] .iloc[0]
        id_card_no = data_id_test.id_card_no.iloc[0]
        multipeloan = self.multipe_data(id_card_no=id_card_no)
        count_multipe_loan = self.count_multipe(multipeloan=multipeloan, requests_date_y=requests_date_y, loan_id=loan_id)
        return count_multipe_loan


if __name__ == '__main__':
    data = MultipeLoanFeat('api_loansuper_x4', 986821).multipe_result()
    # data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    print(data)