#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-18 14:34
# Author     : wuboyuan
# File       : get_old_iziv4_feat.py
# Desc       : 此代码用于老客iziV4变化

from get_data_all import *

class GetOldIziV4feat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id
    def get_last_loan(self):
        conn = pymysql.connect(
            host="",
            user="",
            password="",
            db=self.db,
            charset="utf8")

        sql = f"""
                SELECT t.*
                FROM 
                (SELECT
                a.loan_id,a.id_card_no,b.loan_id as loan_id_b 
                from
                (SELECT * from t_loan_order WHERE loan_id ='{self.loan_id}') as a
                LEFT JOIN
                (SELECT * from t_loan_order WHERE extend_times =0) as b on a.id_card_no=b.id_card_no
                left join t_loan_order_operate as c on c.loan_id=b.loan_id
                WHERE
                a.request_date>c.sell_accounts_date
                order by c.repay_date desc
                limit 999) t
                WHERE
                t.loan_id='{self.loan_id}'
                group by t.loan_id
               """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_old_phone_v4_feat(self,merchant_code=None):
        last_loan=self.get_last_loan()
        data1 =GetDataAll(self.db,self.loan_id).get_izi_phone_v4(merchant_code=merchant_code)
        data2=GetDataAll(self.db,last_loan['loan_id_b'].iloc[0]).get_izi_phone_v4(merchant_code=merchant_code)
        data3 = data1[data1.columns[8:]] - data2[data1.columns[8:]]
        data3['loan_id'] = last_loan['loan_id'].iloc[0]
        return data3

    def get_old_id_v4_feat(self, merchant_code=None):
        last_loan = self.get_last_loan()

        data1 = GetDataAll(self.db, self.loan_id).get_izi_id_v4(merchant_code=merchant_code)
        data2 = GetDataAll(self.db, loan_id=last_loan['loan_id_b'].iloc[0]).get_izi_id_v4(merchant_code=merchant_code)
        data3 = data1[data1.columns[8:]] - data2[data1.columns[8:]]
        data3['loan_id'] = last_loan['loan_id'].iloc[0]
        return data3

if __name__ == '__main__':
    data = GetOldIziV4feat('api_loansuper_x4', 986919).get_old_id_v4_feat(merchant_code='x4_system').get_last_loan()
    data.to_csv( 'C:/Users/Administrator/Desktop/test_data.csv')
    # data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    print(data)