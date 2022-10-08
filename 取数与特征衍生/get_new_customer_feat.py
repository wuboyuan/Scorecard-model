#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-18 16:59
# Author     : wuboyuan
# File       : get_new_customer_feat.py
# Desc       : 此代码用于 取到新客的全部特征

from get_data_all import *

from app_add_feat import *
import warnings

warnings.filterwarnings("ignore")

from add_izi_feat import *

from full_link_feat import *

from add_connect_feat import *

class GetNewCustomerFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id
    def get_new_data_feat(self):
        #取得full_link表里的字段
        data1 = GetDataAll(self.db, self.loan_id).get_fullinfo_data().iloc[-1:]
        data1.index = data1['loan_id']

        #取得除开json字段的其他字段
        full_feat=FullLinkFeat(self.db, self.loan_id).get_all_full_feat(data1=data1)
        full_feat=full_feat.drop(['app_user_id','loan_id','loanId','id','update_date','create_date'],axis=1)

        #取得appfeat数据
        app_json_data =FullLinkFeat(self.db, self.loan_id).get_applist_json(data1)
        app_data = pd.DataFrame(eval(AppAddFeat().get_requests(url=app_json_data['applistJson'].iloc[0])))
        app_data["create_date"] = data1['request_date'].iloc[0]
        app_data["loan_id"] = self.loan_id
        llit = joblib.load('file/llit.pkl')
        app_add_feats = AppAddFeat().app_feats(app_data, llit)

        #取得hidden_json的数据
        hidden_data=FullLinkFeat(self.db, self.loan_id).get_hidden_json(data1=data1)
        hidden_data.index=[hidden_data['loanId'].iloc[0]]
        hidden_data=hidden_data.drop(['loanId'],axis=1)

        #取得izi_feat数据
        izi_data = GetDataAll(self.db, self.loan_id).get_new_izi_data()
        dr_p = ['appUserId', 'identityAddress', 'identityCity', 'identityDateOfBirth', 'identityDistrict',
                'identityGender',
                'identityName', 'identityNationnality', 'identityPlaceOfBirth', 'identityProvince', 'identityVillage',
                'identityWork', 'itemCode', 'names', 'phone', 'systemUserId', 'status', 'systemUserNick',
                'whatsappAvatar',
                'whatsappSignature', 'whatsappUpdatestatusTime']
        columns_izi = set(izi_data.columns) - set(dr_p)
        izi_data = izi_data[columns_izi]
        izi_add_data = IziAddFeat().add_features(izi_data).iloc[-1:]
        izi_add_data.index =[self.loan_id]
        izi_add_data=izi_add_data.drop(['app_user_id','id_number','loan_id','request_date','update_date'],axis=1)

        # 取得通讯录字段
        connect_data = AddConnectFeat(self.db, self.loan_id).add_phone_feat()

        # merge数据

        new_data_feat=full_feat.merge(app_add_feats,how='left',right_index=True,left_index=True)
        new_data_feat=new_data_feat.merge(hidden_data,how='left',right_index=True,left_index=True)
        new_data_feat = new_data_feat.merge(izi_add_data, how='left', right_index=True, left_index=True)
        new_data_feat = new_data_feat.merge(connect_data, how='left', right_index=True, left_index=True)

        return new_data_feat


if __name__ == '__main__':
    #izi_data = GetDataAll('api_loansuper_x4', 986821).get_new_izi_data()
    data = GetNewCustomerFeat('api_loansuper_x4', 978847).get_new_data_feat()
    print(data)