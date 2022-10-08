#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-18 20:22
# Author     : wuboyuan
# File       : get_old_customer_feat.py
# Desc       : 此代码用于取到老客的全部特征

from get_data_all import *

from app_add_feat import *

from multipe_loan_feat import *
from get_old_iziv4_feat import *
from get_old_app_feat import *
import joblib
import warnings

warnings.filterwarnings("ignore")

from add_izi_feat import *

from full_link_feat import *

from add_connect_feat import *


class GetOldCustomerFeat(object):
    pass


class GetOldCustomerFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def get_old_data_feat(self, merchant_code=None):
        # 取得full_link表里的字段
        data1 = GetDataAll(self.db, self.loan_id).get_fullinfo_data().iloc[-1:]
        data1.index = data1['loan_id']

        # 取得除开json字段的其他字段
        full_feat = FullLinkFeat(self.db, self.loan_id).get_all_full_feat(data1=data1)
        full_feat = full_feat.drop(['app_user_id', 'loan_id', 'loanId', 'id', 'update_date', 'create_date'], axis=1)

        # 取得appfeat数据
        app_json_data = FullLinkFeat(self.db, self.loan_id).get_applist_json(data1)
        app_data = pd.DataFrame(eval(AppAddFeat().get_requests(url=app_json_data['applistJson'].iloc[0])))
        app_data["create_date"] = data1['request_date'].iloc[0]
        app_data["loan_id"] = self.loan_id
        llit = joblib.load('file/llit.pkl')
        app_add_feats = AppAddFeat().app_feats(app_data, llit)

        # 取得hidden_json的数据
        hidden_data = FullLinkFeat(self.db, self.loan_id).get_hidden_json(data1=data1)
        hidden_data.index = [hidden_data['loanId'].iloc[0]]
        hidden_data = hidden_data.drop(['loanId'], axis=1)

        # 取得izi_feat数据
        izi_data = GetDataAll(self.db, self.loan_id).get_old_izi_data()
        dr_p = ['appUserId', 'identityAddress', 'identityCity', 'identityDateOfBirth', 'identityDistrict',
                'identityGender',
                'identityName', 'identityNationnality', 'identityPlaceOfBirth', 'identityProvince', 'identityVillage',
                'identityWork', 'itemCode', 'names', 'phone', 'systemUserId', 'status', 'systemUserNick',
                'whatsappAvatar',
                'whatsappSignature', 'whatsappUpdatestatusTime']
        columns_izi = set(izi_data.columns) - set(dr_p)
        izi_data = izi_data[columns_izi]
        izi_add_data = IziAddFeat().add_features(izi_data).iloc[-1:]
        llit = joblib.load('file/llit.pkl')  # APP分类类别 防止有的客户确实某类APP
        izi_add_data.index = [self.loan_id]
        izi_add_data = izi_add_data.drop(['app_user_id', 'id_number', 'loan_id', 'update_date'], axis=1)

        # 取得通讯录字段
        connect_data = AddConnectFeat(self.db, self.loan_id).add_phone_feat()

        # 取得内部多头
        multipeloan_data = MultipeLoanFeat(self.db, self.loan_id).multipe_result()

        # 取得老客iziv4增量
        iziv4_data = GetOldIziV4feat(self.db, self.loan_id).get_old_id_v4_feat(merchant_code=merchant_code)
        iziv4_data.index = [self.loan_id]
        iziv4_data = iziv4_data.drop(['loan_id'], axis=1)
        iziv4_data.columns = iziv4_data.columns + '_old'

        # 老客app特征变化
        old_app_data = GetOldAppFeat(self.db, self.loan_id).count_app_fun()
        old_app_data.columns = old_app_data.columns + '_old'

        # merge数据

        old_data_feat = full_feat.merge(app_add_feats, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(hidden_data, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(izi_add_data, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(connect_data, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(multipeloan_data, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(iziv4_data, how='left', right_index=True, left_index=True)
        old_data_feat = old_data_feat.merge(old_app_data, how='left', right_index=True, left_index=True)

        return old_data_feat


if __name__ == '__main__':

    data = GetOldCustomerFeat('api_loansuper_x4', 986919).get_old_data_feat(merchant_code='x4_system')

    print(data)
