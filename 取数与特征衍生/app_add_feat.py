#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-17 14:51
# Author     : wuboyuan
# File       : app_add_feat.py
# Desc       : 此代码用于app特征的衍生变量

import time
import numpy as np
import re
import copy
import joblib
from get_data_all import *
import requests


class AppAddFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def date_to_timestamp(self, date=None):  # 时间戳转换
        timeArray = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

    def diff_days(self, date1=None, date2=None):  # 时间的相加减
        return (date1 - date2) / 1000 / (3600 * 24)

    def app_data_time(self, data_1=None):   # APP数据里的时间转换
        data=data_1
        data["create_date_timemp"] = data["create_date"].apply(
            lambda x: self.date_to_timestamp(str(x)) * 1000)

        data['create_date'] = data['create_date_timemp']
        data["install_to_create_days"] = data[[
            "firstInstallTime", "create_date"
        ]].apply(lambda x: self.diff_days(date1=x[1], date2=x[0]), axis=1)
        data["update_to_create_days"] = data[[
            "lastUpdateTime", "create_date"
        ]].apply(lambda x: self.diff_days(date1=x[1], date2=x[0]), axis=1)
        data["install_to_update_days"] = data[[
            "firstInstallTime", "lastUpdateTime"
        ]].apply(lambda x:self. diff_days(date1=x[1], date2=x[0]), axis=1)
        data["system_app"] = data[["appName", "packageName"]].apply(lambda x: 1 if x[0] == x[1] else 0, axis=1)
        mean_installdate, std_installdate = np.mean(
            data["install_to_create_days"]), np.std(data["install_to_create_days"])
        mean_updatedate, std_updatedate = np.mean(
            data["update_to_create_days"]), np.std(data["update_to_create_days"])
        up_limit_install = data["install_to_create_days"].quantile(0.9)
        up_limit_update = data["update_to_create_days"].quantile(0.9)
        data["install_to_create_days"] = data["install_to_create_days"].apply(
            lambda x: up_limit_install
            if x > (mean_installdate + 3 * std_installdate) else x)
        data["update_to_create_days"] = data["update_to_create_days"].apply(
            lambda x: up_limit_update
            if x > (mean_updatedate + 3 * std_updatedate) else x)
        data["appName"] = data["appName"].apply(lambda x: str(x).lower())
        data["is_update"] = data["install_to_update_days"].apply(lambda x: 1 if x > 0 else 0)
        return data

    def count_APP_num(self, data=None, llit=None): # 计算总体的APP安装数量比率等等
        data_data_app = data[['category', 'loan_id']].groupby(by='category').count().T
        data_data_app['total_cnt'] = data_data_app.T.sum()
        all_ot = set(llit) - set(data_data_app.columns)

        for i in all_ot:
            data_data_app[i] = 0
        llit2 = set(data_data_app.columns) - set(['total_cnt'])
        data_data_app_1 = data_data_app[llit2] / data_data_app['total_cnt'].values[0]

        data_data_app_1.columns = data_data_app_1.columns + ['ration_total']
        data_data_app_1 = data_data_app_1.fillna(0)

        data_data_app = pd.concat([data_data_app, data_data_app_1], axis=1)
        return data_data_app

    def app_feats(self, data_t=None, llit=None):    # 计算APP分类的数量比率等等
        calss_category_dict = joblib.load('file/calss_category_dict.pkl')    # APP分类的字典

        review_app1 = joblib.load('file/review_app1.pkl')  # 浏览数量APP分类的字典

        install_app1 = joblib.load('file/install_app1.pkl') # 安装数量APP分类的字典

        popular_app_dict1 = joblib.load('file/popular_app_dict1.pkl')  # 各类安装数量APP分类的字典

        app_level1 = joblib.load('file/app_level1.pkl')  # APP风险等级字典

        col5 = joblib.load('file/col5.pkl')  # 全部columns的list,防止有的客户没有某些字段

        timess = ['install_to_create_days', 'update_to_create_days']
        data_t = self.app_data_time(data_t)
        data_t[['install_to_create_days', 'update_to_create_days', 'install_to_update_days']] = data_t[
            ['install_to_create_days', 'update_to_create_days', 'install_to_update_days']].astype(int)
        data_t["category"] = data_t['packageName'].map(calss_category_dict)
        data_t["category"] = data_t["category"].fillna("others")
        zz_data_0 = {}
        zz_data_0['days'] = {}
        zz_data_0['nmbers'] = {}
        zz_data_d = {}
        zz_data_d1 = {}
        zz_data_d1_ration = {}
        for q in timess:
            zz_data_0['days'][q] = {}
            zz_data_0['nmbers'][q] = {}
            data = data_t[data_t[q] <= 3000]
            data[q] = data[q].astype(float)
            zz_data_0['days'][q]['all'] = copy.deepcopy(pd.DataFrame(data[q].describe()).T)
            zz_data_0['days'][q]['all'].columns = 'all_' + q + 'days_' + zz_data_0['days'][q]['all'].columns
            zz_data_0['nmbers'][q]['all'] = copy.deepcopy(data[[q, 'loan_id']].groupby(q).count().describe().T)
            zz_data_0['nmbers'][q]['all'].columns = 'all_' + q + 'nmbers_' + zz_data_0['nmbers'][q]['all'].columns
            indd = -10
            mu = [indd]
            zz_data_d[q] = {}
            zz_data_d1[q] = {}
            zz_data_d1_ration[q] = {}
            for j in [180, 60, 21, 7]:
                data2 = data[data[q] < j]
                zz_data_0['days'][q][j] = copy.deepcopy(pd.DataFrame(data2[q].describe()).T.fillna(0))
                zz_data_0['days'][q][j].columns = str(j) + q + '_days_' + zz_data_0['days'][q][j].columns
                zz_data_0['nmbers'][q][j] = copy.deepcopy(
                    data2[[q, 'loan_id']].groupby(q).count().describe().T.fillna(0))
                zz_data_0['nmbers'][q][j].columns = str(j) + q + '_nmbers_' + zz_data_0['nmbers'][q][j].columns
                zz_data = self.count_APP_num(data2, llit)
                zz_data.columns = str(j) + q + zz_data.columns
                data1_1 = pd.DataFrame(
                    data2['packageName'].map(review_app1).fillna('review_app_others').value_counts()).T
                for i in set(list(set(review_app1.values())) + ['review_app_others']) - set(data1_1.columns):
                    data1_1[i] = 0

                data1_2 = pd.DataFrame(
                    data2['packageName'].map(install_app1).fillna('install_app_others').value_counts()).T
                for i in set(list(set(install_app1.values())) + ['install_app_others']) - set(data1_2.columns):
                    data1_2[i] = 0

                data1_3 = pd.DataFrame(
                    data2['packageName'].map(popular_app_dict1).fillna('popular_app_others').value_counts()).T

                for i in set(list(set(popular_app_dict1.values())) + ['popular_app_others']) - set(data1_3.columns):
                    data1_3[i] = 0

                data1_4 = pd.DataFrame(data2['packageName'].map(app_level1).fillna('app_leve_others').value_counts()).T
                for i in set(list(set(app_level1.values())) + ['app_leve_others']) - set(data1_4.columns):
                    data1_4[i] = 0

                data1_5 = pd.concat([data1_1, data1_2, data1_3, data1_4], axis=1)

                for i in set(col5) - set(data1_5.columns):
                    data1_5[i] = 0
                data1_5 = data1_5[col5]
                data1_5.columns = str(j) + q + data1_5.columns + 'cnt'
                zz_data_d1[q][j] = copy.deepcopy(data1_5)
                if indd > j:
                    for m in mu[1:]:
                        ddl = copy.deepcopy(zz_data_d1[q][m])
                        ddl.columns = zz_data_d1[q][j].columns
                        ddl2 = zz_data_d1[q][j] / ddl
                        cool = q + zz_data_d1[q][j].columns + '_' + zz_data_d1[q][m].columns + 'ration'
                        ddl2.columns = cool
                        zz_data_d1_ration[q][j] = copy.deepcopy(ddl2)

                zz_data[str(j) + q + 'packageName_finance_cnt'] = data2['packageName'].apply(lambda x: 1 if re.search(
                    r'dana|cash|kredit|pinjaman|pinjam|dompet|ksp|ojk|darurat|tunai|modal|finance|cepat|keuangan|uang|loan|saku|tagihan|lender|money|uangku|kami|darurat|emas|loan|kantong|duit|rupiah|uangme|ekpressuang|pinjaman|kredi|uang|dan',
                    x, re.I) else 0).sum()

                zz_data[str(j) + q + 'packageName_gambling_cnt'] = data2['packageName'].apply(
                    lambda x: 1 if re.search(r'Mesinjudi|slot|Pertaruhan|perjudian|kasino', x, re.I) else 0).sum()

                zz_data[str(j) + q + 'packageName_GPS_cnt'] = data2['packageName'].apply(
                    lambda x: 1 if re.search(r'clone|FakeGPS', x, re.I) else 0).sum()

                zz_data[str(j) + q + 'alphabet_cnt'] = data2['packageName'].apply(
                    lambda x: 0 if re.search('[A-Z]', x, re.I) else 1).sum()

                zz_data_d[q][j] = copy.deepcopy(zz_data)

                indd = j
                mu.append(j)
        t_data_1 = pd.concat(
            [zz_data_0['days']['install_to_create_days']['all'], zz_data_0['days']['install_to_create_days'][180],
             zz_data_0['days']['install_to_create_days'][60], zz_data_0['days']['install_to_create_days'][21],
             zz_data_0['days']['install_to_create_days'][7]], axis=1)

        t_data_2 = pd.concat(
            [zz_data_0['days']['update_to_create_days']['all'], zz_data_0['days']['update_to_create_days'][180],
             zz_data_0['days']['update_to_create_days'][60], zz_data_0['days']['update_to_create_days'][21],
             zz_data_0['days']['update_to_create_days'][7]], axis=1)

        t_data_3 = pd.concat(
            [zz_data_0['nmbers']['install_to_create_days']['all'], zz_data_0['nmbers']['install_to_create_days'][180],
             zz_data_0['nmbers']['install_to_create_days'][60], zz_data_0['nmbers']['install_to_create_days'][21],
             zz_data_0['nmbers']['install_to_create_days'][7]], axis=1)

        t_data_4 = pd.concat(
            [zz_data_0['nmbers']['update_to_create_days']['all'], zz_data_0['nmbers']['update_to_create_days'][180],
             zz_data_0['nmbers']['update_to_create_days'][60], zz_data_0['nmbers']['update_to_create_days'][21],
             zz_data_0['nmbers']['update_to_create_days'][7]], axis=1)

        t_data_5 = pd.concat([zz_data_d['install_to_create_days'][180], zz_data_d['install_to_create_days'][60],
                              zz_data_d['install_to_create_days'][21], zz_data_d['install_to_create_days'][7]], axis=1)

        t_data_6 = pd.concat([zz_data_d['update_to_create_days'][180], zz_data_d['update_to_create_days'][60],
                              zz_data_d['update_to_create_days'][21], zz_data_d['update_to_create_days'][7]], axis=1)

        t_data_7 = pd.concat([zz_data_d1['install_to_create_days'][180], zz_data_d1['install_to_create_days'][60],
                              zz_data_d1['install_to_create_days'][21], zz_data_d1['install_to_create_days'][7]],
                             axis=1)

        t_data_8 = pd.concat([zz_data_d1['update_to_create_days'][180], zz_data_d1['update_to_create_days'][60],
                              zz_data_d1['update_to_create_days'][21], zz_data_d1['update_to_create_days'][7]], axis=1)

        t_data_9 = pd.concat(
            [zz_data_d1_ration['install_to_create_days'][60], zz_data_d1_ration['install_to_create_days'][21],
             zz_data_d1_ration['install_to_create_days'][7]], axis=1)

        t_data_10 = pd.concat(
            [zz_data_d1_ration['update_to_create_days'][60], zz_data_d1_ration['update_to_create_days'][21],
             zz_data_d1_ration['update_to_create_days'][7]], axis=1)

        t_data_1.index = [data_t['loan_id'].iloc[0]]
        t_data_2.index = [data_t['loan_id'].iloc[0]]
        t_data_3.index = [data_t['loan_id'].iloc[0]]
        t_data_4.index = [data_t['loan_id'].iloc[0]]
        t_data_5.index = [data_t['loan_id'].iloc[0]]
        t_data_6.index = [data_t['loan_id'].iloc[0]]
        t_data_7.index = [data_t['loan_id'].iloc[0]]
        t_data_8.index = [data_t['loan_id'].iloc[0]]
        t_data_9.index = [data_t['loan_id'].iloc[0]]
        t_data_10.index = [data_t['loan_id'].iloc[0]]
        t_data = pd.concat(
            [t_data_1, t_data_2, t_data_3, t_data_4, t_data_5, t_data_6, t_data_7, t_data_8, t_data_9, t_data_10],
            axis=1)
        return t_data

    def get_requests(self, url, encod=True):
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

if __name__ == '__main__':
    data = GetDataAll('api_loansuper_x4', 986821).get_applist()
    data_a = pd.DataFrame(eval(AppAddFeat().get_requests(url=data['applist_json'].iloc[0])))
    data_a["create_date"] = data['request_date'].iloc[0]
    data_a["loan_id"] = 986821
    llit = joblib.load('file/llit.pkl') # APP分类类别 防止有的客户确实某类APP
    app_add_feats = AppAddFeat().app_feats(data_a,llit)
    print(llit)