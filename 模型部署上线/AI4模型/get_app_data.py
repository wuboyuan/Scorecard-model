# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import joblib
from add_appfeatures import AddFeatures
from parse_data import Request_data
import pandas as pd
import numpy as np
import time


def cale_woe(df1, var, target):
    df = df1.copy()
    # df[var]=df[var].fillna("missing")

    data = df.groupby([var])[target].agg({
        "count": "count",
        "bad": "sum",
        "bad_rate": "mean"
    }).reset_index()
    bad_total = sum(data["bad"])
    data["bad_pcnt"] = data["bad"] / bad_total
    data["good"] = data["count"] - data["bad"]
    data["good_rate"] = 1 - data["bad_rate"]
    good_total = sum(data["good"])
    data["good_pcnt"] = data["good"] / good_total
    data["count_distr"] = data["count"] / df.shape[0]
    data["woe"] = np.log(data["good_pcnt"] / data["bad_pcnt"])
    data["iv"] = (data["good_pcnt"] - data["bad_pcnt"]) * np.log(
        data["good_pcnt"] / data["bad_pcnt"])
    data["iv_sum"] = sum(data["iv"])
    data["lift"] = data["bad_rate"] / df[target].mean()
    data["variable"] = var
    print("{}等于'{}'时最大提升度为{:.2f}".format(
        var, str(data[data["lift"] == data["lift"].max()][var].values[0]),
        data["lift"].max()))
    data = data[[
        "variable", var, 'count', 'bad', 'bad_rate', 'good', 'good_rate',
        'count_distr', 'woe', 'iv', 'iv_sum', 'lift'
    ]]
    return data


def woe_values(df, col, tar):
    woe_data = cale_woe(df, col, tar)

    woe_dict = {x: y for x, y in zip(woe_data[col], woe_data["woe"])}
    return woe_dict


def dump_model(clf, file_name='lgb.model'):
    # lr是一个LogisticRegression模型
    # path='/Users/qianfeng1/Desktop/products/product/model_flask/'
    joblib.dump(clf, file_name)


def load_model(path):
    return joblib.load(path)


def woe_list(df, tar, woe_col=[], mapping=True):
    if mapping:
        # dump_model(woe_dict,file_name="woe_dict.pkl")
        woe_dict_loan = load_model("woe_dict.pmml")
        for col in woe_dict_loan.keys():
            df[col] = df[col].map(woe_dict_loan[col])
    else:
        woe_dict = {}
        for col in woe_col:
            woe_dict[col] = woe_values(df, col, tar)
        for col in woe_dict.keys():
            df[col] = df[col].map(woe_dict[col])
        dump_model(woe_dict, "woe_dict.pmml")
    return df


class Process_appdata:
    def __init__(self, data=None):
        self.data = data

    def job_map(self, x):
        job = [
            '38', '8', '5', '2', '4', '9', '13', '28', '1', '19', '31', '21',
            '3', '6', '26', '36', '7', '37'
        ]
        if x in job:
            return x
        else:
            return "others"

    def brand_map(self, x):
        if x in ['oppo', 'samsung', 'vivo', 'xiaomi', 'realme']:
            return x
        else:
            return "others"

    def age_map(self, x):
        if x < 18:
            return 18
        elif 18 <= x < 54:
            return x
        else:
            return 54

    def map_internet(self, x):
        if x == "NETWORK_WIFI":
            return "wifi"
        elif x == "NETWORK_4G":
            return "4g"
        else:
            return "others"

    def salary_map(self, x):
        if float(x) > 3:
            return 3
        else:
            return x

    def map_add(self, x):
        if x in [
            'Sulawesi Tenggara', 'Papua', 'Sulawesi Tengah', 'Maluku',
            'Nusa Tenggara Barat', 'Kep Bangka Belitung', 'Bengkulu',
            'Gorontalo', 'Kalimantan utara', 'Maluku Utara', 'Papua Barat',
            'Sulawesi Barat', 'Kalimantan'
        ]:
            return "others"
        return x

    def education_map(self, x):
        if x in ["9", "3", "8"]:
            return "3"
        elif float(x) < 3:
            return "3"
        else:
            return x

    def remove_null_col(self, data, thresholds=0.8):
        null_ = data.isnull().sum().reset_index(name="value")
        null_["rate"] = null_["value"] / data.shape[0]
        cols = null_[null_["rate"] <= thresholds]["index"].tolist()
        return cols

    def process_data(self, data=None):
        data["liveInDate"] = pd.to_datetime(data["liveInDate"])
        data["request_date"] = pd.to_datetime(data["request_date"])
        data["live_days"] = data[["request_date",
                                  "liveInDate"]].apply(lambda x:
                                                       (x[0] - x[1]).days,
                                                       axis=1)
        data["age"] = data["age"].apply(self.age_map)
        data["decive_brand"] = data["decive_brand"].apply(
            lambda x: np.NaN if not x else str(x).lower())
        data["decive_brand"] = data["decive_brand"].apply(self.brand_map)
        data["company_job"] = data.company_job.apply(
            lambda x: self.job_map(str(x)))
        data["internet_type"] = data["internet_type"].apply(self.map_internet)
        data["company_province"] = data["company_province"].apply(self.map_add)
        data["famil_province"] = data["famil_province"].apply(self.map_add)
        data["company_month_income"] = data["company_month_income"].apply(self.salary_map)
        data["education_level"] = data["education_level"].apply(
            lambda x: self.education_map(str(x)))
        data = data.fillna(-9999)
        return data

    def to_woe(self, data=None, target=True):
        if target:
            data = woe_list(data,
                            "tar",
                            woe_col=[
                                'company_job',
                                'company_province',
                                'decive_brand',
                                'famil_province',
                                'internet_type',
                                'education_level',
                                'company_month_income'
                            ],
                            mapping=False)
        else:
            data = woe_list(data,
                            "tar",
                            woe_col=[
                                'company_job',
                                'company_province',
                                'decive_brand',
                                'famil_province',
                                'internet_type',
                                'education_level',
                                'company_month_income'
                            ],
                            mapping=True)
        return data


class Get_applist_data:
    def __init__(self, user_id=None):
        self.user_id = user_id

    def calculate_feats(self,db='dompet_second',loan_id=None, user_id="ac93d3ef01a04382a1851fdc948dd1fe"):
        parse_data = Request_data().parse_applist(db=db,loan_id=loan_id,user_id=user_id)
        data = AddFeatures().add_feats(parse_data=parse_data, dataframe=True)
        process = Process_appdata()
        data = process.process_data(data=data)
        data = process.to_woe(data=data, target=False)
        return data


if __name__ == '__main__':
    time1 = time.time()
    data = Get_applist_data().calculate_feats(user_id="d6beefe009524ff3a225417343733f19")
    print(data.boot_time)
#     time2=time.time()
#     cost_time=time2-time1
#     print(data)
#     print(cost_time)