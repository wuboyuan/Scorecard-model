# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from get_app_data import Get_applist_data
from get_izi_data import izi_data_sql
from get_multi import sql_table
import pandas as pd
import warnings
import datetime
import numpy as np
from DateTimeProcess import *

warnings.filterwarnings("ignore")


class Process_concat_data:
    def __init__(self, user_id=None):
        self.user_id = user_id

    def concat_data(self, db="dompet_second",loan_id=None,user_id="fc0185e95a354666bd995edfd8adfb00"):
        app_data = Get_applist_data().calculate_feats(db=db,loan_id=loan_id,user_id=user_id)

        izi_data = izi_data_sql(db=db,loan_id=loan_id,user_id=user_id)
        # izi_data.to_csv("izi_data.csv",index=None)
        # print("izi_data:",izi_data.b_activeYear)
        data = pd.concat([app_data, izi_data], axis=1)
        data=data.fillna(-9999)
        # data.to_csv("data.csv", index=None)
        return data

    def trams_to_numirc(self):
        tram_to_number = [
            'preference_bank_60d',
            'preference_bank_90d',
            'preference_bank_180d',
            'preference_bank_270d',
            'preference_ecommerce_60d',
            'preference_ecommerce_90d',
            'preference_ecommerce_180d',
            'preference_ecommerce_270d',
            'preference_game_60d',
            'preference_game_90d',
            'preference_game_180d',
            'preference_game_270d',
            'preference_lifestyle_60d',
            'preference_lifestyle_90d',
            'preference_lifestyle_180d',
            'preference_lifestyle_270d',
            'phonescore',
            'topup_0_30_avg',
            'topup_0_30_max',
            'topup_0_30_min',
            'topup_0_60_avg',
            'topup_0_60_max',
            'topup_0_60_min',
            'topup_0_90_avg',
            'topup_0_90_max',
            'topup_0_90_min',
            'topup_0_180_avg',
            'topup_0_180_max',
            'topup_0_180_min',
            'topup_0_360_avg',
            'topup_0_360_max',
            'topup_0_360_min',
            'topup_30_60_avg',
            'topup_30_60_max',
            'topup_30_60_min',
            'topup_60_90_avg',
            'topup_60_90_max',
            'topup_60_90_min',
            'topup_90_180_avg',
            'topup_90_180_max',
            'topup_90_180_min',
            'topup_180_360_avg',
            'topup_180_360_max',
            'topup_180_360_min',
            'topup_360_720_avg',
            'topup_360_720_max',
            'topup_360_720_min',
            'update_whatsapp',
            'multiphoneinquiries_7d',
            'multiphoneinquiries_14d',
            'multiphoneinquiries_21d',
            'multiphoneinquiries_30d',
            'multiphoneinquiries_60d',
            'multiphoneinquiries_90d',
            'multiphoneinquiries_total',
            'multiidinquiries_7d',
            'multiidinquiries_14d',
            'multiidinquiries_21d',
            'multiidinquiries_30d',
            'multiidinquiries_60d',
            'multiidinquiries_90d',
            'multiidinquiries_total',
            'b_class', 'b_membership', 'b_activeYear', 'b_activeStatus', 'b_members'
        ]
        return tram_to_number

    def date(self, x):
        try:
            return (pd.to_datetime(x["request_date"]) -
                    pd.to_datetime(x["whatsapp_updatestatus_time"])).days
        except:
            return -9999

    def types_func(self, data=None):
        type_dict = {}
        types_ = data.dtypes.reset_index(name="types")
        type_dict["datetime_type"] = types_[(
                types_.types == "datetime64[ns]")]["index"].tolist()
        type_dict["object_type"] = types_[(
                types_.types == "object")]["index"].tolist()
        type_dict["numberic_type"] = types_[
            (types_.types != "datetime64[ns]")
            & (types_.types != "object")]["index"].tolist()
        return type_dict

    def object_to_numric_features(self, db="dompet_second",loan_id=None,user_id=None):
        data = self.concat_data(db=db,loan_id=loan_id,user_id=user_id)
        data["update_whatsapp"] = data["whatsapp_updatestatus_time"].apply(self.date)
        tram_to_number = self.trams_to_numirc()
        for trams in tram_to_number:
            data[trams] = data[trams].astype(float)
        # types_dict = self.types_func(data=data)
        # numberic_type = types_dict["numberic_type"]
        # numberic_type.remove("id")
        data["boot_time"] = data.boot_time.astype(float)
        # feats = numberic_type + tram_to_number + ["app_user_id", "boot_time"]
        # feats = list(set(feats))
        # if "id" in feats:
        #     print("True")
        #     feats.remove("id")
        # data = data[feats]
        return data

    def add_features(self, data=None):
        for day in [3, 7, 14, 21, 30, 60, 90, 180, 360]:
            data[f"PI_{day}d"] = data[f"A_PI_{day}d"] + data[
                f"B_PI_{day}d"] + data[f"C_PI_{day}d"]
            data[f"II_{day}d"] = data[f"A_II_{day}d"] + data[
                f"B_II_{day}d"] + data[f"C_II_{day}d"]
            data[f"PI_II_{day}d_diff"] = data[f"II_{day}d"] - data[f"PI_{day}d"]
            data[f"PI_II_{day}d_ratio"] = data[f"II_{day}d"] / (
                    data[f"PI_{day}d"] + 10e-8)
            data[f"A_PI_II_{day}d_diff"] = data[f"A_II_{day}d"] - data[
                f"A_PI_{day}d"]
            data[f"B_PI_II_{day}d_diff"] = data[f"B_II_{day}d"] - data[
                f"B_PI_{day}d"]
            data[f"C_PI_II_{day}d_diff"] = data[f"C_II_{day}d"] - data[
                f"C_PI_{day}d"]
            data[f"A_PI_II_{day}d_ratio"] = data[f"A_II_{day}d"] / (
                    data[f"A_PI_{day}d"] + 10e-8)
            data[f"B_PI_II_{day}d_ratio"] = data[f"B_II_{day}d"] / (
                    data[f"B_PI_{day}d"] + 10e-8)
            data[f"C_PI_II_{day}d_ratio"] = data[f"C_II_{day}d"] / (
                    data[f"C_PI_{day}d"] + 10e-8)
            data[f"A_PI_{day}d_ratio"] = data[f"A_PI_{day}d"] / (
                    data[f"PI_{day}d"] + 10e-8)
            data[f"B_PI_{day}d_ratio"] = data[f"B_PI_{day}d"] / (
                    data[f"PI_{day}d"] + 10e-8)
            data[f"C_PI_{day}d_ratio"] = data[f"C_PI_{day}d"] / (
                    data[f"PI_{day}d"] + 10e-8)
            data[f"A_II_{day}d_ratio"] = data[f"A_II_{day}d"] / (
                    data[f"II_{day}d"] + 10e-8)
            data[f"B_II_{day}d_ratio"] = data[f"B_II_{day}d"] / (
                    data[f"II_{day}d"] + 10e-8)
            data[f"C_II_{day}d_ratio"] = data[f"C_II_{day}d"] / (
                    data[f"II_{day}d"] + 10e-8)
            data[f"AB_PI_{day}d_ratio"] = (data[f"B_PI_{day}d"] +
                                           data[f"A_PI_{day}d"]) / (
                                                  data[f"PI_{day}d"] + 10e-8)
            data[f"AB_II_{day}d_ratio"] = (data[f"B_II_{day}d"] +
                                           data[f"A_II_{day}d"]) / (
                                                  data[f"II_{day}d"] + 10e-8)
        days_ = [
            "3d", "7d", "14d", "21d", "30d", "60d", "90d", "180d", "360d", "total"
        ]
        for day in days_:
            data[f"{day}d_nquiries_PI_diff"] = data[
                                                   f"phoneinquiries_{day}"] - data[f"idinquiries_{day}"]
            data[f"{day}d_nquiries_PI_ratio"] = data[f"phoneinquiries_{day}"] / (
                    data[f"idinquiries_{day}"] + 10e-8)
        days1 = ["3d", "7d", "14d", "21d", "30d", "60d", "90d", "180d", "360d"]
        days2 = ["7d", "14d", "21d", "30d", "60d", "90d", "180d", "360d", "total"]
        for day1, day2 in zip(days1, days2):
            data[f"phoneinquiries{day1}_to{day2}_diff"] = data[
                                                              f"phoneinquiries_{day2}"] - data[f"phoneinquiries_{day1}"]
            data[f"phoneinquiries{day1}_to{day2}_increase"] = (
                                                                      data[f"phoneinquiries_{day2}"] - data[
                                                                  f"phoneinquiries_{day1}"]
                                                              ) / (data[f"phoneinquiries_{day2}"] + 10e-8)
            data[f"idinquiries{day1}_to{day2}_diff"] = data[
                                                           f"idinquiries_{day2}"] - data[f"idinquiries_{day1}"]
            data[f"idinquiries{day1}_to{day2}_increase"] = (
                                                                   data[f"idinquiries_{day2}"] - data[
                                                               f"idinquiries_{day1}"]) / (
                                                                   data[f"idinquiries_{day2}"] + 10e-8)
        days1 = ["3d", "7d", "14d", "21d", "30d", "60d", "90d", "180d"]
        days2 = ["7d", "14d", "21d", "30d", "60d", "90d", "180d", "360d"]
        for day1, day2 in zip(days1, days2):
            data[f"B_PI{day1}_to{day2}_diff"] = data[f"B_PI_{day2}"] - data[
                f"B_PI_{day1}"]
            data[f"B_PI{day1}_to{day2}_increase"] = (
                                                            data[f"B_PI_{day2}"] -
                                                            data[f"B_PI_{day1}"]) / (data[f"B_PI_{day1}"] + 10e-8)
            data[f"A_PI{day1}_to{day2}_diff"] = data[f"A_PI_{day2}"] - data[
                f"A_PI_{day1}"]
            data[f"A_PI{day1}_to{day2}_increase"] = (
                                                            data[f"A_PI_{day2}"] -
                                                            data[f"C_PI_{day1}"]) / (data[f"C_PI_{day1}"] + 10e-8)
            data[f"C_PI{day1}_to{day2}_diff"] = data[f"C_PI_{day2}"] - data[
                f"C_PI_{day1}"]
            data[f"C_PI{day1}_to{day2}_increase"] = (
                                                            data[f"C_PI_{day2}"] -
                                                            data[f"C_PI_{day1}"]) / (data[f"C_PI_{day1}"] + 10e-8)
            data[f"B_II{day1}_to{day2}_diff"] = data[f"B_II_{day2}"] - data[
                f"B_II_{day1}"]
            data[f"B_II{day1}_to{day2}_increase"] = (
                                                            data[f"B_II_{day2}"] -
                                                            data[f"B_II_{day1}"]) / (data[f"B_II_{day1}"] + 10e-8)
            data[f"A_II{day1}_to{day2}_diff"] = data[f"A_II_{day2}"] - data[
                f"A_II_{day1}"]
            data[f"A_II{day1}_to{day2}_increase"] = (
                                                            data[f"A_II_{day2}"] -
                                                            data[f"C_II_{day1}"]) / (data[f"C_II_{day1}"] + 10e-8)
            data[f"C_II{day1}_to{day2}_diff"] = data[f"C_II_{day2}"] - data[
                f"C_II_{day1}"]
            data[f"C_II{day1}_to{day2}_increase"] = (
                                                            data[f"C_II_{day2}"] -
                                                            data[f"C_II_{day1}"]) / (data[f"C_II_{day1}"] + 10e-8)
        days2 = ["7d", "14d", "21d", "30d", "60d", "90d", "total"]
        for day in days2:
            data[f"id_multi_{day}_diff"] = data[f"idinquiriesuname_{day}"] - data[
                f"multiidinquiries_{day}"].astype(float)
            data[f"id_multi_{day}_ratio"] = data[f"idinquiriesuname_{day}"] / (
                    data[f"multiidinquiries_{day}"].astype(float) + 10e-8)
            data[f"phone_multi_{day}_diff"] = data[f"phoneinquiries_{day}"] - data[
                f"multiphoneinquiries_{day}"].astype(float)
            data[f"phone_multi_{day}_ratio"] = data[f"phoneinquiries_{day}"] / (
                    data[f"multiphoneinquiries_{day}"].astype(float) + 10e-8)
        range_list = ["0_30", "0_60", "0_90", "0_180", "0_360"]
        for range_ in range_list:
            data[f"topup_{range_}_amount"] = data[f"topup_{range_}_avg"].astype(
                float) * data[f"topup_{range_}_times"].astype(float)
            data[f"topup_{range_}_amount_min_ratio"] = data[
                                                           f"topup_{range_}_min"].astype(float) / (
                                                               data[f"topup_{range_}_amount"] + 10e-8)
            data[f"topup_{range_}_amount_max_ratio"] = data[
                                                           f"topup_{range_}_max"].astype(float) / (
                                                               data[f"topup_{range_}_amount"] + 10e-8)
        return data
        
    def func_total(self,df=None,name = None):
    
        df_sort = df.sort_values(by = ['request_date'])
        df_sort[f'{name}_apply_gap'] = (df_sort['request_date'] - df_sort['request_date'].shift(1)).apply(lambda x:x.total_seconds())
        df_sort_temp = df_sort.set_index('request_date')
        df_sort_temp['num'] = 1
        df_sort_temp['pass_num'] = [1 if i == 5 else 0 for i in df_sort_temp['loan_order_status']]
        df_sort_temp['reject_num'] = [1 if i == 6 else 0 for i in df_sort_temp['loan_order_status']]
        #还可以增加还款次数
        bins_hours = np.arange(1,13,1)
        for hour in bins_hours:
            df_sort_temp[f'{name}_before_apply_{hour}_hour'] = df_sort_temp['num'].rolling(f'{hour}H',closed = 'left').sum()
            df_sort_temp[f'{name}_pass_before_apply_{hour}_hour'] = df_sort_temp['pass_num'].rolling(f'{hour}H',closed = 'left').sum()
            df_sort_temp[f'{name}_reject_before_apply_{hour}_hour'] = df_sort_temp['reject_num'].rolling(f'{hour}H',closed = 'left').sum()
        bins_days = [1,2,3,4,5,6,7,14,21,30]
        for day in bins_days:
            df_sort_temp[f'{name}_before_apply_{day}_day'] = df_sort_temp['num'].rolling(f'{day}D',closed = 'left').sum()
            df_sort_temp[f'{name}_pass_before_apply_{day}_day'] = df_sort_temp['pass_num'].rolling(f'{day}D',closed = 'left').sum()
            df_sort_temp[f'{name}_reject_before_apply_{day}_day'] = df_sort_temp['reject_num'].rolling(f'{day}D',closed = 'left').sum()
        df_sort_new = df_sort_temp.reset_index()
        df_sort_new.drop(['num','pass_num','reject_num'],axis =1,inplace = True)
        df_sort_new.fillna(0,inplace = True)
        return df_sort_new

    def train_data(self, db="dompet_second",loan_id=None, user_id="6238fb092f654ef5a77b4d9f66421429",id_card_no='9271055712860003',):
        data = self.object_to_numric_features(db=db,loan_id=loan_id,user_id=user_id)
        data = self.add_features(data=data)
        data = data.loc[:, ~data.columns.duplicated()]
        data_timefeatures = DateTimeProcess(data['request_date']).process()
        data= pd.concat([data,data_timefeatures],axis = 1)
        return data
    


if __name__ == '__main__':
    data = Process_concat_data().train_data(user_id="0d3959ab917a44729e080512b5fc56d8")
    # print(data.boot_time)
