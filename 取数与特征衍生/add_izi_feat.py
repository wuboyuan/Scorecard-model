#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-17 16:36
# Author     : wuboyuan
# File       : add_izi_feat.py
# Desc       : 此代码用于izi数据的衍生变量


from get_data_all import *



class IziAddFeat:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    # def trams_to_numirc(self):
    #     tram_to_number = [
    #         'preference_bank_60d',
    #         'preference_bank_90d',
    #         'preference_bank_180d',
    #         'preference_bank_270d',
    #         'preference_ecommerce_60d',
    #         'preference_ecommerce_90d',
    #         'preference_ecommerce_180d',
    #         'preference_ecommerce_270d',
    #         'preference_game_60d',
    #         'preference_game_90d',
    #         'preference_game_180d',
    #         'preference_game_270d',
    #         'preference_lifestyle_60d',
    #         'preference_lifestyle_90d',
    #         'preference_lifestyle_180d',
    #         'preference_lifestyle_270d',
    #         'phonescore',
    #         'topup_0_30_avg',
    #         'topup_0_30_max',
    #         'topup_0_30_min',
    #         'topup_0_60_avg',
    #         'topup_0_60_max',
    #         'topup_0_60_min',
    #         'topup_0_90_avg',
    #         'topup_0_90_max',
    #         'topup_0_90_min',
    #         'topup_0_180_avg',
    #         'topup_0_180_max',
    #         'topup_0_180_min',
    #         'topup_0_360_avg',
    #         'topup_0_360_max',
    #         'topup_0_360_min',
    #         'topup_30_60_avg',
    #         'topup_30_60_max',
    #         'topup_30_60_min',
    #         'topup_60_90_avg',
    #         'topup_60_90_max',
    #         'topup_60_90_min',
    #         'topup_90_180_avg',
    #         'topup_90_180_max',
    #         'topup_90_180_min',
    #         'topup_180_360_avg',
    #         'topup_180_360_max',
    #         'topup_180_360_min',
    #         'topup_360_720_avg',
    #         'topup_360_720_max',
    #         'topup_360_720_min',
    #         'update_whatsapp',
    #         'multiphoneinquiries_7d',
    #         'multiphoneinquiries_14d',
    #         'multiphoneinquiries_21d',
    #         'multiphoneinquiries_30d',
    #         'multiphoneinquiries_60d',
    #         'multiphoneinquiries_90d',
    #         'multiphoneinquiries_total',
    #         'multiidinquiries_7d',
    #         'multiidinquiries_14d',
    #         'multiidinquiries_21d',
    #         'multiidinquiries_30d',
    #         'multiidinquiries_60d',
    #         'multiidinquiries_90d',
    #         'multiidinquiries_total',
    #         'b_class', 'b_membership', 'b_activeYear', 'b_activeStatus', 'b_members'
    #     ]
    #     return tram_to_number

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
if __name__ == '__main__':
    data = GetDataAll('api_loansuper_x4', 986821).get_new_izi_data()
    dr_p = ['appUserId', 'identityAddress', 'identityCity', 'identityDateOfBirth', 'identityDistrict', 'identityGender',
            'identityName', 'identityNationnality', 'identityPlaceOfBirth', 'identityProvince', 'identityVillage',
            'identityWork', 'itemCode', 'names', 'phone', 'systemUserId', 'status', 'systemUserNick', 'whatsappAvatar',
            'whatsappSignature', 'whatsappUpdatestatusTime']
    ff_izi = set(data.columns) - set(dr_p)
    data = data[ff_izi]
    data['loanId'] =986821
    izi_add_data = IziAddFeat().add_features(data).iloc[-1:]
    izi_add_data.index = [986821]
    print(izi_add_data)