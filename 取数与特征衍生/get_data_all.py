#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Create time: 2021-08-16 10:20
# Author     : wuboyuan
# File       : get_data_all.py
# Desc       : 此代码用于取数

import pandas as pd
import pymysql
# import joblib


class GetDataAll:
    def __init__(self, db=None, loan_id=None):
        self.db = db
        self.loan_id = loan_id

    def get_fullinfo_data(self):   # 取得fullinfo表里的数据，该表数据基本齐全，缺乏izi的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
    SELECT
    b.*,a.repay_order_status,a.request_date
    FROM
    t_loan_order AS a
    LEFT JOIN t_app_full_link_info AS b ON a.loan_id = b.loan_id 
    WHERE
    a.loan_id="{self.loan_id}"
       """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_new_izi_data(self):  # 取得新客izi的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
               SELECT a.loan_id,a.request_date,f.*
        from
        t_loan_order as a 
        LEFT JOIN
        t_izidata_credit_feature_v2 as f on  a.loan_id=f.loan_id
        WHERE
        a.loan_id='{self.loan_id}' 
        #and 
        #a.request_date<= DATE_SUB(f.create_date,INTERVAL - 10 HOUR )
        #and
        #a.request_date>= DATE_SUB(f.create_date,INTERVAL  10 HOUR )
        and
        f.creditscore>0
        group by 
        a.loan_id
               """

        df = pd.read_sql(sql, conn)
        df["creditscore"] = df["creditscore"].astype(float)
        conn.close()
        return df

    def get_new_izi_data_j(self, merchant_code=None):  # 取的决策引擎的IZI数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
        SELECT  *  from master_risk.t_izidata_credit_feature_v2 
        where merchant_code='{merchant_code}' 
        and loan_id='{self.loan_id}'
        and creditscore>0
               """

        df = pd.read_sql(sql, conn)
        df["creditscore"] = df["creditscore"].astype(float)
        conn.close()
        return df

    def get_applist(self):  # 取得联系人APP的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
       SELECT a.loan_id,a.request_date,e.applist_json
        from
        t_loan_order as a
        LEFT JOIN
        t_app_user_applist as e on a.app_user_id=e.app_user_id
        WHERE
        a.loan_id='{self.loan_id}' 
        and
        a.request_date<= DATE_SUB(e.create_date,INTERVAL - 10 HOUR )
        and
        a.request_date>= DATE_SUB(e.create_date,INTERVAL  10 HOUR )
        group by 
        a.loan_id
               """

        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_izi_phone_v4(self, merchant_code=None):  # 取得电话IZI的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")
        sql = f"""
            SELECT
            *
            from
            t_izi_phone_multiple_v4
            WHERE
            loan_id={self.loan_id}
               """
        df = pd.read_sql(sql, conn)
        conn.close()
        if len(df) < 1:
            conn = pymysql.connect(
                host="",
                port=12345,
                user=,
                password=,
                db=self.db,
                charset="utf8")
            sql = f"""
                SELECT
                *
                from
                master_risk.t_izi_phone_multiple_v4
                WHERE
                merchant_code='{merchant_code}'
                and
                loan_id={self.loan_id}
                   """
            df = pd.read_sql(sql, conn)
            conn.close()
        return df

    def get_izi_id_v4(self, merchant_code=None):  # 取得身份证IZI的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")
        sql = f"""
               SELECT
               *
               from
               t_izi_id_mutli_dict_v4
               WHERE
               loan_id={self.loan_id}
                  """
        df = pd.read_sql(sql, conn)
        conn.close()
        if len(df) < 1:
            conn = pymysql.connect(
                host="",
                port=12345,
                user=,
                password=,
                db=self.db,
                charset="utf8")
            sql = f"""
                   SELECT
                   *
                   from
                   master_risk.t_izi_id_mutli_dict_v4
                   WHERE
                   merchant_code='{merchant_code}'
                   and
                   loan_id={self.loan_id}
                      """
            df = pd.read_sql(sql, conn)
            conn.close()
        return df

    def get_connections(self):  # 取得联系人数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
      SELECT a.loan_id,a.request_date,g.connect_json
            from
            t_loan_order as a 
            LEFT JOIN 
            t_app_user_connections as g on a.app_user_id=g.app_user_id
            WHERE
            a.loan_id='{self.loan_id}'   
            #and 
            #a.request_date<= DATE_SUB(g.create_date,INTERVAL - 10 HOUR )
            #and
            #a.request_date>= DATE_SUB(g.create_date,INTERVAL  10 HOUR )
            group by 
            a.loan_id
               """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_user_info(self):  # 取得个人信息数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
        SELECT a.loan_id,
                     a.request_date,
                     b.sex,
                     b.company_name,
                     b.company_month_income,
                     b.company_job,
                     b.company_province,
                     b.company_city,
                     b.whatsapp_account,
                     b.facebook_account,
                     b.son_number,
                     b.marry_status,
                     b.surname,
                     b.education_level,
                     b.children_total,
                     b.liveInDate,
                     b.age,
                     b.religion
        from
        t_loan_order as a 
        LEFT JOIN
        t_app_user_info as b on a.app_user_id=b.app_user_id
        WHERE
        a.loan_id='{self.loan_id}'   
        and 
        a.request_date<= DATE_SUB(b.create_date,INTERVAL - 10 HOUR )
        and
        a.request_date>= DATE_SUB(b.create_date,INTERVAL  10 HOUR )
        group by 
        a.loan_id
               """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_hidden_data(self):  # 取得埋点1的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
        SELECT a.loan_id,
                a.request_date,
                c.is_switch_pages,
                c.read_privacy_agreement_time,
                c.read_privacy_agreement_times,
                c.internet_type,
                c.boot_time,
                c.battery_power,
                c.screen_rate_long,
                c.screen_rate_width,
                c.storage_total_size,
                c.storage_available_size,
                c.sd_card_total_size,
                c.sd_card_available_size,
                from
                t_loan_order as a 
                LEFT JOIN
                t_app_user_hidden_data as c on a.app_user_id=c.app_user_id
                WHERE
                a.loan_id='{self.loan_id}'
                and 
                a.request_date<= DATE_SUB(c.crate_date,INTERVAL - 10 HOUR )
                and
                a.request_date>= DATE_SUB(c.crate_date,INTERVAL  10 HOUR )
                group by 
                a.loan_id
               """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_app_user_phone(self):  # 取得手机信息
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
            SELECT a.loan_id,
                         a.request_date,
                         d.decive_brand,
                         d.device_model,
                         d.had_change_decive,
                         d.longitude,
                         d.latitude,
                         d.user_risk_type
            from
            t_loan_order as a 
            LEFT JOIN
            t_app_user as d on a.app_user_id=d.app_user_id
            WHERE
            a.loan_id='{self.loan_id}'
            group by 
            a.loan_id
            """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_hidden_data_second(self):  # 取得埋点2的数据
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
               SELECT a.loan_id,a.request_date,f.*
        from
        t_loan_order as a 
        LEFT JOIN
        t_app_user_hidden_second_data as f on  a.app_user_id=f.app_user_id
        WHERE
        a.loan_id='{self.loan_id}' 
        and 
        a.request_date<= DATE_SUB(f.crate_date,INTERVAL - 10 HOUR )
        and
        a.request_date>= DATE_SUB(f.crate_date,INTERVAL  10 HOUR )
        group by 
        a.loan_id
               """

        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_id_data(self):  # 取得订单表信息
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql2 = f"""
    
    select a.loan_id,a.is_new,a.mobile,a.id_card_no,a.request_date,a.app_user_id,a.app_user_name
    from t_loan_order a
    where
    a.loan_id='{self.loan_id}' 
    
        """
        df = pd.read_sql(sql2, conn)
        conn.close()
        return df

    def get_multipe_loan(self, id_card_no=None):  # 取得内部多头信息
        # data_id = self.get_id_data().loc[-1:]
        # data_id_test = data_id[data_id['loan_id'] == self.loan_id]
        # id_card_no = data_id_test.id_card_no.iloc[0]
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
     SELECT
                a.loan_id,
                a.request_date,
                b.repay_date,
                a.repay_order_status,
                a.allplatfrom_user_type,
                a.item_code,
                a.id_card_no,
                a.mobile,
                a.extend_times,
                a.loan_order_status,
                a.remaining_days,
                b.sell_accounts_date,
                            b.push_cash_date,
                            a.loan_cycle,
                            a.loan_amount
            FROM
                t_loan_order a
                LEFT JOIN t_loan_order_operate b ON a.loan_id = b.loan_id 
            WHERE
                1 = 1 
                            and
                            a.id_card_no="{id_card_no}"
                        UNION
            SELECT
                loan_id,
                request_date,
                repay_date,
                1 AS 'repay_order_status',
                CAST( JSON_EXTRACT( loan_order_json, '$.allplatfromUserType' ) AS SIGNED ) AS 'allplatfrom_user_type',
                item_code,
                id_card_no,
                mobile,
                CAST( JSON_EXTRACT( loan_order_json, '$.extendTimes' ) AS SIGNED ) AS 'extend_times',
                5 AS 'loan_order_status',
                postpone_day AS 'remaining_days', 
                create_date AS 'sell_account_date' ,
                            push_cash_date,
                              CAST( JSON_EXTRACT( loan_order_json, '$.loanCycle' ) AS SIGNED ) AS 'loan_cycle',
                                CAST( JSON_EXTRACT( loan_order_json, '$.loanAmount' ) AS SIGNED ) AS 'loan_amount'
            FROM
                t_loan_order_postpone_log 
                        WHERE
                        id_card_no="{id_card_no}"
    
       """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    def get_old_izi_data(self):  # 取得老客IZI信息
        conn = pymysql.connect(
            host="",
            user=,
            password=,
            db=self.db,
            charset="utf8")

        sql = f"""
    SELECT
    b.*
    from
    (SELECT * from t_loan_order WHERE loan_id="{self.loan_id}" LIMIT 1) as a 
    LEFT JOIN
    t_izidata_credit_feature_v2 as b on a.app_user_id=b.app_user_id
    WHERE
    a.request_date>b.create_date
    and
    a.loan_id="{self.loan_id}"
    and b.creditscore>0
               """

        df = pd.read_sql(sql, conn).iloc[-1:]
        df["creditscore"] = df["creditscore"].astype(float)
        conn.close()
        return df


if __name__ == '__main__':

    data = GetDataAll('api_loansuper_x4', 978847).get_new_izi_data()
     #data = joblib.load('C:/Users/Administrator/Desktop/api_loansuper_x2.pkl')
    #data.to_csv('C:/Users/Administrator/Desktop/api_loansuper_x2.csv')
    print(data)
