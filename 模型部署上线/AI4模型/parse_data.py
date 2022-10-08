# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import time
import datetime
import pymysql
import pandas as pd
import numpy as np
import requests
import joblib
import math

class Request_data:
    def __init__(self, user_id=None, date1=None, date2=None, date=None, url=None):
        self.user_id = user_id
        # self.date1 = date1
        # self.date2 = date2
        # self.date = date
        # self.url = url

    def strftime(self, timeStamp):
        timeStamp = timeStamp
        dateArray = datetime.datetime.utcfromtimestamp(timeStamp)
        otherStyleTime = dateArray.strftime("%Y-%m-%d")
        return otherStyleTime

    def diff_days(self, date1=None, date2=None):
        return (date1 - date2) / 1000 / (3600 * 24)

    def date_to_timestamp(self, date=None):
        timeArray = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

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

    def sql_applist(self,db="dompet", loan_id=None,user_id='bbbc94a7d8a24738a72539a37e2e4229'):
        conn = pymysql.connect(
            host="",
        user='',
        password='',
            db=db,
            charset="utf8")
        sql = """
            SELECT * FROM t_app_user_applist WHERE applist_json is not null
    
            """
        sql2 = f"""
    
SELECT
	( CASE WHEN a.remaining_days < 0 AND a.repay_order_status = 0 THEN 1 ELSE 0 END ) AS 'tar',
	b.repay_date,
	ee.*,
	a.allplatfrom_user_type,
	( CASE WHEN a.remaining_days < 0 THEN 1 ELSE 0 END ) AS 'is_first_overdue',
	d.sex,
	d.education_level,
	d.company_province,
	d.famil_province,
	d.company_month_income,
	d.begin_work_year,
	( CASE WHEN d.company_province = d.famil_province THEN 1 ELSE 0 END ) AS same_place,
	a.request_date,
	d.liveInDate,
	d.age,
	d.company_job,
	b.sell_accounts_date,
	f.device_model,
	f.decive_brand,
	DATEDIFF( a.request_date, d.liveInDate ) AS to_now_live_days,
	ff.is_switch_pages,
	ff.read_privacy_agreement_time,
	ff.read_privacy_agreement_times,
	ff.internet_type,
	ff.boot_time,
	ff.battery_power,
	ff.screen_rate_long,
	ff.screen_rate_width 
FROM
	t_loan_order AS a
	LEFT JOIN t_loan_order_operate AS b ON a.loan_id = b.loan_id
	LEFT JOIN (
	SELECT
		a.* 
	FROM
		t_app_user_applist AS a
		INNER JOIN ( SELECT MIN( a.id ) AS 'id', app_user_id FROM t_app_user_applist AS a WHERE a.loan_id = '{loan_id}' GROUP BY a.loan_id ) AS b ON a.id = b.id 
	) AS ee ON a.loan_id = ee.loan_id
	LEFT JOIN t_app_user_info AS d ON d.loan_id = a.loan_id
	LEFT JOIN ( SELECT app_user_id, device_model, decive_brand ,loan_id FROM t_app_user_device WHERE loan_id = '{loan_id}' GROUP BY loan_id ) AS f ON f.loan_id = a.loan_id
	LEFT JOIN t_app_user_hidden_data AS ff ON ff.loan_id = a.loan_id 
WHERE
	a.loan_id = '{loan_id}'
        """
        df = pd.read_sql(sql2, conn)
        #df = df.drop_duplicates("app_user_id")
        conn.close()
        return df

    def parse_applist(self, db="dompet",loan_id=None,user_id="bbbc94a7d8a24738a72539a37e2e4229"):
        """ df 为数据库抓取数据列表，url为applist存放位置"""
        df = self.sql_applist(db=db,loan_id=loan_id,user_id=user_id)
        url = df[df.app_user_id == user_id]["applist_json"].iloc[0]
        dict_ = eval(self.get_requests(url))
        text = []
        cols = ['appName', 'firstInstallTime', 'lastUpdateTime', 'packageName']
        for i in range(len(dict_)):
            test = dict_[i]
            text.append(test)
        data = pd.DataFrame(text, columns=cols)
        df = df[df.app_user_id == user_id]
        df["create_date_timemp"] = df["create_date"].apply(
            lambda x: self.date_to_timestamp(str(x)) * 1000)
        data["create_date"] = df[df.app_user_id == user_id]["create_date_timemp"].iloc[0]
        data["install_to_create_days"] = data[[
            "firstInstallTime", "create_date"
        ]].apply(lambda x: self.diff_days(date1=x[1], date2=x[0]), axis=1)
        data["update_to_create_days"] = data[[
            "lastUpdateTime", "create_date"
        ]].apply(lambda x: self.diff_days(date1=x[1], date2=x[0]), axis=1)
        data["install_to_update_days"] = data[[
            "firstInstallTime", "lastUpdateTime"
        ]].apply(lambda x: self.diff_days(date1=x[1], date2=x[0]), axis=1)
        data["system_app"] = data[["appName", "packageName"]].apply(lambda x: 1 if x[0] == x[1] else 0, axis=1)
        mean_installdate, std_installdate = np.mean(
            data["install_to_create_days"]), np.std(data["install_to_create_days"])
        mean_updatedate, std_updatedate = np.mean(
            data["update_to_create_days"]), np.std(data["update_to_create_days"])
        # print("mean_installdate,std_installdate",mean_installdate,std_installdate)
        up_limit_install = data["install_to_create_days"].quantile(0.9)
        up_limit_update = data["update_to_create_days"].quantile(0.9)
        down_limit_install = data["install_to_create_days"].quantile(0.01)
        down_limit_update = data["update_to_create_days"].quantile(0.01)
        data["install_to_create_days"] = data["install_to_create_days"].apply(
            lambda x: up_limit_install
            if x > (mean_installdate + 3 * std_installdate) else x)
        data["update_to_create_days"] = data["update_to_create_days"].apply(
            lambda x: up_limit_update
            if x > (mean_updatedate + 3 * std_updatedate) else x)
        data["appName"] = data["appName"].apply(lambda x: str(x).lower())
        data["is_update"] = data["install_to_update_days"].apply(lambda x: 1 if x > 0 else 0)
        data["app_user_id"] = user_id
        data["repay_date"] = df[df.app_user_id == user_id]["repay_date"].iloc[0]
        lnratio_dict = joblib.load('packagename_log_ratio_202102.pkl') #新增的三行，求贝叶斯概率。相应的要修改add_appfeatures.py中对应点cols
        bayes_prob = appListBayesProb(lnratio_dict,data)
        data['bayes_prob'] = bayes_prob
        cols = [
            'tar', 'item_code', 'allplatfrom_user_type', 'is_first_overdue', 'sex',
            'education_level', 'company_province', 'famil_province',
            'company_month_income', 'begin_work_year', 'same_place', 'request_date',
            'liveInDate', 'age', 'company_job', 'sell_accounts_date', 'device_model',
            'decive_brand', 'to_now_live_days', 'is_switch_pages',
            'read_privacy_agreement_time', 'read_privacy_agreement_times',
            'internet_type', 'boot_time', 'battery_power', 'screen_rate_long',
            'screen_rate_width'
        ]
        for col in cols:
            data[col] = df[col].iloc[0]
        return data
    
def appListBayesProb(logRatio,appInfo):
    # logRatio是每个app的对数好坏比的字典和好坏比的先验比值，appInfo是user_id对应的app信息dataframe格式
    app_n = len(appInfo)
    ln_ratio = appInfo['appName'].map(logRatio).sum()
    odds = math.exp(ln_ratio)*logRatio['good_bad_rate']/app_n
    prob = odds/(1+odds)
    return prob


#if __name__ == '__main__':
    #print(Request_data().parse_applist(user_id="bbbc94a7d8a24738a72539a37e2e4229").head(3))


# if __name__ == '__main__':
#     print(Request_data().parse_applist(user_id="bbbc94a7d8a24738a72539a37e2e4229").head(3))
