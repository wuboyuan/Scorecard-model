import pandas as pd
import numpy as np
import pymysql
import time
import datetime
import joblib
import re
import difflib
import copy
import requests
import json

llit=joblib.load('llit0731.pkl')
calss_category_dict=joblib.load('calss_category_dict0731.pkl')
review_app1=joblib.load('review_app0731.pkl')
install_app1=joblib.load('install_app0731.pkl')
popular_app_dict1=joblib.load('popular_app_dict0731.pkl')
app_level1=joblib.load('app_level0731.pkl')
app_connect_feat=joblib.load('app_connect_feat.pkl')
wrong_data=joblib.load('wrong_data.pkl')

def get_requests(url, encod=True):
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

def sql_applist(db=None,loan_id=None):
    conn = pymysql.connect(
        host="",
        user="wuboyuan",
        password="",
        db=f"{db}",
        charset="utf8")

    sql=f"""
SELECT
*
from
t_app_user_applist
WHERE
loan_id="{loan_id}"
   """              
    df=pd.read_sql(sql,conn)
    conn.close()
    return df

def date_to_timestamp(date=None):
    timeArray = time.strptime(date, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp
def diff_days(date1=None, date2=None):
    return (date1 - date2) / 1000 / (3600 * 24)
def app_data_time(data=None):
    text = []
    cols = ['appName', 'firstInstallTime', 'lastUpdateTime', 'packageName']
    data["create_date_timemp"] = data["create_date"].apply(
                lambda x: date_to_timestamp(str(x)) * 1000)

    data['create_date']=data['create_date_timemp']
    data["install_to_create_days"] = data[[
        "firstInstallTime", "create_date"
    ]].apply(lambda x: diff_days(date1=x[1], date2=x[0]), axis=1)
    data["update_to_create_days"] = data[[
        "lastUpdateTime", "create_date"
    ]].apply(lambda x:diff_days(date1=x[1], date2=x[0]), axis=1)
    data["install_to_update_days"] = data[[
        "firstInstallTime", "lastUpdateTime"
    ]].apply(lambda x: diff_days(date1=x[1], date2=x[0]), axis=1)
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
    return data

def count_APP_num(data=None,llit=None):
    data_data_app=data[['category','loan_id']].groupby(by='category').count().T
    data_data_app['total_cnt']=data_data_app.T.sum()
    all_ot=set(llit)-set(data_data_app.columns)

    for i in all_ot:
        data_data_app[i]=0
    #llit2=set(data_data_app.columns)-set(['total_cnt'])
    #data_data_app_1=data_data_app[llit2]/data_data_app['total_cnt'].values[0]

    #data_data_app_1.columns=data_data_app_1.columns+['ration_total']
    #data_data_app_1=data_data_app_1.fillna(0)

    #data_data_app=pd.concat([data_data_app,data_data_app_1],axis=1)
    return data_data_app

def app_feats(data_t=None,llit=None):
    timess=['install_to_create_days','update_to_create_days']
    data_t=app_data_time(data=data_t)
    data_t[['install_to_create_days','update_to_create_days','install_to_update_days']]=data_t[['install_to_create_days','update_to_create_days','install_to_update_days']].astype(int)
    data_t["category"]=data_t['packageName'].map(calss_category_dict)
    data_t["category"] = data_t["category"].fillna("others")
    zz_data_0={}
    zz_data_0['days']={}
    zz_data_0['nmbers']={}
    zz_data_d={}
    zz_data_d1={}
    zz_data_d1_ration={}
    for q in timess:
        zz_data_0['days'][q]={}
        zz_data_0['nmbers'][q]={}
        data=data_t[data_t[q]<=3000]
        data[q]=data[q].astype(float)
        zz_data_0['days'][q]['all']=copy.deepcopy(pd.DataFrame(data[q].describe()).T)[['count','mean','std','min','max']]
        zz_data_0['days'][q]['all'].columns='all_'+q+'days_'+zz_data_0['days'][q]['all'].columns
        zz_data_0['nmbers'][q]['all']=copy.deepcopy(data[[q,'loan_id']].groupby(q).count().describe().T)[['count','mean','std','min','max']]
        zz_data_0['nmbers'][q]['all'].columns='all_'+q+'nmbers_'+zz_data_0['nmbers'][q]['all'].columns
        indd=-10
        mu=[indd]
        zz_data_d[q]={}
        zz_data_d1[q]={}
        zz_data_d1_ration[q]={}
        for j in [180,21,7]:
            data2=data[data[q]<j]
            zz_data_0['days'][q][j]=copy.deepcopy(pd.DataFrame(data2[q].describe()).T[['count','mean','std','min','max']].fillna(0))
            zz_data_0['days'][q][j].columns=str(j)+q+'_days_'+zz_data_0['days'][q][j].columns
            zz_data_0['nmbers'][q][j]=copy.deepcopy(data2[[q,'loan_id']].groupby(q).count().describe().T[['count','mean','std','min','max']].fillna(0))
            zz_data_0['nmbers'][q][j].columns=str(j)+q+'_nmbers_'+zz_data_0['nmbers'][q][j].columns
            zz_data=count_APP_num(data=data2,llit=llit)
            zz_data.columns=str(j)+q+zz_data.columns
            data1_1=pd.DataFrame(data2['packageName'].map(review_app1).fillna('review_app_others').value_counts()).T
            for i in set(list(set(review_app1.values()))+['review_app_others'])-set(data1_1.columns):
                data1_1[i]=0

            data1_2=pd.DataFrame(data2['packageName'].map(install_app1).fillna('install_app_others').value_counts()).T
            for i in set(list(set(install_app1.values()))+['install_app_others'])-set(data1_2.columns):
                data1_2[i]=0

            #data1_3=pd.DataFrame(data2['packageName'].map(popular_app_dict1).fillna('popular_app_others').value_counts()).T
           
            #for i in set(list(set(popular_app_dict1.values()))+['popular_app_others'])-set(data1_3.columns):
                #data1_3[i]=0


            data1_4=pd.DataFrame(data2['packageName'].map(app_level1).fillna('app_leve_others').value_counts()).T
            for i in set(list(set(app_level1.values()))+['app_leve_others'])-set(data1_4.columns):
                data1_4[i]=0
                
            data1_5=pd.concat([data1_1,data1_2,data1_4],axis=1)

            #or i in set(col5)-set(data1_5.columns):
                #ata1_5[i]=0
            #ata1_5=data1_5[col5]
            data1_5.columns=str(j)+q+data1_5.columns+'cnt'
            zz_data_d1[q][j]=copy.deepcopy(data1_5)


            zz_data[str(j)+q+'packageName_finance_cnt']=data2['packageName'].apply(lambda x: 1 if re.search(r'dana|cash|kredit|pinjaman|pinjam|dompet|ksp|ojk|darurat|tunai|modal|finance|cepat|keuangan|uang|loan|saku|tagihan|lender|money|uangku|kami|darurat|emas|loan|kantong|duit|rupiah|uangme|ekpressuang|pinjaman|kredi|uang|dan', x, re.I) else 0).sum()

            zz_data[str(j)+q+'packageName_gambling_cnt']=data2['packageName'].apply(lambda x: 1 if re.search(r'Mesinjudi|slot|Pertaruhan|perjudian|kasino', x, re.I) else 0).sum()

            zz_data[str(j)+q+'packageName_GPS_cnt']=data2['packageName'].apply(lambda x: 1 if re.search(r'clone|FakeGPS', x, re.I) else 0).sum()

            zz_data[str(j)+q+'alphabet_cnt']=data2['packageName'].apply(lambda x: 0 if re.search( '[A-Z]', x, re.I) else 1).sum()

            zz_data_d[q][j]=copy.deepcopy(zz_data)

            indd=j
            mu.append(j)
    t_data_1=pd.concat([zz_data_0['days']['install_to_create_days']['all'],zz_data_0['days']['install_to_create_days'][180],zz_data_0['days']['install_to_create_days'][21],zz_data_0['days']['install_to_create_days'][7]],axis=1)

    t_data_2=pd.concat([zz_data_0['days']['update_to_create_days']['all'],zz_data_0['days']['update_to_create_days'][180],zz_data_0['days']['update_to_create_days'][21],zz_data_0['days']['update_to_create_days'][7]],axis=1)

    t_data_3=pd.concat([zz_data_0['nmbers']['install_to_create_days']['all'],zz_data_0['nmbers']['install_to_create_days'][180],zz_data_0['nmbers']['install_to_create_days'][21],zz_data_0['nmbers']['install_to_create_days'][7]],axis=1)

    t_data_4=pd.concat([zz_data_0['nmbers']['update_to_create_days']['all'],zz_data_0['nmbers']['update_to_create_days'][180],zz_data_0['nmbers']['update_to_create_days'][21],zz_data_0['nmbers']['update_to_create_days'][7]],axis=1)

    t_data_5=pd.concat([zz_data_d['install_to_create_days'][180],zz_data_d['install_to_create_days'][21],zz_data_d['install_to_create_days'][7]],axis=1)

    t_data_6=pd.concat([zz_data_d['update_to_create_days'][180],zz_data_d['update_to_create_days'][21],zz_data_d['update_to_create_days'][7]],axis=1)

    t_data_7=pd.concat([zz_data_d1['install_to_create_days'][180],zz_data_d1['install_to_create_days'][21],zz_data_d1['install_to_create_days'][7]],axis=1)

    t_data_8=pd.concat([zz_data_d1['update_to_create_days'][180],zz_data_d1['update_to_create_days'][21],zz_data_d1['update_to_create_days'][7]],axis=1)
    t_data_1.index=[data_t['loan_id'].iloc[0]]
    t_data_2.index=[data_t['loan_id'].iloc[0]]
    t_data_3.index=[data_t['loan_id'].iloc[0]]
    t_data_4.index=[data_t['loan_id'].iloc[0]]
    t_data_5.index=[data_t['loan_id'].iloc[0]]
    t_data_6.index=[data_t['loan_id'].iloc[0]]
    t_data_7.index=[data_t['loan_id'].iloc[0]]
    t_data_8.index=[data_t['loan_id'].iloc[0]]
    t_data=pd.concat([t_data_1,t_data_2,t_data_3,t_data_4,t_data_5,t_data_6,t_data_7,t_data_8],axis=1)
    return t_data

company_phone=joblib.load('company_phone.pkl')

person_phone=joblib.load('person_phone.pkl')

def get_conncet(db=None,loan_id=None):
    conn = pymysql.connect(
        host="",
       
    user='wuboyuan',
    password='',
        db=f"{db}",
        charset="utf8")

    sql2 = f"""

SELECT
g.connect_json,g.loan_id,g.create_date
from
t_app_user_connections as g
WHERE
g.loan_id={loan_id}
limit
1
    """
    df = pd.read_sql(sql2, conn)
    conn.close()
    return df

def add_phone_feat(db=None,loan_id=None):
    data_phone=get_conncet(db=db,loan_id=loan_id).iloc[-1:]
    data_phone_1=pd.DataFrame(eval(data_phone['connect_json'].iloc[0]))
    data_phone_1['loan_id']=loan_id
    data_phone_1['phone']=data_phone_1['phone'].apply(lambda x:x.replace("-", "").replace(" ", ""))
    data_phone_1['phone_1']=data_phone_1['phone'].apply(lambda x : 1 if re.compile(r'((^\+628)|(^08)|(^628)|(^\+08))+([0-9]{8,11})$').match(x) else 0)
    data_phone_2=data_phone_1.drop_duplicates(subset=['loan_id','name'],keep='first')
    data_phone_3=data_phone_1.drop_duplicates(subset=['loan_id','phone'],keep='first')
    data_phone_feat={}
    data_phone_feat['phone_count_t']=len(data_phone_1)
    data_phone_feat['phone_count_valid_t']=data_phone_1['phone_1'].sum()
    data_phone_feat['phone_count_t_drop']=len(data_phone_2)
    data_phone_feat['phone_count_valid_drop']=data_phone_2['phone_1'].sum()
    data_phone_feat['phone_count_name_drop']=len(data_phone_2[['loan_id','name']].groupby('name').count())
    data_test_p=data_phone_1[['loan_id','name']].groupby('name').count()
    data_phone_feat['phone_count_name_duplicates_drop']=len(data_test_p[data_test_p['loan_id']>=2])
    data_phone_feat['phone_count_name_duplicates_max_drop']=data_test_p[data_test_p['loan_id']>=2].max().iloc[0]
    data_phone_feat['phone_count_t_p_drop']=len(data_phone_3)
    data_phone_feat['phone_count_valid_p_drop']=data_phone_3['phone_1'].sum()
    data_test_n=data_phone_1[['loan_id','phone']].groupby('phone').count()
    data_phone_feat['phone_count_name_duplicates_p_drop']=len(data_test_n[data_test_n['loan_id']>=2])
    data_phone_feat['phone_count_name_duplicates_max_p_drop']=data_test_n[data_test_n['loan_id']>=2].max().iloc[0]
    data_phone_feat['phone_count_person_phone_drop']=len(data_phone_2[data_phone_2['name'].isin(person_phone)])
    data_phone_feat['phone_count_company_phone_drop']=len(data_phone_2[data_phone_2['name'].isin(company_phone)])
    data_phone_feat['phone_count_person_phone_p_drop']=len(data_phone_3[data_phone_3['name'].isin(person_phone)])
    data_phone_feat['phone_count_company_phone_p_drop']=len(data_phone_3[data_phone_3['name'].isin(company_phone)])
    data_phone_feat_data=pd.DataFrame(data_phone_feat,index=[loan_id])
    return data_phone_feat_data

def kuan_table(db=None,loan_id=None):
    timess=['install_to_create_days','update_to_create_days']
    data_applist=sql_applist(db=db,loan_id=loan_id).iloc[-1:]

    app_data=pd.DataFrame(eval(get_requests(data_applist['applist_json'].iloc[0], encod=True)))

    app_data['create_date']=data_applist['create_date'].iloc[0]

    app_data['loan_id']=data_applist['loan_id'].iloc[0]

    app_feats_data=app_feats(data_t=app_data,llit=llit)

    add_phone_feat_data=add_phone_feat(db=db,loan_id=loan_id)

    result=app_feats_data.merge(add_phone_feat_data,how='left',right_index=True,left_index=True)
    return result

import logging
import traceback

def logger(db=None,loan_id=None, wrong_code=None,filename=None):
    logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  # 日志格式
                        datefmt='%Y-%m-%d %H:%M:%S',  # 时间格式：2018-11-12 23:50:21
                        filename=f'{filename}.log',  # 日志的输出路径
                        filemode='a')
    logging.error(f'无法获取数据,错误db:{db},user_id:{loan_id},\n报错原因:\n{wrong_code}')

def get_app_connect_feat(db=None,loan_id=None):
    try:
        data_1_i=kuan_table(db=db,loan_id=loan_id).iloc[-1:]
        data_1_i['loan_id']=loan_id
        data_1_i['merchant_code']=db
        data_1_i.index=[0]
        return data_1_i[app_connect_feat].iloc[0].to_json()
    except:
        exstr = traceback.format_exc()
        logger(db,loan_id, exstr,filename='wrong_loan')
        wrong_data['loan_id']=loan_id
        wrong_data['merchant_code']=db
        return wrong_data.iloc[0].to_json()