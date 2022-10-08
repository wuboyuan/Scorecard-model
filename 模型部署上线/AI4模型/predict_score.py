# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from process_data import *
import numpy as np
import joblib
import time
import pymysql

class Predict:
    def __init__(self, user_id=None):
        self.user_id = user_id

    def load_model(self, path):
        return joblib.load(path)

    def prob_to_score(self, prob, base_point=450, PDO=100):
        y = np.log(prob / (1 - prob))
        return base_point + PDO / np.log(2) * (-y)

    def get_train_data(self,db='dompet_second',loan_id=None,user_id=None,id_card_no=None):
        data = Process_concat_data().train_data(db=db,loan_id=loan_id,user_id=user_id,id_card_no=id_card_no)
        return data

    def predtion(self, data=None, model_name="model_name"):
        select_ = self.load_model("app_izi_feats.pkl")
        clf_0 = self.load_model(f"{model_name}_lgb_0.pmml")
        clf_1 = self.load_model(f"{model_name}_lgb_1.pmml")
        clf_2 = self.load_model(f"{model_name}_lgb_2.pmml")
        clf_3 = self.load_model(f"{model_name}_lgb_3.pmml")
        clf_4 = self.load_model(f"{model_name}_lgb_4.pmml")
        data["pred0"] = clf_0.predict_proba(data[select_], num_iteration=clf_0.best_iteration_)[:, 1]
        data["pred1"] = clf_1.predict_proba(data[select_], num_iteration=clf_1.best_iteration_)[:, 1]
        data["pred2"] = clf_2.predict_proba(data[select_], num_iteration=clf_2.best_iteration_)[:, 1]
        data["pred3"] = clf_3.predict_proba(data[select_], num_iteration=clf_3.best_iteration_)[:, 1]
        data["pred4"] = clf_4.predict_proba(data[select_], num_iteration=clf_4.best_iteration_)[:, 1]
        data["pred_mean"] = (data["pred0"] + data[
            "pred1"] + data["pred2"] + data["pred3"] + data["pred4"]) / 5
        data["score"] = data["pred_mean"].apply(self.prob_to_score)
        return data
    def id_card_no_sql(self,db="dompet_second",loan_id=None):
        conn = pymysql.connect(
            host="",
            user="",
            password="",
            db=db,
            charset="utf8")

        sql = f"""
               SELECT
                a.id_card_no,a.app_user_id
            FROM
                t_loan_order AS a 
            WHERE
                a.loan_id = '{loan_id}' 
            ORDER BY
                a.request_date
               """

        df = pd.read_sql(sql, conn)
        df=df[-1:]
        idcar=df['id_card_no'].iloc[0]
        app_user_id=df['app_user_id'].iloc[0]
        #df["creditscore"] = df["creditscore"].astype(float)
        conn.close()
        return idcar,app_user_id

    def to_predict(self, db=None,loan_id=None, model_name=None):
        score_dict = {}
        try:
            id_card_no,app_user_id=self.id_card_no_sql(db=db,loan_id=loan_id)
            data = self.get_train_data(db=db,loan_id=loan_id,user_id=app_user_id,id_card_no=id_card_no)
            data = self.predtion(data=data, model_name=model_name)
            score_dict["score"]=data["score"].iloc[0]
            return score_dict
        except:
            score_dict["score"] = -9999
            return score_dict


if __name__ == '__main__':
    time1=time.time()
    print(Predict().to_predict(db='dompet_second',user_id="e9e3e6e6ea7c4e279d392c3dbd4ce6de",model_name="izi_app_model"))
    time2=time.time()
    cost_time=time2-time1
    print("cost_time:",cost_time)