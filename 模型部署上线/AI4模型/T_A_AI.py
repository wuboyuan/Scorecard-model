from predict_score import *
from flask import Flask, jsonify
import warnings
warnings.filterwarnings("ignore")
app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def hello():
    return "hello,how are you?"


@app.route("/T_AI2_score_A_x4/loan_id/<loan_id>", methods=["GET", "POST"])
def T_AI2_score_A_x4(loan_id):
    loan_id=int(loan_id)
    score_dict =Predict().to_predict(db='api_loansuper_x4',loan_id=loan_id,model_name="izi_app_model_T4")
    resp = jsonify(score_dict)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


if __name__ == '__main__':
    app.run(host='', port='')
    #nohup gunicorn -w 4 -b ************ T_A_AI:app &