from kuan_table import *
from flask import Flask, jsonify
import warnings
warnings.filterwarnings("ignore")
app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def hello():
    return "hello,how are you?"


@app.route("/kuan_table_J_x/loan_id/<loan_id>", methods=["GET", "POST"])
def kuan_table_J_x(loan_id):
    loan_id=int(loan_id)
    score_dict =get_app_connect_feat(db='api_loansuper',loan_id=loan_id)
    resp = jsonify(score_dict)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp




if __name__ == '__main__':
    app.run(host='', port='')
    #nohup gunicorn -w 4 -b ****************** x1run_kuan:app &