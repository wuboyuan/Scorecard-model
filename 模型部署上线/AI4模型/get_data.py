from get_test_data import *
from flask import Flask, jsonify
import warnings
warnings.filterwarnings("ignore")
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def hello():
    return "hello,how are you?"
    
@app.route("/AI2_score_A_Data/user_id/<user_id>", methods=["GET", "POST"])
def AI2_score_A_Data(user_id):
    score_dict =to_predict_data( db='dompet',user_id=user_id)   
    resp = jsonify(score_dict)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp
    
@app.route("/AI2_score_B_Data/user_id/<user_id>", methods=["GET", "POST"])
def AI2_score_B_Data(user_id):
    score_dict =to_predict_data( db='dompet_second',user_id=user_id)   
    resp = jsonify(score_dict)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

if __name__ == '__main__':
    app.run(host='', port='')
    #nohup gunicorn -w 4 -b ** get_data:app &
