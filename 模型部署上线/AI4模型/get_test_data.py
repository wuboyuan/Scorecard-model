from predict_score import *


def to_predict_data( db=None,user_id=None):
    try:
        id_card_no=Predict().id_card_no_sql(db=db,user_id=user_id)
        data = Predict().get_train_data(db=db,user_id=user_id,id_card_no=id_card_no)
        return data.to_dict(orient='list')
    except:
        data = -9999
        return data