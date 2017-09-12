import os
import re
import pickle
import time

import pandas as pd
import sqlalchemy

import extract_data
import config

conf = config.Config()
INPUT_DIR = conf.input_folder
OUTPUT_DIR = conf.output_folder
MODEL_LOCATION = conf.model_path

MODEL_ID = 'ARFV001'
DELIVERY_ID = 0
if not os.path.exists(os.path.join(INPUT_DIR, 'delivery_i.old')):
    DELIVERY_ID = pickle.load(open(os.path.join(INPUT_DIR, 'delivery_i.old'), 'rb'))
    DELIVERY_ID += 1
pickle.dump(DELIVERY_ID, open(os.path.join(INPUT_DIR, 'delivery_i.old')))


out_db_engine_path = "mssql+pyodbc://CommUWAdmin:5huWR?@SQLNAD-CommUW/CommUW3?driver=ODBC+Driver+13+for+SQL+Server"


def show_duration(fun):
    def time_wrap():
        start = time.time()
        fun()
        duration = int((time.time() - start) / 60)
        unit = 'Minutes'
        if duration > 60:
            duration = round((duration/60), 2)
            unit = 'Hours'
        print('\n\nCompleted process in {} {}'.format(duration, unit))
    return time_wrap()


def _format(data):
    data.rename(columns={'CLAIMID_X': 'CLAIMID'})
    data['SCORING_DATE'] = pd.to_datetime(time.strftime("%m/%d/%Y"))
    data['REPORTEDDATE'] = pd.to_datetime(data['REPORTEDDATE'])
    data['OPEN_DAYS'] = (pd.to_datetime(time.strftime("%m/%d/%Y")) - data.REPORTEDDATE).apply(lambda x: x.days)
    data['CLAIM_STATUS'] = 'Open'
    data['SIU_Previous_Classification_Status'] = ''
    data['RED_FLAGS'] = ''
    data['MODELID'] = MODEL_ID
    data['DELIVERYID'] = '{}D{}'.format(MODEL_ID, str(DELIVERY_ID))
    return data


@show_duration
def main():
    out_db_engine = sqlalchemy.create_engine(out_db_engine_path)
    table_name = 'Auto_fraud_refresh_out'

    extractor = data_extract.DataExtract()
    scoring_data = extractor.extract()

    gbm_model = pickle.load(open(os.path.join(MODEL_LOCATION, 'gbm.model'), 'rb'))
    preds = gbm_model.predict_proba(scoring_data['data'])[:, 1]

    data = scoring_data['info']
    data = _format(data)

    data['PREDICTED_PROB'] = pd.Series(preds).apply(lambda x: round(x, 4))

    # try:
    #     prev_data = pd.read_sql("select * from CommUW3.dbo.{}".format(table_name), con=out_db_engine)
    #     if len(prev_data):
    #         DELIVERY_ID = prev_data.loc[prev_data.SCORING_DATE == prev_data.SCORING_DATE.max(), 'DELIVERYID'].unique()[
    #             0]
    #         DELIVERY_ID = int(re.findall(r'[0-9]+$', DELIVERY_ID)[0])
    #         DELIVERY_ID += 1
    #         data['DELIVERYID'] = '{}D{}'.format(MODEL_ID, str(DELIVERY_ID))
    # except:
    #     data.to_sql(name=table_name, con=out_db_engine, if_exists='append', index=False)

    data.to_sql(name=table_name, con=out_db_engine, if_exists='append', index=False)

    # data.to_csv(os.path.join(OUTPUT_DIR, 'scored.csv'), index=False)
    return data


if __name__ == '__main__':
    main()
