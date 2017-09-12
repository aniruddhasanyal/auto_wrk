


import pandas as pd
import os
import pickle
import sys
from txt2struct import TextToStructure

data = None
data_filename = '/home/c10148c/auto_fraud_v5/new_trainData1015.csv'
if len(sys.argv)>1 and os.path.isfile(sys.argv[1]): data_filename = sys.argv[1]
try:
    data = pd.read_csv(data_filename)
except Exception as e:
    print('Error: {}'.format(e))
    print('Exiting Program. Please try again with a proper file name.')
    exit(1)

# data = data[:100]
# This will raise exception when no pandas dataframe is assigned to data
data.columns = [c.lower() for c in data.columns]

data = data.fillna('')
data['claimid_month_year'] = data['claimid'].astype('str') + data['month'].astype('str') + data['year'].astype('str')

w_docs = data['adjuster_notes']
w_index = data['claimid_month_year']

remove_list = ['iv', 'iviv', 'ov', 'ov1', 'ov2', 'ov3', 'ov4', 'veh1', 'veh2', 'veh', 'iv1', 'iv2', 'iv3', 'cv1', 'cv2',
               'cv', 'v2', '2010',
               'robert', '2008', '2007', '2006', '2005', '2004', '2000', '000', '2003', '2002', '2009', '2001',
               'robert', 'william',
               'adam', 'kleindickert', 'mark', 'paul', 'jeff', 'ashley', '000000', '010000', '025000', '005000', '04',
               '05', 'insured',
               'vehicle', '1', '2', '3', 'vehicle', 'claimant', 'clmnt', 'car', 'dmg', 'insd', 'insur', 'id', 'toyota',
               'dodge', 'chevi',
               'cause', 'damage', 'siu', 'special', 'investigation', 'unit', 'document', 'id', 'yes', 'x0d', '10',
               '500', '1000', 'w', 'v', '4',
               '6', 'stephen fredek', 'tawsha johnson', 'daniel grizzaffi', 'charl forsyth', 'shanna chandler',
               'theodor hoffman',
               'attach attach', 'daniel', '0', 'brenda ledbett', 'iso iso', 'actionx0d', '2500', 'diana blackwel',
               'check check', '00',
               '00 03', 'diana', 'cunningham lindsey', '24001001', '1000x0d', '2448', 'matthew kursewicz', 'stephen',
               'Fredek', 'tawsha',
               'johnson', 'grizzaffi', 'shanna', 'chandler', 'charl', 'forsyth', 'theodox', 'hoffman', 'caylor',
               'copeland', 'trey', 'dalton',
               'brenda', 'ledbett', 'alisha', 'wright', 'lynx', 'ltra', 'agener', 'blackwell', 'alicia', 'mann',
               'siebel', 'administrator',
               'odett', 'goer', 'ana', 'cana', 'fredek', 'charl', 'forsyth']

propData = TextToStructure(text=w_docs, ext_stop=remove_list)
out_dic = propData.train_lda(topics=30, topic_words=10, chunksize=10000, passes=10, workers=12,
                             tfidf=True, sktf=False, top_features=None)

pd.DataFrame(data=out_dic['topic_distribution'], index=w_index).to_csv('doc_topic_30_monthlydata.csv')

os.makedirs('auto_result_data', exist_ok=True)
pickle.dump(out_dic, open(os.path.join('auto_result_data', 'autoLDAData0106.p'), 'wb'))
print('Saved output dictionary as: auto_result_data/autoLDAData0106.p')

