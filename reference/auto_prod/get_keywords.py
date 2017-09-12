from txt2struct import TextToStructure
import pandas as pd
import os
import pickle
import sys

data = None
data_filename = '/home/c10148c/auto_fraud_v5/new_trainData1015.csv'
if len(sys.argv)>1 and os.path.isfile(sys.argv[1]): data_filename = sys.argv[1]
try:
    data = pd.read_csv(data_filename)
except Exception as e:
    print('Error: {}'.format(e))
    print('Exiting Program. Please try again with a proper file name.')
    exit(1)
    
data.columns = [c.lower() for c in data.columns]

data = data.fillna('')
data['claimid_month_year'] = data['claimid'].astype('str') + data['month'].astype('str') + data['year'].astype('str')

w_docs = data['adjuster_notes']
w_index = data['claimid_month_year']
w_response = data['siu_flag']

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

propData = TextToStructure(text=w_docs, index=w_index, ext_stop=remove_list)
key_phrases = propData.get_key_imp(top_feature_tfidf=None, n_phrases=500, response=w_response)

print('\nPrinting the top 50 Key Phrases, sorted by Chi sq scores....')
print(key_phrases.head(50))

