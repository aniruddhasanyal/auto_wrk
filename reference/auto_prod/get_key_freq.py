from txt2struct import TextToStructure
import pandas as pd
import os
import pickle
import sys

data = None
data_filename = '/home/e06315e/Data/2017 DATA/AN_2017.csv'
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

selected_keys = pd.read_csv('/home/c10148c/auto_fraud_v4/selected_keywords.csv').icol(0).tolist()

autoData = TextToStructure(text=w_docs, index=w_index, ext_stop=None)
autoData.get_term_freq(selected_keys).to_csv('key_phrases_new2017.csv')
