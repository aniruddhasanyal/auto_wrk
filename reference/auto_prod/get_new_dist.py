import pickle
from gensim import models, corpora
import pandas as pd
import numpy as np
from tqdm import tqdm

data_filename = '/home/e06315e/Data/2017 DATA/AN_2017.csv'
model_out_location = 'auto_result_data/autoLDAData0106.p'

data = pd.read_csv(data_filename)

data.columns = [c.lower() for c in data.columns]

data = data.fillna('')
data['claimid_month_year'] = data['claimid'].astype('str') + data['month'].astype('str') + data['year'].astype('str')

raw_texts = data['adjuster_notes']

lda_data_all = pickle.load( open(model_out_location, 'rb' ) )

lda_model_trained = lda_data_all['model']
print('Tokenizing documents...\n')
text_tokenized = [txt.split(' ') for txt in tqdm(raw_texts)]

dic_new = corpora.Dictionary(text_tokenized)
print('Creating new corpus...\n')
corpus_new = [dic_new.doc2bow(txt) for txt in tqdm(text_tokenized)]

new_doc_topic_dist = np.array(lda_model_trained.get_document_topics(corpus_new, minimum_probability=0))[:, :, 1]
pd.DataFrame(data=new_doc_topic_dist, index=data['claimid_month_year']).to_csv('new_doc_dist.csv')

print('Generated new document distribution, saved as: new_doc_dist2017.csv')
