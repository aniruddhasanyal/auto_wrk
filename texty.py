'''
This class abstracts several text processing tasks like cleaning and feature extraction
using several techniques.

Author: Aniruddha Sanyal
'''


import os
import atexit
import pickle
from tqdm import tqdm
import pandas as pd
import numpy as np
import string
import re
import logging
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectKBest, chi2
from gensim import models, corpora, matutils
import current
from delegates import show_duration
from config import Config


class Texty:
    def __init__(self, text=None, ext_stop=None, regex=None, ngram=(1, 3)):
        self.text = text
        self.stop = set(stopwords.words('english'))
        if ext_stop: self.stop.union(set(ext_stop)).union(set(string.punctuation))
        self.current = current.Current()

    def create_out_dir(self):
        pass


    def unescape(self, s):
        '''This is a function to remove stubborn garbage in the text'''

        s = str(s)
        s = re.sub(r"<.*?>", "", s)
        s = s.replace("&lt;", "less").replace("&gt;", "greater").replace("&amp;", "and").replace("x0D", "")
        s = s.replace("\n", " ").replace("\r", " ").replace("*", " ").replace("&#;", '').replace("\t", "")
        s = s.replace("(", "").replace(")", "").replace("null,", "").replace('\'', "")
        s = re.sub('[^A-Za-z,.[space]]+', '', s)
        s = re.sub('[ \t]+', ' ', s)
        return s

    @show_duration
    def clean_txt(self, txt=None, save=True):
        '''This will create a list of cleaned up documents. Different from clean_txt below.'''

        counter = self.current.get_counter()

        if self.text is None and txt is None:
            print('Please provide a list of text to clean')
            exit(1)
        elif txt is None:
            txt = self.text

        print('\nStarting Text Cleanup...\n')

        clean_docs = []
        for note in tqdm(txt):
            words = word_tokenize(note.lower().strip(string.punctuation))
            words = [self.unescape(w) for w in words if w not in self.stop and len(w) > 3]
            clean_docs.append(' '.join(words))

        if save:
            print('Writing clean tokens to file: plain_clean_docs.p')
            pickle.dump(clean_docs, open("plain_clean_docs.p", "wb"))

        return clean_docs

    def tokenize(self, txt):
        return [self.unescape(w) for w in word_tokenize(txt.lower()) if
                w not in self.stop and len(w) > 3]

    @show_duration
    def get_tfidf_select_features(self, txt=None, top_featrures=500, ngram=(1, 3)):
        '''This creates a tf-idf matrix for the selected number of top features
        provided by the top_features parameter'''

        print('\nUsing sklearn for tfidf...\n')
        vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, ngram_range=ngram,
                                     smooth_idf=False, tokenizer=self.tokenize,
                                     stop_words='english', max_features=top_featrures)
        corpus = self.clean_txt()
        tf_idf = vectorizer.fit_transform(corpus)
        print('Completed sklearn tfidf\n')
        # vocabulary_gensim = {}
        # for key, val in vectorizer.vocabulary_.items():
        #     vocabulary_gensim[val] = key
        return_obj = {'tf-idf': tf_idf,
                      'vector': vectorizer}
        pickle.dump(return_obj, open('sklearn_tfidf_dict.p', 'wb'))
        return return_obj

    @show_duration
    def get_key_imp(self, top_feature_tfidf=2000, n_phrases=500, response=None):
        '''Returns a pandas data-frame with the key-phrases sorted by a normalized score'''

        if response is None:
            print('Please provide a response vector to obtain key-phrase importance')
            return None

        data_features = self.get_tfidf_select_features(top_feature_tfidf)
        tfidf = data_features['tf-idf']
#        tfidf = data_features['tf-idf'].toarray()
        features_names = data_features['vector'].get_feature_names()
        idf = data_features['vector'].idf_

        ch2 = SelectKBest(chi2, k=n_phrases)
        X_train = ch2.fit_transform(tfidf, response)

        top_ranked_features = sorted(enumerate(ch2.scores_), key=lambda x: x[1], reverse=True)[:n_phrases]
        top_ranked_features_indices = list(map(list, zip(*top_ranked_features)))[0]

        top_keywords_df = pd.DataFrame({
            'Feature_Name': list(features_names),
            'Chi2_Score': list(ch2.scores_),
            'P_value': list(ch2.pvalues_),
            'Tf_Idf': list(idf)
        })

        # derive weight of each word by: Chi2 X tfidf
        top_keywords_df['Normalized_Weight'] = top_keywords_df.Chi2_Score * top_keywords_df.Tf_Idf
        top_keywords_df = top_keywords_df.sort_values(by='Normalized_Weight', ascending=False)
        top_keywords_df[:n_phrases].to_csv('top_keywords.csv')
        return top_keywords_df

    @show_duration
    def train_lda(self, topics=10, topic_words=10, chunksize=100, passes=2, workers=1, save_file=True,
                  verbose=True, tfidf=False, sktf=True, top_features=2000):
        '''Trains a Gensim LDA model on a) words counts b)Gensim tf-idf c)sklearn tf-idf with selected number of top
        features indicated by the option tfidf=True for sktf=True for sklearn feature selected tf-idf and if both are
        false, a word count option is taken. Returns a dictionary with the model, dictionary, corpus the doc topic
        probability distribution and the topic words. '''




        if verbose:
            print('\n\nStarting model training | topics: {} | chunksize: {} | passes: {} | processes: {}'.format(
                topics, chunksize, passes, workers))
            logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
        else:
            logging.basicConfig(filename='training.log', format='%(asctime)s : %(levelname)s : %(message)s',
                                level=logging.INFO)

        if sktf:
            vector_out = self.get_tfidf_select_features(top_features)
            corpus = matutils.Sparse2Corpus(vector_out['tf-idf'], documents_columns=False)
            vector = vector_out['vector']
            dic = dict((v, k) for k, v in vector.vocabulary_.items())
            print('\nSuccessfully created ifidf corpus...\n')
        else:
            clean_tokens = self.clean_txt(verbose)
            os.makedirs('text')
            pickle.dump(clean_tokens, open('text/clean_text.p', 'wb'))
            dic = corpora.Dictionary(clean_tokens)
            corpus = [dic.doc2bow(txt) for txt in clean_tokens]

        os.makedirs('corpus')
        pickle.dump(dic, open('corpus/dictionary.p', 'wb'))
        pickle.dump(corpus, open('corpus/corpus.p', 'wb'))

        if tfidf:
            print('\nUsing TfIdf')


            tfidf = models.TfidfModel(corpus)
            corpus = tfidf[corpus]
            corpus.save('corpus_tfidf.model')
            # corpus_tfidf = corpora.MmCorpus.load('corpus_tfidf')
            tfidf_duration = round((time.time() - tfidf_start) / 60, 2)
            print('\nTfIdf constructed in {} minutes and saved as corpus_tfidf.model'.format(tfidf_duration))
            os.makedirs('tfidf_corpus{}'.format(tfidf_start), exist_ok=True)
            corpus.save('tfidf_corpus{}/tfidf.model'.format(tfidf_start))

        lda_model_mc = models.ldamulticore.LdaMulticore(corpus=corpus, num_topics=topics, id2word=dic,
                                                        chunksize=chunksize, passes=passes, workers=workers)
        # lda_model_mc = models.ldamodel.LdaModel(corpus=corpus, num_topics=topics, id2word=dic,
        #                                         chunksize=chunksize, passes=passes)
        if save_file:
            uid =
            os.makedirs('model{}'.format(uid), exist_ok=True)
            lda_model_mc.save('model{}/trainedLDA{}.model'.format(uid, start))

        lda_time = round((time.time() - start) / 3600, 2)
        print('\ncompleted training in {} hours'.format(lda_time))

        doc_topic_dist = np.array(lda_model_mc.get_document_topics(corpus, minimum_probability=0))[:, :, 1]

        topics = lda_model_mc.show_topics(num_topics=-1, num_words=topic_words, formatted=True)
        topic_word_prob = []
        for i, topic in topics:
            tw = []
            for e in topic.split('+'):
                tw.append(e.split('*'))
            topic_word_prob.append(tw)
        topic_word_prob = np.array(topic_word_prob)
        return_dict = {'model': lda_model_mc,
                       'dictionary': dic,
                       'corpus': corpus,
                       'topic_distribution': doc_topic_dist,
                       'topic_word_prob': topic_word_prob}
        return return_dict