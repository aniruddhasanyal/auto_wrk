'''
This class abstracts several text processing tasks like cleaning and feature extraction
using several techniques.

Author: Aniruddha Sanyal
'''


import pickle
import pandas as pd
import time
import string
from tqdm import tqdm
import re
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectKBest, chi2
import numpy as np
from gensim import models, corpora, matutils
import logging
import os

class TextToStructure:
    def __init__(self, text=None, ext_stop=None, regex=None, ngram=2):
        self.text = text
        self.ext_stop = ext_stop
        self.regex = regex
        self.ngram = ngram

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

    def clean_light(self):
        '''This will create a list of cleaned up documents. Different from clean_txt below.'''

        if self.text is None:
            print('Please provide a list of text to clean')
            exit(1)

        stop_words = stopwords.words('english')
        if self.ext_stop is not None:
            stop_words.extend(self.ext_stop)
        stop_words = set(stop_words)

        print('\nStarting Text Cleanup...\n')

        start = time.time()
        clean_docs = []
        for note in tqdm(self.text):
            words = word_tokenize(note.lower().strip(string.punctuation))
            words = [self.unescape(w) for w in words if w not in stop_words and len(w) > 3]
            clean_docs.append(' '.join(words))

        print('\nCompleted cleaning in {} minutes\n'.format(round((time.time() - start) / 60)))
        print('Writing clean tokens to file: plain_clean_docs.p')
        pickle.dump(clean_docs, open("plain_clean_docs.p", "wb"))
        return clean_docs


    def clean_txt(self, verbose=False):
        '''This will create n-grams based on the value passed to the ngram parameter in the constructor, default: 2'''

        if self.text is None:
            print('Please provide a list of text to clean')
            exit(1)

        if self.regex is None: self.regex = r'(\s\W*[0-9]*[a-zA-Z]*\W*[0-9]*[a-zA-Z]*\W*)|[0-9]*'
        if verbose: print('\nStarting Text Cleanup for bi-gram Generation...\n')

        start = time.time()
        # stemmer = PorterStemmer()
        stop_words = stopwords.words('english')
        if self.ext_stop is not None:
            stop_words.extend(self.ext_stop)
        stop_words = set(stop_words)
        bi_clean_notes = []
        for note in tqdm(self.text):
            words = word_tokenize(note.lower().strip(string.punctuation))
            words = [self.unescape(w) for w in words if w not in stop_words and len(w) > 3]
            bi_clean_notes.append([' '.join([a, b]) for a, b in list(zip(*[words[i:] for i in range(self.ngram)]))])
        print('Writing clean tokens to file: clean_bigram.p')
        pickle.dump(bi_clean_notes, open("clean_bigram.p", "wb"))
        if verbose: print('\nCompleted cleaning and bi-gram generation in {} minutes'.format(round((time.time() - start) / 60)))
        return bi_clean_notes


    def get_tfidf_select_features(self, top_featrures=500):
        '''This creates a tf-idf matrix for the selected number of top features
        provided by the top_features parameter'''

        print('\nUsing sklearn for tfidf...\n')
        vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, ngram_range=(2, 3),
                                     smooth_idf=False, tokenizer=word_tokenize,
                                     stop_words='english', max_features=top_featrures)
        corpus = self.clean_light()
        tf_idf = vectorizer.fit_transform(corpus)
        print('Completed sklearn tfidf\n')
        # vocabulary_gensim = {}
        # for key, val in vectorizer.vocabulary_.items():
        #     vocabulary_gensim[val] = key
        return_obj = {'tf-idf': tf_idf,
                      'vector': vectorizer}
        pickle.dump(return_obj, open('sklearn_tfidf_dict.p', 'wb'))
        return return_obj


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


    def train_lda(self, topics=10, topic_words=10, chunksize=100, passes=2, workers=1, save_file=True,
                  verbose=True, tfidf=False, sktf=True, top_features=2000):
        '''Trains a Gensim LDA model on a) words counts b)Gensim tf-idf c)sklearn tf-idf with selected number of top 
        features indicated by the option tfidf=True for sktf=True for sklearn feature selected tf-idf and if both are 
        false, a word count option is taken. Returns a dictionary with the model, dictionary, corpus the doc topic 
        probability distribution and the topic words. '''

        start = time.time()

        # clean_tokens = self.clean_txt(verbose)

        if verbose:
            import logging
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
            tfidf_start = time.time()

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
            uid = start
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


    def get_term_freq(self, key_list=None):
        '''Accepts a list of key-phrases and returns a pandas data-frame with the frequency of the keywords in the 
        initialized document list '''

        from tqdm import tqdm
        key_list = list(key_list)
        if key_list is None:
            print('Please provide a list of key phrases to count')
            exit(1)
        key_phrase_df = pd.DataFrame(columns=key_list)
        print('Extracting keyphrase counts for keys: \n')
        print(', '.join(key_list))
        for column in tqdm(key_list):
            for i in tqdm(range(len(self.text))):
                key_phrase_df[column][i] = self.text[i].count(column)
        return key_phrase_df


def demo():
    pass

if __name__ == '__main__':
    demo()

