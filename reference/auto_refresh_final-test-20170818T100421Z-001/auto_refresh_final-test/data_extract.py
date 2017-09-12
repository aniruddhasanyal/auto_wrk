import re
import os
import pandas as pd
import numpy as np
import datetime
import pickle
import pyodbc

from sklearn.feature_extraction import DictVectorizer
from sklearn.preprocessing import Imputer

from config import Config

class DataExtract:
    def __init__(self):
        self.conf = Config()


    def unescape(self, s):
        '''This is a function to remove stubborn garbage in the text in a very hanwavy way
        Please feel free to optimize with proper regular expressions'''

        s = str(s)
        s = re.sub(r"<.*?>", "", s)
        s = s.replace("&lt;", "less").replace("&gt;", "greater").replace("&amp;", "and").replace("x0D", "")
        s = s.replace("\n", " ").replace("\r", " ").replace("*", " ").replace("&#;", '').replace("\t", "")
        # this has to be last:
        s = s.replace("(", "").replace(")", "").replace("null,", "").replace('\'', "")
        # next step removes all characters other than alphanumerical and space
        s = re.sub('[^A-Za-z,.[space]]+', '', s)
        s = re.sub('[ \t]+', ' ', s)
        return s


    def getFeatures(self, value, featureList, idx):
        '''I think we can get rid of this. Didnt get time to figure out how'''
        features = {}
        for f in featureList:
            features[f] = value[f][idx]
        return features
    
    
    def categorize_var(self, df, old_col, val, new_col):
        df[new_col] = 0
        df[new_col][df[old_col] == val] = 1


    def extract(self):

        INPUT_DIR = self.conf.input_folder
        OUTPUT_DIR = self.conf.output_folder
        MODEL_LOCATION = self.conf.model_path

        connection_str = self.conf.get_db_conn()
        cnxn = pyodbc.connect(connection_str)
        cursor = cnxn.cursor()

        red_flags = {'fire_flags': ['FIRE', 'EXPLOSION', 'BLAST', 'ARSON'],
                          'theft_flags': ['THEFT', 'BURGLARY', 'STEAL', 'STOLE', 'ROBBERY', 'LARCENY'],
                          'total_loss_flags': ['TOTALLOSS', 'TOTAL LOSS'],
                          'attorney_flags': ['ATTORNEY', 'LAWYER', 'COUNSEL', 'ADVOCATE', 'LITIGATION'],
                          'chiro_unst_flags': ['CHIROPRACTOR', 'PHYSIOTHERAPY', 'CHIROPRACTIC'],
                          'collision_flags': ['REA', 'REAR', 'COLL', 'COLI']}

        cursor.execute(""" select distinct claimid, Ext_MethodOfEntry into #All_Auto_Claims_base from 
        (select  claimid, Ext_MethodOfEntry, (case when PrimaryCoverage='10004' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10047' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10047' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='10047' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10048' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10049' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10049' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10049' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10050' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10050' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10051' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10051' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10051' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10052' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10053' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10054' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10057' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10058' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10060' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10060' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10061' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10063' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10064' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10064' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10064' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10064' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10064' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10065' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10065' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10066' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10066' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10066' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10067' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10067' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10069' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10071' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10071' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10078' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10078' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10081' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10082' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10085' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10087' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='10088' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10089' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10089' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10090' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10091' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10091' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10092' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10092' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10094' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10095' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10095' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10160' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10214' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10278' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10278' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10279' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10280' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10280' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10281' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10282' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10283' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10286' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10287' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10287' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10287' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10287' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10287' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10288' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10288' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10288' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10288' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10290' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10290' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10290' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10290' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10292' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10292' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10293' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10294' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10295' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10295' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10296' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10296' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10298' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10299' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10304' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='10318' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='10318' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10319' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10320' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10367' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10367' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10367' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10367' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10368' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10369' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10370' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10371' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10371' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10372' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10372' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10373' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10374' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10374' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10374' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10375' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10376' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10376' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10380' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10387' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10388' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10388' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10389' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10389' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10390' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10631' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10633' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10633' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10634' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10634' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10634' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10635' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10635' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10637' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10638' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10638' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10639' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10639' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10640' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10640' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10642' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10643' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10646' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10649' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10653' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10653' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10654' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10654' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10655' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10656' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10656' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10656' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10656' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10656' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10657' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10660' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10661' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10661' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10663' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10665' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10666' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10667' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10684' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10684' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10684' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10685' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10685' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10687' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10769' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10794' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10794' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10794' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10795' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10795' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10795' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10800' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10800' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10800' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10801' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10801' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10801' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10840' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10847' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10849' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='10895' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10895' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10896' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10896' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10896' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10903' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10905' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10981' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10982' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10982' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='10984' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10985' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10986' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10987' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10989' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='10999' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='10999' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='10999' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11000' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11000' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11000' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11001' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11002' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='11002' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11003' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='11122' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11122' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11143' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11143' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11144' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11208' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11225' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11225' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11248' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='11249' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='11258' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11258' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11264' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11265' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11265' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='11266' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11266' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11267' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11268' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11268' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11269' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11269' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11270' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11271' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11271' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='11273' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11273' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11274' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11274' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11275' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11275' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11276' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11276' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11276' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11365' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11365' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11374' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11374' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11497' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='11614' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11634' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11716' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11724' and exposuretype_desc='General'  then 'Auto'
        when PrimaryCoverage='11726' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11767' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11880' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11881' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11882' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11883' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='11884' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='11885' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='11886' and exposuretype_desc='Property'  then 'Auto'
        when PrimaryCoverage='11887' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='11889' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Bodily Injury'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Med Pay'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='PIP'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Towing and Labor'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Vehicle'  then 'Auto'
        when PrimaryCoverage='' and exposuretype_desc='Vehicle'  then 'Auto' end) as lob

        from (select claim.id claimid,
        exposure.CoverageID,
        exposure.PrimaryCoverage,
        exposure.ExposureType,
        cctl_exposuretype.[DESCRIPTION] as exposuretype_desc,
        claim.Ext_MethodOfEntry
        from GWCC_ProdCopy.dbo.cc_exposure exposure inner join GWCC_ProdCopy.dbo.cc_claim claim
        on claim.id=exposure.claimid
        left join GWCC_ProdCopy.dbo.cctl_exposuretype
        on exposure.exposuretype= GWCC_ProdCopy.dbo.cctl_exposuretype.ID
        where claim.CloseDate is null ) as base) as base2
        where lob='Auto' """)

        ALL_AUTO_CLAIMS2 = pd.read_sql(("""select * from #All_Auto_Claims_base """), cnxn)

        get_claim_details = """select distinct base_claim.CLAIMID,cc_claim.ClaimNumber,LOSSCAUSEID,
        LOSSTYPEID,LOSSLOCATIONCITY,
        LOSSLOCATIONCOUNTY,LOSSLOCATIONSTATEID,LOSSLOCATIONZIP,LOSSLOCATIONCOUNTRYID,
        cc_claim.DESCRIPTION,LITIGATIONSTATUSID,
        LITIGATIONDATE,SUBROGATIONSTATUSID,CLAIMCURRENCYID,
        CLAIMSTATEID,base_claim.REOPENDATE,CLAIMANTFIRSTNAME,CLAIMANTLASTNAME,
        CLAIMANTCOMPANYNAME,CLAIMANTTYPEID,REOPENEDREASONID,
        cc_claim.closedate as CLAIMCLOSEDATE,LASTUPDATED,CLAIMTIERID,
        base_claim.RETIRED,EXT_CLAIMSOURCESYSTEMID,
        base_claim.EXT_SOURCECLAIMNUMBER,
        EXT_VENDORNAMEID,
        JURISDICTIONSTATEID,base_claim.LOBCODE,
        CLAIMINFOID,base_claim.EXT_SOURCECATCODE,
        base_claim.AGENCYID, base_claim.EXT_POLICYSOURCESYS,ETL_ASOFDATE,CLOSEDOUTCOMEID ,base_claim.WEATHERRELATED,
        base_claim.ASSIGNEDGROUPID,base_claim.POLICYID,
        base_claim.EXT_INSUREDREPORTINGCD,LOSSLOCATIONADDRESSLINE1,
        LOSSLOCATIONADDRESSLINE2,LOSSLOCATIONADDRESSLINE3,base_claim.WEATHER,base_claim.CREATETIME,
        CLAIMANTMIDDLENAME,
        base_claim.FAULT,base_claim.FAULTRATING,
        Auto_claims.Ext_MethodOfEntry, cc_claim.reporteddate, cc_claim.lossdate, 
        datediff(day, cc_claim.LossDate, cc_claim.ReportedDate) AS Report_lag, 
        cc_claim.Ext_ResponsibleParty,
        cctl_ext_responsibleparty.[DESCRIPTION] AS responsibleparty_desc,
        cc_claim.LossLocationID,
        cc_claim.LocationOfTheft,
        cctl_locationoftheft.[DESCRIPTION] as LocationOfTheft_desc,
        cc_claim.Ext_LocationofLoss,
        cctl_ext_locationofloss.[DESCRIPTION] as ext_locationofloss_desc,
        cc_claim.LitigationStatus,
        cctl_litigationstatus.[DESCRIPTION] as litigationstatus_desc,
        cc_claim.ClaimantDenormID,
        cc_claim.InsuredDenormID,
        upper(INS.NAME) AS INSURED_NAME,
                 upper(CLM.NAME) AS CLAIMANT_NAME


        from GWCC_Datamart_ProdCopy.dbo.c_claim_d base_claim 
        inner join 
        #All_Auto_Claims_base Auto_claims
        on base_claim.CLAIMID = Auto_claims.CLAIMID
        left join 
        GWCC_Prodcopy.dbo.cc_claim cc_claim
        on base_claim.CLAIMID = cc_claim.id
        left join GWCC_ProdCopy.dbo.cctl_ext_responsibleparty cctl_ext_responsibleparty
        on cc_claim.ext_responsibleparty=cctl_ext_responsibleparty.ID

        left join GWCC_ProdCopy.dbo.cctl_locationoftheft
        on cc_claim.LocationOfTheft=GWCC_ProdCopy.dbo.cctl_locationoftheft.ID

        left join GWCC_ProdCopy.dbo.cctl_litigationstatus
        on cc_claim.litigationstatus=GWCC_ProdCopy.dbo.cctl_litigationstatus.ID
        left join GWCC_ProdCopy.dbo.cctl_ext_locationofloss
        on cc_claim.ext_locationofloss=GWCC_ProdCopy.dbo.cctl_ext_locationofloss.ID


        left join (select * from 
        GWCC_DataMart_ProdCopy.dbo.o_cc_contact
        where ETL_ISCURRENTRECORD = 1) INS
                 on cc_claim.InsuredDenormID=INS.id
                 left join (select * from 
        GWCC_DataMart_ProdCopy.dbo.o_cc_contact
        where ETL_ISCURRENTRECORD = 1) CLM
                 on cc_claim.ClaimantDenormID=CLM.id
        WHERE base_claim.ETL_ISCURRENTRECORD = 1 """
        CLAIM_BASE = pd.read_sql(get_claim_details, cnxn)

        EXPOSURE_BASE = pd.read_sql(("""select distinct base_exposure.CLAIMID as CLAIMID_exp, base_exposure.EXPOSUREID ,base_exposure.EXPOSURESTATEID ,base_exposure.
        EXPOSURETYPEID ,base_exposure.
        CLAIMORDER ,base_exposure.EXPOSURECLOSEDATE ,base_exposure.CLOSEDOUTCOMEID ,base_exposure.LOSSCATEGORYID ,base_exposure.
        LOSSPARTYID ,base_exposure.EXPOSUREREOPENDATE ,base_exposure.REOPENEDREASONID ,base_exposure.SETTLEDATE ,base_exposure.SETTLEMETHODID ,base_exposure.INJURYTYPEID ,base_exposure.
        DETAILEDINJURYTYPEID ,base_exposure.CLAIMANTFIRSTNAME ,base_exposure.CLAIMANTLASTNAME ,base_exposure.CLAIMANTCOMPANY ,
        exposure.ClaimantType as Claimanttypeid ,
        cctl_claimanttype.[DESCRIPTION]ASClaimantType_DESC ,base_exposure.
        EXPOSURETIERID ,base_exposure.CREATEDATE ,base_exposure.EXT_CLOSEDWITHOUTPAY ,base_exposure.ETL_ASOFDATE ,base_exposure.INCIDENTID ,base_exposure.
        VEHICLEID ,base_exposure.VIN   ,base_exposure.
        CLAIMANTDENORMID as CLAIMANTDENORMID_dm ,base_exposure.MAKE ,base_exposure.
        MODEL ,base_exposure.YEAR ,base_exposure.DATEVEHICLESOLD ,base_exposure.TOTALLOSS ,base_exposure.TOTALLOSSPOINTS ,base_exposure.Speed ,
        ext_methodofentry as ext_methodofentry_exp,
        exposure.ClaimantDenormID as ClaimantDenormID_exp,
        upper(con.Name) as Name_exp,

        exposure.PrimaryCoverage,
        cctl_coveragetype.[DESCRIPTION] as coveragetype_desc,
        exposure.ExposureType,
        cctl_exposuretype.[DESCRIPTION] as exposuretype_desc

        from GWCC_Datamart_ProdCopy.dbo.c_exposure_d base_exposure 
        inner join 
        #All_Auto_Claims_base Auto_claims
        on base_exposure.CLAIMID = Auto_claims.CLAIMID
        left join 
        (select * from 
        GWCC_DataMart_ProdCopy.dbo.o_cc_exposure
        where ETL_ISCURRENTRECORD = 1 ) exposure
        on base_exposure.exposureid=exposure.id
        left join GWCC_ProdCopy.dbo.cctl_claimanttype cctl_claimanttype
        on exposure.ClaimantType=cctl_claimanttype.ID

        left join GWCC_ProdCopy.dbo.cctl_exposuretype
        on exposure.exposuretype=GWCC_ProdCopy.dbo.cctl_exposuretype.ID
        left join GWCC_ProdCopy.dbo.cctl_coveragetype
        on exposure.PrimaryCoverage=GWCC_ProdCopy.dbo.cctl_coveragetype.ID
        left join (select * from  GWCC_DataMart_Prodcopy.dbo.o_cc_contact
        where ETL_ISCURRENTRECORD =1 ) con
                 on exposure.ClaimantDenormID=con.id
        where base_exposure.ETL_ISCURRENTRECORD = 1   """), cnxn)

        COV = pd.read_csv(os.path.join(INPUT_DIR, 'coverage.csv'))

        EXPOSURE_BASE_F = EXPOSURE_BASE.merge(COV, on=['PrimaryCoverage', 'exposuretype_desc'], how='left')

        CLAIM_COV = EXPOSURE_BASE_F.loc[:, ['CLAIMID_exp', 'cov_rollup_3']]
        CLAIM_COV_GRP = []
        for name, group in CLAIM_COV[['CLAIMID_exp', 'cov_rollup_3']].groupby('CLAIMID_exp', as_index=False):
            if any(g in ['Auto BI', 'PIP', 'Med Pay'] for g in group['cov_rollup_3']):
                CLAIM_COV_GRP.append({'CLAIMID_exp': name, 'BI': 1})
            else:
                CLAIM_COV_GRP.append({'CLAIMID_exp': name, 'BI': 0})
        CLAIM_COV_GRP = pd.DataFrame(CLAIM_COV_GRP)

        EXPOSURE_BASE_FINAL = EXPOSURE_BASE_F.merge(CLAIM_COV_GRP, on=['CLAIMID_exp'], how='left')

        CLAIM_BASE_FINAL = CLAIM_BASE.merge(CLAIM_COV_GRP, right_on=['CLAIMID_exp'], left_on=['CLAIMID'], how='left')
        CLAIM_BASE_FINAL = CLAIM_BASE_FINAL.drop_duplicates()

        INCIDENT_BASE_FINAL = pd.read_sql(("""select base_incident.CLAIMID ,base_incident.ID as incidentid ,base_incident.Ext_LossPartyType ,
        base_incident.VehicleID ,base_incident.VehicleLossParty ,
        base_incident.VehicleType ,base_incident.Description ,base_incident.VehiclePolStatus ,base_incident.DateSalvageAssigned ,
        base_incident.OwnerRetainingSalvage ,base_incident.Ext_DidAccidentInvolve ,base_incident.Ext_DriverNumber ,base_incident.Ext_IsVehPhysicalDamaged ,
        base_incident.Ext_VehDamageEstDetermined ,base_incident.UpdateTime ,base_incident.Subtype ,base_incident.Ext_LowImpactIncident ,base_incident.CreateTime ,
         base_incident.VehicleDriveable,base_incident.Collision,
        base_incident.Ext_RearEndCollision,
        veh_type.[DESCRIPTION] as vehicletype_desc

        from GWCC_Datamart_ProdCopy.dbo.o_cc_incident base_incident 
        inner join #All_Auto_Claims_base Auto_claims
        on base_incident.CLAIMID = Auto_claims.CLAIMID
        left join 
        GWCC_ProdCopy.dbo.cctl_vehicletype veh_type
        on base_incident.vehicletype=veh_type.id 
        where base_incident.ETL_ISCURRENTRECORD = 1  """), cnxn)

        BODY_BASE_FINAL = pd.read_sql("""select claimid as claimid, 
        coalesce(count(case when primarybodypart='10001' then claimid end),0) as Body_Head,
        coalesce(count(case when primarybodypart='10002' then claimid end),0) as Body_Neck,
        coalesce(count(case when primarybodypart='10003' then claimid end),0) as Body_Upper_ext,
        coalesce(count(case when primarybodypart='10004' then claimid end),0) as Body_Trunk,
        coalesce(count(case when primarybodypart='10005' then claimid end),0) as Body_Lower_ext,
        coalesce(count(case when primarybodypart='10006' then claimid end),0) as Body_Unknown,
        coalesce(count(case when primarybodypart='10007' then claimid end),0) as Body_Multiple

        from (select a.claimid, a.id as incidentid, b.primarybodypart
        from GWCC_DataMart_ProdCopy.dbo.o_cc_incident a left join GWCC_Prodcopy.dbo.cc_bodypart b
        on a.ID=b.IncidentID
        where a.ClaimID in (select distinct ClaimID from #All_Auto_Claims_base)
         ) a
        group by ClaimID """, cnxn)

        PAYMENT_BASE_FINAL = pd.read_sql(("""select a.claimid
        ,sum(case when b.TRANSACTIONTYPE_TYPECODE = 'Reserve' then b.TRANSACTIONAMOUNT else 0 end) as RESERVE,
        sum(case when b.TRANSACTIONTYPE_TYPECODE = 'Payment' then b.TRANSACTIONAMOUNT else 0 end) as PAYMENT

        from #All_Auto_Claims_base as a
        left join 
        GWCC_Prodcopy.dbo.cc_claim as cc_claim 
        on a.claimid = cc_claim.id
        left join 
        (select CLAIMNUMBER, ISSUEDATE, TRANSACTIONAMOUNT, TRANSACTIONTYPE_TYPECODE  from 
        GWCC_DataMart_Prodcopy.dbo.EXT_TRANSACTION_F
        WHERE TRANSACTIONTYPE_TYPECODE in ('Payment', 'Reserve') ) AS b 
        on cc_claim.CLAIMNUMBER = b.CLAIMNUMBER
        group by a.claimid
        order by a.claimid """), cnxn)

        CONTRIBFACTOR_BASE_FINAL = pd.read_sql(("""  select distinct a.claimid, a.pricontributingfactors , c.[description] as contrib_desc

        from GWCC_ProdCopy.dbo.cc_contribfactor as a 
        inner join 
        #All_Auto_Claims_base as b 
        on a.claimid = b.claimid 
        left join GWCC_Prodcopy.dbo.cctl_pricontributingfactors c
        on a.PriContributingFactors=c.ID
        order by a.claimid   """), cnxn)

        CC_POLICY_BASE_FINAL = pd.read_sql(("""  select PolicyNumber, POL.ID AS POLICYID, c.claimid, EffectiveDate, OrigEffectiveDate, ExpirationDate,PolicyType,
        ReportingDate,Ext_CustomerNumber,UnderwritingGroup,TotalVehicles

        from GWCC_ProdCopy.dbo.cc_policy pol
        inner join
        (select policyid, claimid from GWCC_ProdCopy.dbo.cc_claim as cc_claim 
        inner join #All_Auto_Claims_base as b
        on cc_claim.id = b.claimid ) as c
        on  pol.id = c.policyid
        where year(pol.effectivedate)>=1984   """), cnxn)

        CC_POLICY_ALL_FINAL = pd.read_sql(("""select distinct ID as POLICYID,POLICYNUMBER, EFFECTIVEDATE, ORIGEFFECTIVEDATE, EXPIRATIONDATE,POLICYTYPE
        from GWCC_ProdCopy.dbo.cc_policy pol
        """), cnxn)

        CC_EXPOSURE_ALL_FINAL = pd.read_sql(("""select distinct ID as EXPOSUREID, CLAIMID, CLAIMANTDENORMID AS CLAIMANTDENORMID_EXP,
        LOSSPARTY
        from GWCC_ProdCopy.dbo.cc_EXPOSURE
        """), cnxn)

        CC_CLAIM_ALL_FINAL = pd.read_sql(("""select distinct ID AS CLAIMID,CLAIMNUMBER, REPORTEDDATE, POLICYID, CLAIMANTDENORMID, INSUREDDENORMID
        from GWCC_ProdCopy.dbo.cc_claiM pol
        """), cnxn)

        CC_CONTACT_BASE_FINAL = pd.read_sql(("""
        select DISTINCT ID as contactid,
                 DateOfBirth,
                 Gender,
                 Occupation,
                 upper(Name) as Name,
        		 upper(FirstName) as FirstName,
        		 upper(LastName) as LastName,


                 primaryaddressID

               from  GWCC_Prodcopy.dbo.cc_contact con"""), cnxn)

        CC_ADDRESS_BASE_FINAL = pd.read_sql(("""select ID as addressid, AddressLine1 as contact_address, city as contact_city, 
        PostalCode as contact_postalcode, State as contact_state 
        from 
        gwcc_prodcopy.dbo.cc_address cc_address 
        inner join
        (select primaryaddressID from gwcc_prodcopy.dbo.cc_contact as  a
        inner join 
        (select distinct CLAIMANTDENORMID  from GWCC_Datamart_ProdCopy.dbo.C_EXPOSURE_D as a 
        inner join 
        #All_Auto_Claims_base as b
        on a.CLAIMID = b.claimid ) c 
        on a.ID = c.CLAIMANTDENORMID ) as d 
        on cc_address.ID = d.PrimaryAddressID """), cnxn)

        AVG_NOTES_PER_DAY = pd.read_sql(("""select a.claimid , (a.cnt_notes*1.0/b.days_open*1.0)*1.0 avg_notes_perday from 
        (select claimid, count(*) cnt_notes
           from GWCC_ProdCopy.dbo.cc_note a 
        where claimid in (select claimid from #All_Auto_Claims_base )
        and YEAR(authoringdate) = year(GETDATE()) and month(AuthoringDate) = month(GETDATE())
        group by claimid ) a 
        left join 
        (select id,  
        case when (year(reporteddate)*100+month(reporteddate)) != (year(GETDATE())*100+month(GETDATE()))
        then day(getdate()) else DATEDIFF(day, ReportedDate, getdate()) end as days_open
        from GWCC_ProdCopy.dbo.cc_claim 
        where id in (select claimid from #All_Auto_Claims_base )) b 
        on a.claimid = b.id 
        order by (a.cnt_notes/b.days_open) desc  """), cnxn)

        cursor.execute("""SELECT a.CLAIMID,  STUFF(
          (SELECT ',' + b.Body
                           FROM [GWCC_Prodcopy].[dbo].[cc_note] b       
                           WHERE a.claimID = b.claimID

                           order by claimid,authoringdate asc
                           FOR XML PATH('')),1,1,'') AS Adjuster_Notes 
                           into #AN_REFERRED
                           FROM #All_Auto_Claims_base  a""")

        UNST_DATA_PROD = pd.read_sql(("""select * from #AN_REFERRED  """), cnxn)
        UNST_DATA_PROD['Adjuster_Notes'] = UNST_DATA_PROD['Adjuster_Notes'].apply(self.unescape)

        # Converting all column names into upper case
        UNST_DATA_PROD.columns = [col.upper() for col in UNST_DATA_PROD.columns]
        CLAIM_BASE_FINAL.columns = [col.upper() for col in CLAIM_BASE_FINAL.columns]
        EXPOSURE_BASE_FINAL.columns = [col.upper() for col in EXPOSURE_BASE_FINAL.columns]
        INCIDENT_BASE_FINAL.columns = [col.upper() for col in INCIDENT_BASE_FINAL.columns]
        BODY_BASE_FINAL.columns = [col.upper() for col in BODY_BASE_FINAL.columns]
        PAYMENT_BASE_FINAL.columns = [col.upper() for col in PAYMENT_BASE_FINAL.columns]
        CONTRIBFACTOR_BASE_FINAL.columns = [col.upper() for col in CONTRIBFACTOR_BASE_FINAL.columns]
        CC_POLICY_BASE_FINAL.columns = [col.upper() for col in CC_POLICY_BASE_FINAL.columns]
        CC_CONTACT_BASE_FINAL.columns = [col.upper() for col in CC_CONTACT_BASE_FINAL.columns]
        CC_ADDRESS_BASE_FINAL.columns = [col.upper() for col in CC_ADDRESS_BASE_FINAL.columns]
        ALL_AUTO_CLAIMS2.columns = [col.upper() for col in ALL_AUTO_CLAIMS2.columns]
        COV.columns = [col.upper() for col in COV.columns]
        AVG_NOTES_PER_DAY.columns = [col.upper() for col in AVG_NOTES_PER_DAY.columns]

        UNST_DATA_PROD['ADJUSTER_NOTES'] = UNST_DATA_PROD['ADJUSTER_NOTES'].astype(str).apply(str.lower)
        UNST_DATA_PROD['FIRE_UNST_F'] = UNST_DATA_PROD['ADJUSTER_NOTES'].apply(
            lambda x: int(any(flag.lower() in x for flag in red_flags['fire_flags'])))
        UNST_DATA_PROD['THEFT_UNST_F'] = UNST_DATA_PROD['ADJUSTER_NOTES'].apply(
            lambda x: int(any(flag.lower() in x for flag in red_flags['theft_flags'])))
        UNST_DATA_PROD['TOTALLOSS_UNST_F'] = UNST_DATA_PROD['ADJUSTER_NOTES'].apply(
            lambda x: int(any(flag.lower() in x for flag in red_flags['total_loss_flags'])))
        UNST_DATA_PROD['ATTORNEY_UNST_F'] = UNST_DATA_PROD['ADJUSTER_NOTES'].apply(
            lambda x: int(any(flag.lower() in x for flag in red_flags['attorney_flags'])))
        UNST_DATA_PROD['CHIRO_UNST_F'] = UNST_DATA_PROD['ADJUSTER_NOTES'].apply(
            lambda x: int(any(flag.lower() in x for flag in red_flags['chiro_unst_flags'])))

        CLAIM_BASE_FINAL['REPORTEDDATE'] = pd.to_datetime(CLAIM_BASE_FINAL['REPORTEDDATE'])
        CLAIM_BASE_FINAL['FINAL_YR_MNTH'] = CLAIM_BASE_FINAL['REPORTEDDATE'].map(lambda X: 100 * X.year + X.month)
        CLAIM_EXP_FINAL = pd.merge(CLAIM_BASE_FINAL, EXPOSURE_BASE_FINAL, how='left', left_on='CLAIMID',
                                   right_on='CLAIMID_EXP')
        CLAIM_INC_FINAL = pd.merge(CLAIM_BASE_FINAL, INCIDENT_BASE_FINAL, how='left', left_on='CLAIMID',
                                   right_on='CLAIMID')

        CLAIM_EXP_INC_FINAL = pd.merge(CLAIM_EXP_FINAL, INCIDENT_BASE_FINAL, how='left', left_on='INCIDENTID',
                                       right_on='INCIDENTID')

        dummy_col2 = ['CLAIMID_x', 'EXPOSUREID', 'INCIDENTID', 'CLAIMANTDENORMID', 'LOSSPARTYID', 'FINAL_YR_MNTH',
                      'COV_ROLLUP_3',
                      'CLAIMANTTYPEID_y', 'VEHICLETYPE']

        AUTO_DATA_EXP_AGG = CLAIM_EXP_INC_FINAL[dummy_col2]
        AUTO_DATA_EXP_AGG = AUTO_DATA_EXP_AGG.drop_duplicates()

        denorm_unique = pd.DataFrame(AUTO_DATA_EXP_AGG[['CLAIMID_x', 'CLAIMANTTYPEID_y', 'CLAIMANTDENORMID']].groupby(
            ['CLAIMID_x', 'CLAIMANTTYPEID_y'], as_index=False)['CLAIMANTDENORMID'].apply(
            lambda x: len(x.unique()))).reset_index()
        denorm_unique['CLAIMANTTYPEID_y'] = denorm_unique['CLAIMANTTYPEID_y'].astype(int)

        denorm_unique = denorm_unique.groupby(
            ['CLAIMID_x', 'CLAIMANTTYPEID_y', 'CLAIMANTDENORMID']).count().reset_index()
        denorm_unique = denorm_unique.pivot(index='CLAIMID_x', columns='CLAIMANTTYPEID_y',
                                            values='CLAIMANTDENORMID').reset_index().fillna(0)
        cols = ['CLAIMANTDENORMID_{}'.format(col) for col in denorm_unique.columns if str(col).startswith('1')]
        cols.insert(0, 'CLAIMID_x')
        denorm_unique.columns = cols

        AUTO_DATA_EXP_AGG['INSURED_NEW_V'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE == 10003.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['TP_NEW_V'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10002.0 and x.VEHICLETYPE == 10003.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['INSURED_RENTED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE in [10005.0, 10006.0] else 0, axis=1)
        AUTO_DATA_EXP_AGG['TP_RENTED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10002.0 and x.VEHICLETYPE in [10005.0, 10006.0] else 0, axis=1)
        AUTO_DATA_EXP_AGG['INSURED_OWNED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE == 10004.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['TP_OWNED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10002.0 and x.VEHICLETYPE == 10004.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['INSURED_LISTED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE == 10002.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['TP_LISTED'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10002.0 and x.VEHICLETYPE == 10002.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['INSURED_OTHER'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE == 10009.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['TP_OTHER'] = AUTO_DATA_EXP_AGG.apply(
            lambda x: 1 if x.LOSSPARTYID == 10001.0 and x.VEHICLETYPE == 10009.0 else 0, axis=1)
        AUTO_DATA_EXP_AGG['EXPOSUREID'] = AUTO_DATA_EXP_AGG['EXPOSUREID'].astype(str)
        AUTO_DATA_EXP_AGG['INCIDENTID'] = AUTO_DATA_EXP_AGG['INCIDENTID'].astype(str)

        AUTO_DATA_EXP_AGG2 = AUTO_DATA_EXP_AGG.groupby(['CLAIMID_x'], as_index=False).agg(
            {'INSURED_NEW_V': np.sum,
             'TP_NEW_V': np.sum,
             'INSURED_RENTED': np.sum,
             'TP_RENTED': np.sum,
             'INSURED_OWNED': np.sum,
             'TP_OWNED': np.sum,
             'INSURED_LISTED': np.sum,
             'TP_LISTED': np.sum,
             'INSURED_OTHER': np.sum,
             'TP_OTHER': np.sum,
             'EXPOSUREID': pd.Series.nunique,
             'INCIDENTID': pd.Series.nunique})
        AUTO_DATA_EXP_AGG2 = AUTO_DATA_EXP_AGG2.merge(denorm_unique, on='CLAIMID_x')

        CLAIM_BASE_FINAL2 = pd.merge(CLAIM_BASE_FINAL,
                                     AUTO_DATA_EXP_AGG2[
                                         ['CLAIMID_x', 'CLAIMANTDENORMID_10001', 'CLAIMANTDENORMID_10002',
                                          'CLAIMANTDENORMID_10003', 'CLAIMANTDENORMID_10004',
                                          'CLAIMANTDENORMID_10005', 'CLAIMANTDENORMID_10006',
                                          'CLAIMANTDENORMID_10007', 'CLAIMANTDENORMID_10008',
                                          'CLAIMANTDENORMID_10009', 'CLAIMANTDENORMID_10010',
                                          'CLAIMANTDENORMID_10011', 'CLAIMANTDENORMID_10014', 'INSURED_NEW_V',
                                          'TP_NEW_V', 'INSURED_RENTED', 'TP_RENTED', 'INSURED_OWNED', 'TP_OWNED',
                                          'INSURED_LISTED', 'TP_LISTED', 'INSURED_OTHER',
                                          'TP_OTHER']].drop_duplicates(), how='left', left_on='CLAIMID',
                                     right_on='CLAIMID_x')

        # -----------------------------To be optimized using .loc----------------------------------------------------

        VEH_DATA1 = CLAIM_EXP_INC_FINAL[['CLAIMID_x', 'EXPOSUREID', 'FINAL_YR_MNTH', 'LOSSPARTYID', 'LOSSDATE', 'YEAR']]
        VEH_DATA1['LOSSDATE'] = pd.to_datetime(VEH_DATA1['LOSSDATE'])
        VEH_DATA1['V_AGE'] = VEH_DATA1['LOSSDATE'].map(lambda X: X.year) - VEH_DATA1['YEAR']
        VEH_DATA1['LOSSPARTYID'] = VEH_DATA1['LOSSPARTYID'].fillna(0)
        VEH_DATA1['FP_V_AGE'] = VEH_DATA1.apply(lambda x: x.V_AGE if int(x.LOSSPARTYID) == 10001 else 0, axis=1)
        VEH_DATA1['TP_V_AGE'] = VEH_DATA1.apply(lambda x: x.V_AGE if int(x.LOSSPARTYID) == 10002 else 0, axis=1)
        VEH_DATA2 = VEH_DATA1.groupby(['CLAIMID_x'], as_index=False).agg({'FP_V_AGE': np.max, 'TP_V_AGE': np.max})
        VEH_DATA2['FP_V_AGE'] = VEH_DATA2['FP_V_AGE'].fillna(4)
        VEH_DATA2['TP_V_AGE'] = VEH_DATA2['TP_V_AGE'].fillna(4)
        VEH_DATA2['FP_V_AGE'] = VEH_DATA2['FP_V_AGE'].apply(lambda X: 0 if X < 0 else X)
        VEH_DATA2['TP_V_AGE'] = VEH_DATA2['TP_V_AGE'].apply(lambda X: 0 if X < 0 else X)
        VEH_DATA2['FP_V_AGE'] = VEH_DATA2['FP_V_AGE'].apply(lambda X: 50 if X > 50 else X)
        VEH_DATA2['TP_V_AGE'] = VEH_DATA2['TP_V_AGE'].apply(lambda X: 50 if X > 50 else X)

        # -------------------------------------------------------------------------------------------------------------

        AUTO_FINAL_DATA = pd.merge(CLAIM_BASE_FINAL, VEH_DATA2, how='left', left_on='CLAIMID', right_on='CLAIMID_x')

        POLICY_DATA_BASE = CC_POLICY_BASE_FINAL
        POLICY_DATA_ALL = CC_POLICY_ALL_FINAL

        POLICY_DATA_ALL.columns = map(str.upper, POLICY_DATA_ALL.columns)
        POLICY_DATA_BASE.columns = map(str.upper, POLICY_DATA_BASE.columns)

        POLICY_DATA_BASE['EFFECTIVEDATE'] = pd.to_datetime(POLICY_DATA_BASE['EFFECTIVEDATE'])
        POLICY_DATA_BASE['EXPIRATIONDATE'] = pd.to_datetime(POLICY_DATA_BASE['EXPIRATIONDATE'])

        POLICY_DATA_BASE2 = pd.merge(POLICY_DATA_BASE, POLICY_DATA_ALL, on='POLICYNUMBER')

        del POLICY_DATA_BASE['CLAIMID']  # not required as we have PolicyID to join on

        CC_CONTACT_BASE_FINAL.columns = [col.upper() for col in CC_CONTACT_BASE_FINAL.columns]
        CC_ADDRESS_BASE_FINAL.columns = [col.upper() for col in CC_ADDRESS_BASE_FINAL.columns]

        CONTACT_BASE = CC_CONTACT_BASE_FINAL.copy()
        ADDRESS_BASE = CC_ADDRESS_BASE_FINAL.copy()

        AUTO_FINAL_DATA2 = pd.merge(CLAIM_EXP_INC_FINAL, CONTACT_BASE, how='left', left_on=['CLAIMANTDENORMID_EXP'],
                                    right_on=['CONTACTID'])

        AUTO_FINAL_DATA2_1 = pd.merge(AUTO_FINAL_DATA2, ADDRESS_BASE, how='left', left_on=['PRIMARYADDRESSID'],
                                      right_on=['ADDRESSID'])

        AUTO_FINAL_DATA3_LOSSLOC = AUTO_FINAL_DATA2_1[
            ['CLAIMID_x', 'LOSSPARTYID', 'LOSSLOCATIONCITY', 'CONTACT_CITY', 'LOSSLOCATIONSTATEID', 'CONTACT_STATE',
             'LOSSLOCATIONZIP', 'CONTACT_POSTALCODE']].drop_duplicates()

        AUTO_FINAL_DATA3_LOSSLOC_FP = AUTO_FINAL_DATA3_LOSSLOC[AUTO_FINAL_DATA3_LOSSLOC['LOSSPARTYID'] == 10001]
        AUTO_FINAL_DATA3_LOSSLOC_TP = AUTO_FINAL_DATA3_LOSSLOC[AUTO_FINAL_DATA3_LOSSLOC['LOSSPARTYID'] == 10002]

        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_CITY_TP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10002 and x.CONTACT_CITY == x.LOSSLOCATIONCITY) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_CITY_FP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10001 and x.CONTACT_CITY == x.LOSSLOCATIONCITY) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_CITY_FP_TP'] = AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_CITY_FP'] + \
                                                         AUTO_FINAL_DATA3_LOSSLOC[
                                                             'LOSSLOC_CITY_FP']
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_ZIP_FP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10001 and x.CONTACT_POSTALCODE == x.LOSSLOCATIONZIP) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_ZIP_TP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10002 and x.CONTACT_POSTALCODE == x.LOSSLOCATIONZIP) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_ZIP_FP_TP'] = AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_ZIP_FP'] + \
                                                        AUTO_FINAL_DATA3_LOSSLOC[
                                                            'LOSSLOC_ZIP_TP']
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_STATE_FP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10001 and x.CONTACT_STATE == x.LOSSLOCATIONSTATEID) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_STATE_TP'] = AUTO_FINAL_DATA3_LOSSLOC.apply(
            lambda x: 1 if (x.LOSSPARTYID == 10002 and x.CONTACT_STATE == x.LOSSLOCATIONSTATEID) else 0, axis=1)
        AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_STATE_FP_TP'] = AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_STATE_FP'] + \
                                                          AUTO_FINAL_DATA3_LOSSLOC['LOSSLOC_STATE_TP']

        AUTO_FINAL_DATA3_LOSSLOC_AGG = AUTO_FINAL_DATA3_LOSSLOC.groupby(['CLAIMID_x'], as_index=False).agg(
            {'LOSSLOC_CITY_FP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_CITY_TP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_CITY_FP_TP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_ZIP_FP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_ZIP_TP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_ZIP_FP_TP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_STATE_FP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_STATE_TP': lambda x: 1 if np.sum(x) else 0,
             'LOSSLOC_STATE_FP_TP': lambda x: 1 if np.sum(x) else 0})

        lossloc_col = ['CLAIMID_x', 'LOSSLOC_CITY_FP_F', 'LOSSLOC_CITY_TP_F', 'LOSSLOC_CITY_FP_TP_F',
                       'LOSSLOC_ZIP_FP_F',
                       'LOSSLOC_ZIP_TP_F', 'LOSSLOC_ZIP_FP_TP_F', 'LOSSLOC_STATE_FP_F', 'LOSSLOC_STATE_TP_F',
                       'LOSSLOC_STATE_FP_TP_F']
        AUTO_FINAL_DATA3_LOSSLOC_AGG.columns = lossloc_col

        AUTO_FINAL_DATA3_LOSSLOC_AGG2 = AUTO_FINAL_DATA3_LOSSLOC_AGG[lossloc_col]

        AUTO_FINAL_DATA3 = pd.merge(AUTO_FINAL_DATA, POLICY_DATA_BASE, how='left', on='POLICYID')
        AUTO_FINAL_DATA3['REPORTEDDATE'] = pd.to_datetime(AUTO_FINAL_DATA3['REPORTEDDATE'])
        AUTO_FINAL_DATA3['EXPIRATIONDATE'] = pd.to_datetime(AUTO_FINAL_DATA3['EXPIRATIONDATE'])
        AUTO_FINAL_DATA3['LOSSDATE'] = pd.to_datetime(AUTO_FINAL_DATA3['LOSSDATE'])
        AUTO_FINAL_DATA3['PC_ISSUE_REPORT_LAG'] = (
        AUTO_FINAL_DATA3.REPORTEDDATE - AUTO_FINAL_DATA3.EFFECTIVEDATE).astype(
            'timedelta64[D]')
        AUTO_FINAL_DATA3['PC_REPORT_EXPRN_LAG'] = (
        AUTO_FINAL_DATA3.EXPIRATIONDATE - AUTO_FINAL_DATA3.REPORTEDDATE).astype(
            'timedelta64[D]')
        AUTO_FINAL_DATA3['PC_LOSS_EXPRN_LAG'] = (AUTO_FINAL_DATA3.EXPIRATIONDATE - AUTO_FINAL_DATA3.LOSSDATE).astype(
            'timedelta64[D]')
        AUTO_FINAL_DATA3['PC_REPORT_EXPRN_LAG'] = AUTO_FINAL_DATA3['PC_REPORT_EXPRN_LAG'].fillna(0)
        AUTO_FINAL_DATA3['PC_ISSUE_REPORT_LAG'] = AUTO_FINAL_DATA3['PC_ISSUE_REPORT_LAG'].fillna(0)

        LOSS_DESC_DATA = CLAIM_BASE_FINAL[['CLAIMID', 'DESCRIPTION']].drop_duplicates()
        LOSS_DESC_DATA['DESCRIPTION'] = LOSS_DESC_DATA['DESCRIPTION'].astype(str).apply(str.upper)

        COLL_DATA2 = CLAIM_EXP_INC_FINAL[['CLAIMID_x', 'COLLISION', 'EXT_REARENDCOLLISION']].drop_duplicates()
        COLL_DATA2['COLLISION2'] = COLL_DATA2['COLLISION'] + COLL_DATA2['EXT_REARENDCOLLISION']

        COLL_DATA3 = COLL_DATA2.groupby(['CLAIMID_x'], as_index=False).agg({'COLLISION2': np.sum})
        COLL_DATA3['COLLISION_INC'] = COLL_DATA3['COLLISION2'].apply(lambda X: 1 if X else 0)

        LOSS_DESC_DATA['COLLISION_DESC'] = LOSS_DESC_DATA['DESCRIPTION'].apply(
            lambda x: int(any(flag in x for flag in red_flags['collision_flags'])))
        LOSS_DESC_DATA['THEFT_FLAG'] = LOSS_DESC_DATA['DESCRIPTION'].apply(
            lambda x: int(any(flag in x for flag in red_flags['theft_flags'])))
        LOSS_DESC_DATA['FIRE_FLAG'] = LOSS_DESC_DATA['DESCRIPTION'].apply(
            lambda x: int(any(flag in x for flag in red_flags['fire_flags'])))

        LOSS_DESC_DATA2 = pd.merge(LOSS_DESC_DATA, COLL_DATA3, how='inner', left_on='CLAIMID', right_on='CLAIMID_x')
        LOSS_DESC_DATA2['COLLISION_INC_DESC'] = LOSS_DESC_DATA2.apply(
            lambda x: int(x.COLLISION_INC or x.COLLISION_DESC), axis=1)

        TOTALLOSS_DATA = CLAIM_EXP_INC_FINAL[['CLAIMID_x', 'TOTALLOSS']].drop_duplicates()

        TOTALLOSS_DATA2 = TOTALLOSS_DATA.groupby(['CLAIMID_x'], as_index=False).agg(
            {'TOTALLOSS': lambda x: 1 if np.sum(x) else 0})

        LOSS_DESC_DATA3 = pd.merge(LOSS_DESC_DATA2, TOTALLOSS_DATA2, how='inner', on='CLAIMID_x')

        LOW_IMPACT_DATA = CLAIM_EXP_INC_FINAL[['CLAIMID_x', 'EXT_LOWIMPACTINCIDENT']].drop_duplicates()
        LOW_IMPACT_DATA['EXT_LOWIMPACTINCIDENT'] = LOW_IMPACT_DATA['EXT_LOWIMPACTINCIDENT'].fillna(0).astype(int)

        LOW_IMPACT_DATA2 = LOW_IMPACT_DATA.groupby(['CLAIMID_x'], as_index=False).agg(
            {'EXT_LOWIMPACTINCIDENT': lambda x: 1 if np.sum(x) else 0})

        LOSS_DESC_DATA4 = pd.merge(LOSS_DESC_DATA3, LOW_IMPACT_DATA2, how='inner', on='CLAIMID_x')

        VEH_DRIVE_DATA = CLAIM_EXP_INC_FINAL[['CLAIMID_x', 'VEHICLEDRIVEABLE']].drop_duplicates()
        VEH_DRIVE_DATA['VEHICLEDRIVEABLE'] = VEH_DRIVE_DATA['VEHICLEDRIVEABLE'].fillna(0).astype(int)

        VEH_DRIVE_DATA2 = VEH_DRIVE_DATA.groupby(['CLAIMID_x'], as_index=False).agg({'VEHICLEDRIVEABLE': np.sum})

        LOSS_DESC_DATA5 = pd.merge(LOSS_DESC_DATA4, VEH_DRIVE_DATA2, how='inner', on='CLAIMID_x')

        LOSS_DESC_DATA6 = LOSS_DESC_DATA5[
            ['CLAIMID_x', 'THEFT_FLAG', 'FIRE_FLAG', 'COLLISION_INC_DESC', 'TOTALLOSS', 'EXT_LOWIMPACTINCIDENT',
             'VEHICLEDRIVEABLE']]

        AUTO_FINAL_DATA7 = pd.merge(AUTO_FINAL_DATA3, LOSS_DESC_DATA6, how='left', left_on='CLAIMID_x',
                                    right_on='CLAIMID_x')

        cursor.execute("""select claimid, matchreasons
        into #iso_data
        from GWCC_ProdCopy.dbo.cc_claimisomatchreport
        where ClaimID in (select distinct ClaimID from #All_Auto_Claims_base)""")
        cursor.execute("""select distinct claimid, matchreasons into #iso_distinct
        from 
        #iso_data
        order by claimid""")
        cursor.execute("""DECLARE @inpTbl TABLE(claimid VARCHAR(100), matchreasons VARCHAR(100));
        INSERT INTO @inpTbl 
        select * from #iso_distinct;

        WITH ConvertToXMLLikeStrings AS
        (
            SELECT claimid, CAST('<x>' + REPLACE(matchreasons,',','</x><x>') + '</x>' AS XML) AS MyData
            FROM @inpTbl AS it
        )

        SELECT claimid, MyData.value('x[1]','varchar(max)') AS Val1
              ,MyData.value('x[2]','varchar(max)') AS Val2
              ,MyData.value('x[3]','varchar(max)') AS Val3
              ,MyData.value('x[4]','varchar(max)') AS Val4
              ,MyData.value('x[5]','varchar(max)') AS Val5
              ,MyData.value('x[6]','varchar(max)') AS Val6
              ,MyData.value('x[7]','varchar(max)') AS Val7

             into #iso_parsed
        FROM ConvertToXMLLikeStrings;""")
        cursor.execute("""select * into #iso_final from (
        (select claimid, val1 as matchreason from #iso_parsed where Val1 is not null)
        union
        (select claimid, Val2 from #iso_parsed where Val2 is not null)
        union
        (select claimid, Val3 from #iso_parsed where Val3 is not null)
        union
        (select claimid, Val4 from #iso_parsed where Val4 is not null)
        union
        (select claimid, Val5 from #iso_parsed where Val5 is not null)
        union
        (select claimid, Val6 from #iso_parsed where Val6 is not null)
        union
        (select claimid, Val7 from #iso_parsed where Val7 is not null)
        ) a
        order by claimid""")
        cursor.execute("""SELECT a.claimid, STUFF(
        (SELECT ',' + b.matchreason
        FROM #iso_final b      
        WHERE a.claimID = b.claimID
        FOR XML PATH('')),1,1,'') AS iso_concat
        into #concat_iso 
        FROM #iso_final AS a    
        GROUP BY a.claimid""")
        cursor.execute("""DECLARE @inpTbl1 TABLE(claimid VARCHAR(100), matchreasons VARCHAR(100));
        INSERT INTO @inpTbl1 
        select * from #iso_data;

        WITH ConvertToXMLLikeStrings AS
        (
            SELECT claimid, CAST('<x>' + REPLACE(matchreasons,',','</x><x>') + '</x>' AS XML) AS MyData
            FROM @inpTbl1 AS it
        )

        SELECT claimid, MyData.value('x[1]','varchar(max)') AS Val1
              ,MyData.value('x[2]','varchar(max)') AS Val2
              ,MyData.value('x[3]','varchar(max)') AS Val3
              ,MyData.value('x[4]','varchar(max)') AS Val4
              ,MyData.value('x[5]','varchar(max)') AS Val5
              ,MyData.value('x[6]','varchar(max)') AS Val6
              ,MyData.value('x[7]','varchar(max)') AS Val7

             into #iso_parsed_sum
        FROM ConvertToXMLLikeStrings;""")
        cursor.execute("""select * into #iso_row
        from 
        (SELECT row_number() over( order by claimid) as row, claimid, val1, val2, val3, val4, val5, val6, val7
        from #iso_parsed_sum) a""")
        cursor.execute("""select distinct row, claimid
        into #iso_row_distinct
        from #iso_row""")
        cursor.execute("""select * into #iso_final_sum from (
        (select row, val1 as matchreason from #iso_row where Val1 is not null)
        union
        (select row, Val2 from #iso_row where Val2 is not null)
        union
        (select row, Val3 from #iso_row where Val3 is not null)
        union
        (select row, Val4 from #iso_row where Val4 is not null)
        union
        (select row, Val5 from #iso_row where Val5 is not null)
        union
        (select row, Val6 from #iso_row where Val6 is not null)
        union
        (select row, Val7 from #iso_row where Val7 is not null)
        ) a
        order by row""")
        cursor.execute("""select a.row, b.claimid, a.matchreason
        into #iso_sum_data
        from #iso_final_sum a left join #iso_row_distinct b
        on a.row=b.row""")
        cursor.execute("""select claimid, (case when matchreason='A' then 1 else 0 end) as ISO_address,
        (case when matchreason='D' then 1 else 0 end) as ISO_DL,
        (case when matchreason='L' then 1 else 0 end) as ISO_Licenseplatenumber,
        (case when matchreason='LL' then 1 else 0 end) as ISO_Losslocation,
        (case when matchreason='N' then 1 else 0 end) as ISO_Name,
        (case when matchreason='P' then 1 else 0 end) as ISO_Phone,
        (case when matchreason='S' then 1 else 0 end) as ISO_SSN,
        (case when matchreason='V' then 1 else 0 end) as ISO_VIN
        into #iso_flag_sum
        from #iso_sum_data""")
        cursor.execute("""select claimid, 
        SUM(ISO_address) as ISO_address,
        SUM(iso_dl) as iso_dl,
        SUM(ISO_Licenseplatenumber) as ISO_Licenseplatenumber,
        SUM(ISO_Losslocation) as ISO_Losslocation,
        SUM(ISO_Name) as ISO_Name,
        SUM(ISO_Phone) as ISO_Phone,
        SUM(ISO_SSN) as ISO_SSN,
        SUM(ISO_VIN) as ISO_VIN
        into #iso_matchreason_f
        from #iso_flag_sum
        group by claimid""")

        ISO_DATA = pd.read_sql(("""select * from #iso_matchreason_f """), cnxn)

        ISO_DATA.columns = [col.upper() for col in ISO_DATA.columns]

        AUTO_FINAL_DATA10 = pd.merge(AUTO_FINAL_DATA7, ISO_DATA, how='left', on='CLAIMID')

        AUTO_FINAL_DATA12 = pd.merge(AUTO_FINAL_DATA10, PAYMENT_BASE_FINAL, how='left', on=['CLAIMID'])

        cursor.execute("""select ClaimID, PriContributingFactors INTO #CONTRIB 
        from GWCC_ProdCopy.dbo.cc_contribfactor
        WHERE ClaimID IN (SELECT DISTINCT ClaimID FROM #All_Auto_Claims_base)""")
        cursor.execute("""select claimid, (case when PriContributingFactors in ('10077','10078','10079') then 1 else 0 end) as glass_damage
        ,
        (case when PriContributingFactors in ('10153','10024') then 1 else 0 end) as rear_end_pc ,
        (case when PriContributingFactors in ('10094') then 1 else 0 end) as intersection_accident ,
        (case when PriContributingFactors in ('10083','10154','10169') then 1 else 0 end) as speed_headon_racing ,
        (case when PriContributingFactors in ('10141') then 1 else 0 end) as parked_vehicle ,
        (case when PriContributingFactors in ('10069') then 1 else 0 end) as following_closely 
        into #contri_factor
        from #CONTRIB""")
        cursor.execute("""select ClaimID,
        sum(glass_damage) as glass_damage,
                 sum(rear_end_pc) as rear_end_pc,
                 sum(intersection_accident) as intersection_accident,
                 sum(speed_headon_racing) as speed_headon_racing,
                 sum(parked_vehicle) as parked_vehicle,
                 sum(following_closely) as following_closely
                 into #claim_contri_1
                 from #contri_factor
                 group by ClaimID""")
        cursor.execute("""select ClaimID,
        (case when glass_damage=0 then 0 else 1 end) as GLASS_DAMAGE_F,
        (case when rear_end_pc=0 then 0 else 1 end) as REAR_END_PC_F,
        (case when intersection_accident=0 then 0 else 1 end) as INTERSECTION_PC1,
        (case when speed_headon_racing=0 then 0 else 1 end) as SPEED_HEADON_PC_F,
        (case when parked_vehicle=0 then 0 else 1 end) as PARKED_VEHICLE,
        (case when following_closely=0 then 0 else 1 end) as FOLL_CLOSELY_PC1
                 into #claim_contri
                 from #claim_contri_1""")

        CONTRIBFACTOR_17 = pd.read_sql(("""select * from #claim_contri  """), cnxn)
        CONTRIBFACTOR_17.columns = [col.upper() for col in CONTRIBFACTOR_17.columns]

        AUTO_FINAL_DATA15 = pd.merge(AUTO_FINAL_DATA12, CONTRIBFACTOR_17, how='left', on=['CLAIMID'])

        AUTO_FINAL_DATA16 = pd.merge(AUTO_FINAL_DATA15, BODY_BASE_FINAL, how='left', on='CLAIMID')

        AUTO_FINAL_DATA17 = pd.merge(AUTO_FINAL_DATA16, AUTO_FINAL_DATA3_LOSSLOC_AGG2, how='left', left_on='CLAIMID',
                                     right_on='CLAIMID_x')
        AUTO_FINAL_DATA17 = AUTO_FINAL_DATA17.rename(columns={'CLAIMID_x_x': 'CLAIMID_x'})

        AVG_NOTES_PER_DAY.columns = [col.upper() for col in AVG_NOTES_PER_DAY.columns]

        AUTO_FINAL_DATA18 = pd.merge(AUTO_FINAL_DATA17, AVG_NOTES_PER_DAY, how='left', left_on='CLAIMID',
                                     right_on='CLAIMID')

        CC_CLAIM_EXPOSURE = pd.merge(CC_CLAIM_ALL_FINAL, CC_EXPOSURE_ALL_FINAL, how='left', on='CLAIMID')

        CC_CLAIM_EXPOSURE_POLICY = pd.merge(CC_CLAIM_EXPOSURE, CC_POLICY_ALL_FINAL, how='left', on='POLICYID')

        CC_CLAIM_EXPOSURE_POLICY_CON_1 = pd.merge(CC_CLAIM_EXPOSURE_POLICY,
                                                  CC_CONTACT_BASE_FINAL[['CONTACTID', 'NAME']],
                                                  how='left', left_on='CLAIMANTDENORMID_EXP', right_on='CONTACTID')

        CC_CLAIM_EXPOSURE_POLICY_CON_2 = pd.merge(CC_CLAIM_EXPOSURE_POLICY_CON_1,
                                                  CC_CONTACT_BASE_FINAL[['CONTACTID', 'NAME']],
                                                  how='left', left_on='CLAIMANTDENORMID', right_on='CONTACTID')

        CC_CLAIM_EXPOSURE_POLICY_CON_3 = pd.merge(CC_CLAIM_EXPOSURE_POLICY_CON_2,
                                                  CC_CONTACT_BASE_FINAL[['CONTACTID', 'NAME']],
                                                  how='left', left_on='INSUREDDENORMID', right_on='CONTACTID')
        CC_CLAIM_EXPOSURE_POLICY_CON_3.columns = ['CLAIMID', 'CLAIMNUMBER', 'REPORTEDDATE', 'POLICYID',
                                                  'CLAIMANTDENORMID', 'INSUREDDENORMID', 'EXPOSUREID',
                                                  'CLAIMANTDENORMID_EXP', 'LOSSPARTY', 'POLICYNUMBER', 'EFFECTIVEDATE',
                                                  'ORIGEFFECTIVEDATE', 'EXPIRATIONDATE', 'POLICYTYPE', 'CONTACTID_x',
                                                  'NAME_EXP', 'CONTACTID_y', 'CLAIMANTNAME', 'CONTACTID', 'INSUREDNAME']

        CC_CLAIM_EXPOSURE_POLICY_CONTACT = CC_CLAIM_EXPOSURE_POLICY_CON_3[['EXPOSUREID', 'CLAIMID', 'LOSSPARTY',
                                                                           'CLAIMNUMBER', 'REPORTEDDATE', 'POLICYID',
                                                                           'CLAIMANTDENORMID_EXP', 'CONTACTID_x',
                                                                           'NAME_EXP',
                                                                           'CLAIMANTDENORMID', 'CONTACTID_y',
                                                                           'CLAIMANTNAME',
                                                                           'INSUREDDENORMID',
                                                                           'CONTACTID', 'INSUREDNAME', 'POLICYNUMBER',
                                                                           'EFFECTIVEDATE', 'ORIGEFFECTIVEDATE',
                                                                           'EXPIRATIONDATE', 'POLICYTYPE']]
        CC_CLAIM_EXPOSURE_POLICY_CONTACT[['NAME_EXP', 'CLAIMANTNAME', 'INSUREDNAME']] = \
        CC_CLAIM_EXPOSURE_POLICY_CONTACT[['NAME_EXP', 'CLAIMANTNAME', 'INSUREDNAME']].astype(str)
        CC_CLAIM_EXPOSURE_POLICY_CONTACT.replace('None', np.NaN, inplace=True)

        CC_CLAIM_EXPOSURE_POLICY_CONTACT['LOSSPARTY'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT['LOSSPARTY'].fillna(0)

        CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.CLAIMANTNAME if (x.NAME_EXP is None and x.CLAIMANTNAME is not None) else x.INSUREDNAME if (
                x.NAME_EXP is None and x.CLAIMANTNAME is None and int(x.LOSSPARTY) == 10001) else x.NAME_EXP, axis=1)

        CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.CLAIMANTNAME if (
                x.NAME_EXP is None and x.CLAIMANTNAME is not None and int(x.LOSSPARTY) == 10002) else x.INSUREDNAME if (
                x.NAME_EXP is None and x.CLAIMANTNAME is None and x.INSUREDNAME is not None and int(
                    x.LOSSPARTY) == 10001) else x.NAME_EXP, axis=1)

        CC_CLAIM_EXPOSURE_POLICY_CONTACT['INSUREDNAME_F'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.CLAIMANTNAME if (
                x.NAME_EXP is None and x.CLAIMANTNAME is not None and int(
                    x.LOSSPARTY) == 10001 and x.INSUREDNAME is None) else x.NAME_EXP if (
                x.NAME_EXP is not None and x.INSUREDNAME is None and int(
                    x.LOSSPARTY) == 10001) else x.INSUREDNAME, axis=1)
        CC_CLAIM_EXPOSURE_POLICY_CONTACT['CLAIMANTNAME_F'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.NAME_EXP if int(x.LOSSPARTY) == 10002 and x.NAME_EXP is not None else x.CLAIMANTNAME, axis=1)

        CC_CLAIM_EXPOSURE_POLICY_CONTACT.columns = ['EXPOSUREID_N', 'CLAIMID_N', 'LOSSPARTY_N', 'CLAIMNUMBER_N',
                                                    'REPORTEDDATE_N',
                                                    'POLICYID_N', 'CLAIMANTDENORMID_EXP_N', 'CONTACTID_x_N',
                                                    'NAME_EXP_N',
                                                    'CLAIMANTDENORMID_N', 'CONTACTID_y_N', 'CLAIMANTNAME_N',
                                                    'INSUREDDENORMID_N',
                                                    'CONTACTID_N', 'INSUREDNAME_N', 'POLICYNUMBER_N', 'EFFECTIVEDATE_N',
                                                    'ORIGEFFECTIVEDATE_N', 'EXPIRATIONDATE_N', 'POLICYTYPE_N',
                                                    'FINALNAME_N',
                                                    'FINALNAME_1_N', 'INSUREDNAME_F_N', 'CLAIMANTNAME_F_N']

        CLAIM_EXP_INC_FINAL_CON = pd.merge(CLAIM_EXP_INC_FINAL, CC_CLAIM_EXPOSURE_POLICY_CONTACT, how='left',
                                           left_on=['CLAIMID_x', 'EXPOSUREID'], right_on=['CLAIMID_N', 'EXPOSUREID_N'])

        NAME_CLAIM = CC_CLAIM_EXPOSURE_POLICY_CONTACT[CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1_N'].notnull()][
            ['CLAIMID_N', 'FINALNAME_1_N']].drop_duplicates()

        PREV_CLAIMS_CLM1 = pd.merge(CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'CLAIMANTNAME_F_N']].drop_duplicates(),
                                    NAME_CLAIM,
                                    how='left', left_on='CLAIMANTNAME_F_N', right_on='FINALNAME_1_N')
        PREV_CLAIMS_CLM3 = PREV_CLAIMS_CLM1.groupby(['CLAIMID_x', 'CLAIMANTNAME_F_N'], as_index=False).CLAIMID_N.count()

        PREV_CLAIMS_1 = pd.merge(CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'FINALNAME_1_N']].drop_duplicates(), NAME_CLAIM,
                                 how='left', on='FINALNAME_1_N')

        PREV_CLAIMS_3 = PREV_CLAIMS_1.groupby(['CLAIMID_x', 'FINALNAME_1_N'], as_index=False).CLAIMID_N.count()

        CON_CLM_1 = CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'CLAIMANTNAME_F_N', 'CLAIMANTDENORMID_EXP_N']]

        CON_CLM_2 = pd.merge(CON_CLM_1, PREV_CLAIMS_CLM3, how='left', on=['CLAIMID_x', 'CLAIMANTNAME_F_N'])
        CON_CLM_2.columns = ['CLAIMID', 'CLAIMANTNAME_F_N', 'CLAIMANTDENORMID_EXP_N', 'CLAIM_CNT_NAME_CLM']

        CON_EXP_1 = CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'FINALNAME_1_N', 'CLAIMANTDENORMID_EXP_N']]

        CON_EXP_2 = pd.merge(CON_EXP_1, PREV_CLAIMS_3, how='left', on=['CLAIMID_x', 'FINALNAME_1_N'])

        CON_INS_1 = CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'INSUREDNAME_F_N', 'CLAIMANTDENORMID_EXP_N']]

        PREV_CLAIMS_INS1 = pd.merge(CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'INSUREDNAME_F_N']].drop_duplicates(),
                                    NAME_CLAIM,
                                    how='left', left_on='INSUREDNAME_F_N', right_on='FINALNAME_1_N')

        PREV_CLAIMS_INS3 = PREV_CLAIMS_INS1.groupby(['CLAIMID_x', 'INSUREDNAME_F_N'], as_index=False).CLAIMID_N.count()

        CON_EXP_2.columns = ['CLAIMID', 'FINALNAME_1_N', 'CLAIMANTDENORMID_EXP_N', 'CLAIM_CNT_NAME_EXP']

        CON_INS_2 = pd.merge(CON_INS_1, PREV_CLAIMS_INS3, how='left', on=['CLAIMID_x', 'INSUREDNAME_F_N'])
        CON_INS_2.columns = ['CLAIMID', 'INSUREDNAME_F_N', 'CLAIMANTDENORMID_EXP_N', 'CLAIM_CNT_NAME_INS']

        CON_EXP_2['CLAIM_CNT_NAME_EXP'] = CON_EXP_2['CLAIM_CNT_NAME_EXP'].fillna(1)
        CON_EXP_2['RANK1'] = CON_EXP_2.sort_values(['CLAIM_CNT_NAME_EXP', 'FINALNAME_1_N'], ascending=[0, 0]).groupby(
            ['CLAIMID']).cumcount() + 1

        CON_INS_2['CLAIM_CNT_NAME_INS'] = CON_INS_2['CLAIM_CNT_NAME_INS'].fillna(1)
        CON_INS_2['RANK1'] = CON_INS_2.sort_values(['CLAIM_CNT_NAME_INS', 'INSUREDNAME_F_N'], ascending=[0, 0]).groupby(
            ['CLAIMID']).cumcount() + 1

        CON_CLM_2['CLAIM_CNT_NAME_CLM'] = CON_CLM_2['CLAIM_CNT_NAME_CLM'].fillna(1)
        CON_CLM_2['RANK1'] = CON_CLM_2.sort_values(['CLAIM_CNT_NAME_CLM', 'CLAIMANTNAME_F_N'],
                                                   ascending=[0, 0]).groupby(
            ['CLAIMID']).cumcount() + 1

        CON_EXP_3 = CON_EXP_2[CON_EXP_2['RANK1'] == 1]
        CON_INS_3 = CON_INS_2[CON_INS_2['RANK1'] == 1]
        CON_CLM_3 = CON_CLM_2[CON_CLM_2['RANK1'] == 1]

        CON_CLM_4 = CON_CLM_3[['CLAIMID', 'CLAIM_CNT_NAME_CLM']]
        CON_INS_4 = CON_INS_3[['CLAIMID', 'CLAIM_CNT_NAME_INS']]

        CON_CLM_INS_4 = pd.merge(CON_CLM_4, CON_INS_4, how='left', on=['CLAIMID'])

        AUTO_FINAL_DATA19 = pd.merge(AUTO_FINAL_DATA18, CON_CLM_INS_4, how='left', on=['CLAIMID'])

        PREV_RENEWALS = CC_CLAIM_EXPOSURE_POLICY_CONTACT.groupby("POLICYNUMBER_N", as_index=False).agg(
            {"EFFECTIVEDATE_N": pd.Series.nunique})

        PREV_POL_CLAIMS = CC_CLAIM_EXPOSURE_POLICY_CONTACT.groupby("POLICYNUMBER_N", as_index=False).agg(
            {"CLAIMID_N": pd.Series.nunique})

        AUTO_FINAL_DATA20 = pd.merge(AUTO_FINAL_DATA19, PREV_RENEWALS, how='left', left_on='POLICYNUMBER',
                                     right_on='POLICYNUMBER_N')

        TEN_LIST_EXP = CC_CLAIM_EXPOSURE_POLICY_CONTACT.loc[CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1_N'].isnull(),
                                                            ['CLAIMID_N', 'EFFECTIVEDATE_N', 'ORIGEFFECTIVEDATE_N',
                                                             'FINALNAME_1_N']].drop_duplicates()

        CC_CLAIM_EXPOSURE_POLICY_CONTACT['INS_DEN'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.CLAIMANTDENORMID_EXP_N if x.INSUREDDENORMID_N is None else x.INSUREDDENORMID_N, axis=1)
        CC_CLAIM_EXPOSURE_POLICY_CONTACT['CLM_DEN'] = CC_CLAIM_EXPOSURE_POLICY_CONTACT.apply(
            lambda x: x.CLAIMANTDENORMID_EXP_N if x.INSUREDDENORMID_N is None else x.INSUREDDENORMID_N, axis=1)

        NAME_CLAIM_NULL_EXP = CC_CLAIM_EXPOSURE_POLICY_CONTACT.loc[
            CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1_N'].isnull(),
            ['CLAIMID_N', 'CLAIMANTDENORMID_EXP_N']].drop_duplicates()

        NAME_CLAIM_NULL_INS = CC_CLAIM_EXPOSURE_POLICY_CONTACT.loc[
            CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1_N'].isnull(),
            ['CLAIMID_N', 'INS_DEN']].drop_duplicates()

        NAME_CLAIM_NULL_CLM = CC_CLAIM_EXPOSURE_POLICY_CONTACT.loc[
            CC_CLAIM_EXPOSURE_POLICY_CONTACT['FINALNAME_1_N'].isnull(),
            ['CLAIMID_N', 'CLM_DEN']].drop_duplicates()

        CC_CLAIM_EXPOSURE_POLICY_CONTACT_T = CC_CLAIM_EXPOSURE_POLICY_CONTACT.drop_duplicates()
        CC_CLAIM_EXPOSURE_POLICY_CONTACT_T['FINALNAME_1_N'].replace('nan', np.NaN, inplace=True)

        TEN_LIST_EXP['ORIGEFFECTIVEDATE_N'] = pd.to_datetime(TEN_LIST_EXP['ORIGEFFECTIVEDATE_N'], errors='coerce')
        TEN_LIST_EXP['EFFECTIVEDATE_N'] = pd.to_datetime(TEN_LIST_EXP['EFFECTIVEDATE_N'], errors='coerce')
        TEN_LIST_EXP['EFFECTIVEDATE_N'] = TEN_LIST_EXP['EFFECTIVEDATE_N'].fillna(datetime.datetime)
        TEN_LIST_EXP['ORIG_EFF_YR'] = TEN_LIST_EXP['ORIGEFFECTIVEDATE_N'].map(lambda x: x.year)

        TEN_LIST_EXP['EFF_DATE_F'] = TEN_LIST_EXP.apply(
            lambda x: x.EFFECTIVEDATE_N if x.ORIG_EFF_YR is None else x.ORIGEFFECTIVEDATE_N, axis=1)

        TEN_LIST_EXP_NULL = \
        CC_CLAIM_EXPOSURE_POLICY_CONTACT_T[CC_CLAIM_EXPOSURE_POLICY_CONTACT_T['FINALNAME_1_N'].isnull()][
            ['CLAIMID_N', 'EFFECTIVEDATE_N', 'ORIGEFFECTIVEDATE_N', 'INS_DEN']].drop_duplicates()
        TEN_LIST_EXP_NULL['ORIGEFFECTIVEDATE_N'] = pd.to_datetime(TEN_LIST_EXP_NULL['ORIGEFFECTIVEDATE_N'],
                                                                  errors='coerce')
        TEN_LIST_EXP_NULL['EFFECTIVEDATE_N'] = pd.to_datetime(TEN_LIST_EXP_NULL['EFFECTIVEDATE_N'], errors='coerce')
        TEN_LIST_EXP_NULL['ORIG_EFF_YR'] = TEN_LIST_EXP_NULL['ORIGEFFECTIVEDATE_N'].map(lambda x: x.year)
        TEN_LIST_EXP_NULL['EFF_DATE_F'] = TEN_LIST_EXP_NULL.apply(
            lambda x: x.EFFECTIVEDATE_N if x.ORIG_EFF_YR is None else x.ORIGEFFECTIVEDATE_N, axis=1)
        TEN_LIST_EXP_NULL_F = pd.DataFrame(TEN_LIST_EXP_NULL.groupby('INS_DEN', as_index=False)['EFF_DATE_F'].min())

        CLAIM_EXP_INC_FINAL_CON['INS_DEN'] = CLAIM_EXP_INC_FINAL_CON.apply(
            lambda x: x.CLAIMANTDENORMID_EXP_N if x.INSUREDDENORMID_N is None else x.INSUREDDENORMID_N, axis=1)
        CLAIM_EXP_INC_FINAL_CON['CLM_DEN'] = CLAIM_EXP_INC_FINAL_CON.apply(
            lambda x: x.CLAIMANTDENORMID_EXP_N if x.INSUREDDENORMID_N is None else x.INSUREDDENORMID_N, axis=1)

        TEN_INS_1 = CLAIM_EXP_INC_FINAL_CON[['CLAIMID_x', 'INSUREDNAME_F_N', 'INS_DEN']]

        TEN_INS_2 = pd.merge(TEN_INS_1, TEN_LIST_EXP_NULL_F, how='left', on='INS_DEN')

        TEN_LIST_EXP_F = pd.DataFrame(TEN_LIST_EXP.groupby('FINALNAME_1_N', as_index=False)['EFF_DATE_F'].min())

        TEN_INS_3 = pd.merge(TEN_INS_2, TEN_LIST_EXP_F, how='left', left_on='INSUREDNAME_F_N', right_on='FINALNAME_1_N')
        TEN_INS_3['EFF_DATE_FINAL'] = TEN_INS_3['EFF_DATE_F_y']
        TEN_INS_3['EFF_DATE_FINAL'] = TEN_INS_3['EFF_DATE_FINAL'].fillna(TEN_INS_3['EFF_DATE_F_x'])
        TEN_INS_3['EFF_DATE_FINAL'] = pd.to_datetime(TEN_INS_3['EFF_DATE_FINAL'], errors='coerce')

        TEN_INS_4 = TEN_INS_3[['CLAIMID_x', 'EFF_DATE_FINAL']].drop_duplicates()
        TEN_INS_4['RANK1'] = TEN_INS_4.sort_values(['EFF_DATE_FINAL'], ascending=[1]).groupby(
            ['CLAIMID_x']).cumcount() + 1

        TEN_INS_5 = TEN_INS_4[TEN_INS_4['RANK1'] == 1]

        CLM_REP = AUTO_FINAL_DATA19[['CLAIMID_x', 'REPORTEDDATE']].drop_duplicates()
        CLM_REP['REPORTEDDATE'] = pd.to_datetime(CLM_REP['REPORTEDDATE'])

        TENURE_1 = pd.merge(TEN_INS_5, CLM_REP, how='left', on='CLAIMID_x')
        TENURE_1['LENGTH'] = TENURE_1['REPORTEDDATE'].sub(TENURE_1['EFF_DATE_FINAL'], axis=0)

        TENURE_1['LENGTH'] = TENURE_1['LENGTH'] / np.timedelta64(1, 'D')
        TENURE_1['LENGTH'] = TENURE_1['LENGTH'].fillna(1625)
        TENURE_1['LENGTH'] = TENURE_1['LENGTH'].astype(int)
        del TENURE_1['RANK1']

        TENURE_1.columns = ['CLAIMID_x', 'EFF_DATE_FINAL', 'REPORTEDDATE', 'CLM_TENURE']

        AUTO_FINAL_DATA21 = pd.merge(AUTO_FINAL_DATA20, TENURE_1[['CLAIMID_x', 'CLM_TENURE']], how='left',
                                     on='CLAIMID_x')

        LOSSCAUSE = pd.read_csv(os.path.join(INPUT_DIR, 'LOSSCAUSE_GROUPING2.csv'))

        AUTO_FINAL_DATA25 = pd.merge(AUTO_FINAL_DATA21, LOSSCAUSE, how='left', on='LOSSCAUSEID')

        AUTO_FINAL_DATA25_1 = pd.merge(AUTO_FINAL_DATA25, AUTO_DATA_EXP_AGG2, how='left', left_on='CLAIMID',
                                       right_on='CLAIMID_x')

        coverage_rollup = CLAIM_EXP_INC_FINAL.loc[:, ['CLAIMID_x', 'EXPOSUREID', 'INCIDENTID', 'COV_ROLLUP_3']]
        coverage_rollup['COV_ROLLUP_3'] = coverage_rollup['COV_ROLLUP_3'].astype(str)

        coverage_rollup = pd.get_dummies(coverage_rollup)

        if not any(col.endswith('Inland Marine') for col in coverage_rollup.columns):
            coverage_rollup['COV_ROLLUP_3_Inland Marine'] = 0

        if not any(col.endswith('Contents') for col in coverage_rollup.columns):
            coverage_rollup['COV_ROLLUP_3_Contents'] = 0

        if not any(col.endswith('Loss of Use') for col in coverage_rollup.columns):
            coverage_rollup['COV_ROLLUP_3_Loss of Use'] = 0

        coverage_rollup2 = coverage_rollup.groupby(['CLAIMID_x'], as_index=False).agg({'COV_ROLLUP_3_Collision': np.sum,
                                                                                       'COV_ROLLUP_3_Comprehensive': np.sum,
                                                                                       'COV_ROLLUP_3_Auto PD': np.sum,
                                                                                       'COV_ROLLUP_3_Rental Reimbursement': np.sum,
                                                                                       'COV_ROLLUP_3_UIM/UM': np.sum,
                                                                                       'COV_ROLLUP_3_Towing and Labor': np.sum,
                                                                                       'COV_ROLLUP_3_Inland Marine': np.sum,
                                                                                       'COV_ROLLUP_3_Building': np.sum,
                                                                                       'COV_ROLLUP_3_GL PD': np.sum,
                                                                                       'COV_ROLLUP_3_nan': np.sum,
                                                                                       'COV_ROLLUP_3_All Other': np.sum,
                                                                                       'COV_ROLLUP_3_GL BI': np.sum,
                                                                                       'COV_ROLLUP_3_Contents': np.sum,
                                                                                       'COV_ROLLUP_3_PIP': np.sum,
                                                                                       'COV_ROLLUP_3_Auto BI': np.sum,
                                                                                       'COV_ROLLUP_3_Med Pay': np.sum,
                                                                                       'COV_ROLLUP_3_Loss of Use': np.sum
                                                                                       })

        AUTO_FINAL_DATA25_2 = pd.merge(AUTO_FINAL_DATA25_1, coverage_rollup2, how='left', left_on='CLAIMID',
                                       right_on='CLAIMID_x')

        AUTO_FINAL_DATA25_2['LOSSDATE'] = pd.to_datetime(AUTO_FINAL_DATA25_2['LOSSDATE'])

        AUTO_FINAL_DATA25_2['LOSSHOUR'] = AUTO_FINAL_DATA25_2['LOSSDATE'].apply(lambda x: x.hour)

        AUTO_FINAL_DATA25_2['LOSS_TIMEOFDAY'] = AUTO_FINAL_DATA25_2['LOSSHOUR'].apply(
            lambda x: '0 to 5' if (x >= 0 and x <= 5) else '6 to 11' if (x >= 6 and x <= 11) else '12 to 17' if (
                x >= 12 and x <= 17) else '18 to 23')

        PREV_POL_CLAIMS2 = CC_CLAIM_EXPOSURE_POLICY_CONTACT.groupby("POLICYNUMBER_N", as_index=False).agg(
            {"CLAIMID_N": pd.Series.nunique})

        PREV_POL_CLAIMS2.columns = ['POLICYNUMBER', 'PREV_POL_CLAIMS']

        AUTO_FINAL_DATA25_3 = pd.merge(AUTO_FINAL_DATA25_2, PREV_POL_CLAIMS2, how='left', on='POLICYNUMBER')

        AUTO_FINAL_DATA25_3['DAYS_OPEN'] = (datetime.date.today() - AUTO_FINAL_DATA25_3.REPORTEDDATE).astype(
            'timedelta64[D]')

        AUTO_FINAL_DATA25_3['PAYMENT_PERDAY'] = AUTO_FINAL_DATA25_3['PAYMENT'] / AUTO_FINAL_DATA25_3['DAYS_OPEN']

        lossparty = CLAIM_EXP_INC_FINAL.loc[:, ['CLAIMID_x', 'EXPOSUREID', 'INCIDENTID', 'LOSSPARTYID']]
        lossparty['LOSSPARTYID'] = np.nan_to_num(lossparty['LOSSPARTYID']).astype(int).astype(str)
        lossparty = pd.get_dummies(lossparty)
        if not any(col.endswith('nan') or col.endswith('0') for col in lossparty.columns):
            lossparty['LOSSPARTYID_nan'] = 0
        elif any(col.endswith('0') for col in lossparty.columns):
            lossparty.rename(columns={'LOSSPARTYID_0': 'LOSSPARTYID_nan'}, inplace=True)

        lossparty2 = lossparty.groupby(['CLAIMID_x'], as_index=False).agg({'LOSSPARTYID_10001': np.sum,
                                                                           'LOSSPARTYID_10002': np.sum,
                                                                           'LOSSPARTYID_nan': np.sum})

        AUTO_FINAL_DATA25_4 = pd.merge(AUTO_FINAL_DATA25_3, lossparty2, how='left', left_on='CLAIMID',
                                       right_on='CLAIMID_x')

        injured_cnt = CLAIM_EXP_INC_FINAL.loc[:,
                      ['CLAIMID_x', 'LOSSPARTYID', 'CLAIMANTDENORMID', 'INSUREDDENORMID', 'CLAIMANTDENORMID_DM',
                       'BI_x']]

        injured_cnt['FP_INJURED_CNT'] = injured_cnt.apply(
            lambda x: x.CLAIMANTDENORMID_DM if x.LOSSPARTYID == 10001.0 and x.BI_x == 1 else 0, axis=1)
        injured_cnt['TP_INJURED_CNT'] = injured_cnt.apply(
            lambda x: x.CLAIMANTDENORMID_DM if x.LOSSPARTYID == 10002.0 and x.BI_x == 1 else 0, axis=1)

        injured_cnt2 = injured_cnt.groupby(['CLAIMID_x'], as_index=False).agg(
            {'FP_INJURED_CNT': pd.Series.nunique, 'TP_INJURED_CNT': pd.Series.nunique})

        AUTO_FINAL_DATA25_5 = pd.merge(AUTO_FINAL_DATA25_4, injured_cnt2, how='left', left_on='CLAIMID',
                                       right_on='CLAIMID_x')

        UNST_DATA_PROD['FIRE_UNST_F2'] = UNST_DATA_PROD['FIRE_UNST_F'].apply(lambda X: 1 if X else 0)
        UNST_DATA_PROD['THEFT_UNST_F2'] = UNST_DATA_PROD['THEFT_UNST_F'].apply(lambda X: 1 if X else 0)
        UNST_DATA_PROD['TOTALLOSS_UNST_F2'] = UNST_DATA_PROD['TOTALLOSS_UNST_F'].apply(lambda X: 1 if X else 0)
        UNST_DATA_PROD['ATTORNEY_UNST_F2'] = UNST_DATA_PROD['ATTORNEY_UNST_F'].apply(lambda X: 1 if X else 0)
        UNST_DATA_PROD['CHIRO_UNST_F2'] = UNST_DATA_PROD['CHIRO_UNST_F'].apply(lambda X: 1 if X else 0)

        UNST_DATA_PROD_FLAG = UNST_DATA_PROD.loc[:,
                              ['CLAIMID', 'FIRE_UNST_F2', 'THEFT_UNST_F2', 'TOTALLOSS_UNST_F2', 'ATTORNEY_UNST_F2',
                               'CHIRO_UNST_F2']]

        del AUTO_FINAL_DATA25_5['CLAIMID']

        AUTO_FINAL_DATA25_6 = pd.merge(AUTO_FINAL_DATA25_5, UNST_DATA_PROD_FLAG, how='left', left_on='CLAIMID_x',
                                       right_on='CLAIMID')

        # -----------------------------Extract Topics--------------------------------------------

        from gensim import corpora
        from tqdm import tqdm

        lda_data_all = pickle.load(open(os.path.join(MODEL_LOCATION, 'lda.model'), 'rb'))

        UNST_TOPIC = UNST_DATA_PROD[['CLAIMID', 'ADJUSTER_NOTES']]
        raw_texts = UNST_TOPIC['ADJUSTER_NOTES']

        text_tokenized = [txt.split(' ') for txt in tqdm(raw_texts)]

        dic_new = corpora.Dictionary(text_tokenized)

        corpus_new = [dic_new.doc2bow(txt) for txt in tqdm(text_tokenized)]
        new_doc_topic_dist = np.array(lda_data_all.get_document_topics(corpus_new, minimum_probability=0))[:, :, 1]

        UNST_TOPIC_SCORE = pd.DataFrame(data=new_doc_topic_dist, index=UNST_TOPIC['CLAIMID'])
        UNST_TOPIC_SCORE = UNST_TOPIC_SCORE.reset_index()

        list_col_topic = ['CLAIMID']

        for i in range(20):
            list_col_topic.append('TOPIC_NEW_' + str(i))

        UNST_TOPIC_SCORE.columns = list_col_topic
        # -----------------------------------------------------------------------------------------

        # ---------------------Extract Keywords----------------------------------------------------

        UNST_KW = UNST_DATA_PROD[['CLAIMID', 'ADJUSTER_NOTES']].copy()

        w_docs = UNST_KW['ADJUSTER_NOTES']
        w_index = UNST_KW['CLAIMID']

        selected_keys = ['OWNERSHIP CLAIM', 'CLMT ATTY', 'WAGE LOSS', 'PRIORITY CATEGORY', 'MEDICAL RECORDS',
                         'TEMPORARY OWNERSHIP', 'ATTORNEY OFFICE', 'POLICE REPORT', 'PHYSICAL THERAPY', 'SOFT TISSUE',
                         'NECK PAIN', 'INDEPENDENT MEDICAL', 'BODILY INJURY', 'AUTO BODY', 'SHOP CHOICE', 'BODY SHOP']

        for elem in selected_keys:
            UNST_KW[pd.Series(['KW_', elem]).str.cat()] = (UNST_KW['ADJUSTER_NOTES'].str.contains(elem.lower())).astype(
                int)

        list_col_kp = ['CLAIMID', 'ADJUSTER_NOTES']

        for i in range(16):
            list_col_kp.append('KEY_PHRASE_NEW_' + str(i))

        UNST_KW.columns = list_col_kp

        UNST_KW = UNST_KW.rename(columns={"CLAIMID": "CLAIMID_KW"})

        # ------------------------------------------------------------------------------------------------------

        AUTO_FINAL_DATA25_7 = pd.merge(AUTO_FINAL_DATA25_6, UNST_TOPIC_SCORE, how='left', left_on='CLAIMID_x',
                                       right_on='CLAIMID')
        AUTO_FINAL_DATA25_7 = AUTO_FINAL_DATA25_7.loc[:, ~AUTO_FINAL_DATA25_7.columns.duplicated()]

        AUTO_FINAL_DATA25_8 = pd.merge(AUTO_FINAL_DATA25_7, UNST_KW, how='left', left_on='CLAIMID_x',
                                       right_on='CLAIMID_KW')

        categorize_var_list = ['CLAIMTIERID', 'EXT_LOCATIONOFLOSS_DESC', 'FAULTRATING', 'LITIGATIONSTATUSID',
                               'RESPONSIBLEPARTY_DESC', 'SUBROGATIONSTATUSID', 'LOSS_TIMEOFDAY', 'LOSSCAUSE_GROUP']

        AUTO_FINAL_DATA25_8[categorize_var_list] = AUTO_FINAL_DATA25_8[categorize_var_list].astype(str)

        # ---------------------------To be excluded-------------------------------------
        # list_cate_col = pickle.load(open(os.path.join(INPUT_DIR, 'training_features.p'), 'rb'))
        # list_cate_col = [col.upper() for col in list_cate_col]
        #
        # # AUTO_FINAL_DATA25_8[list_cate_col].columns = [col.upper() for col in list_cate_col]
        #
        # # list_col_old = AUTO_FINAL_DATA25_8.columns
        # -------------------------------------------------------------------------------

        self.categorize_var(AUTO_FINAL_DATA25_8, 'CLAIMTIERID', '10002', 'CLAIMTIERID_10002')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'CLAIMTIERID', '10004', 'CLAIMTIERID_10004')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'CLAIMTIERID', '10005', 'CLAIMTIERID_10005')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'CLAIMTIERID', '-1', 'CLAIMTIERID_-1')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', '0', 'EXT_LOCATIONOFLOSS_DESC_0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'In Transit - Motor Vehicles',
                       'EXT_LOCATIONOFLOSS_DESC_In Transit - Motor Vehicles')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'Premises of Others',
                       'EXT_LOCATIONOFLOSS_DESC_Premises of Others')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', "Insured's Premises",
                       "EXT_LOCATIONOFLOSS_DESC_Insured's Premises")
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'In Transit - Messengers/Salesman',
                       'EXT_LOCATIONOFLOSS_DESC_In Transit - Messengers/Salesman')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'In Transit - Air/Waterborne',
                       'EXT_LOCATIONOFLOSS_DESC_In Transit - Air/Waterborne')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'In Transit - Railroad',
                       'EXT_LOCATIONOFLOSS_DESC_In Transit - Railroad')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'EXT_LOCATIONOFLOSS_DESC', 'In Transit - Surface Mail',
                       'EXT_LOCATIONOFLOSS_DESC_In Transit - Surface Mail')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '0', 'FAULTRATING_0.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '10005.0', 'FAULTRATING_10005.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '10003.0', 'FAULTRATING_10003.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '10001.0', 'FAULTRATING_10001.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '10004.0', 'FAULTRATING_10004.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '-1.0', 'FAULTRATING_-1.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'FAULTRATING', '10002.0', 'FAULTRATING_10002.0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LITIGATIONSTATUSID', '3', 'LITIGATIONSTATUSID_3')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LITIGATIONSTATUSID', '-1', 'LITIGATIONSTATUSID_-1')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LITIGATIONSTATUSID', '1', 'LITIGATIONSTATUSID_1')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LITIGATIONSTATUSID', '2', 'LITIGATIONSTATUSID_2')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LITIGATIONSTATUSID', '10002', 'LITIGATIONSTATUSID_10002')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', '0', 'RESPONSIBLEPARTY_DESC_0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Unknown', 'RESPONSIBLEPARTY_DESC_Unknown')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Insured', 'RESPONSIBLEPARTY_DESC_Insured')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Other', 'RESPONSIBLEPARTY_DESC_Other')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', "Vandals, Vagrants, Criminals",
                       "RESPONSIBLEPARTY_DESC_Vandals, Vagrants, Criminals")
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Employee', 'RESPONSIBLEPARTY_DESC_Employee')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Additional Insured',
                       'RESPONSIBLEPARTY_DESC_Additional Insured')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Family Member',
                       'RESPONSIBLEPARTY_DESC_Family Member')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Tenant', 'RESPONSIBLEPARTY_DESC_Tenant')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Customer', 'RESPONSIBLEPARTY_DESC_Customer')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Subcontractor',
                       'RESPONSIBLEPARTY_DESC_Subcontractor')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Vendor', 'RESPONSIBLEPARTY_DESC_Vendor')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'RESPONSIBLEPARTY_DESC', 'Public Protection',
                       'RESPONSIBLEPARTY_DESC_Public Protection')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'SUBROGATIONSTATUSID', '-1', 'SUBROGATIONSTATUSID_-1')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'SUBROGATIONSTATUSID', '10001', 'SUBROGATIONSTATUSID_10001')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'SUBROGATIONSTATUSID', '10002', 'SUBROGATIONSTATUSID_10002')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSS_TIMEOFDAY', '0 to 5', 'LOSS_TIMEOFDAY_0 to 5')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSS_TIMEOFDAY', '18 to 23', 'LOSS_TIMEOFDAY_18 to 23')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSS_TIMEOFDAY', '6 to 11', 'LOSS_TIMEOFDAY_6 to 11')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSS_TIMEOFDAY', '12 to 17', 'LOSS_TIMEOFDAY_12 to 17')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'CONVERTED', 'LOSSCAUSE_GROUP_CONVERTED')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'ANIMAL', 'LOSSCAUSE_GROUP_ANIMAL')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'MOTOR_VEHICLE_ACCIDENT',
                       'LOSSCAUSE_GROUP_MOTOR_VEHICLE_ACCIDENT')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'CATASTROPHE', 'LOSSCAUSE_GROUP_CATASTROPHE')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'NON_ACCIDENT_DAMAGE',
                       'LOSSCAUSE_GROUP_NON_ACCIDENT_DAMAGE')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'TOWING_AND_LABOR', 'LOSSCAUSE_GROUP_TOWING_AND_LABOR')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'GLASS_DAMAGE', 'LOSSCAUSE_GROUP_GLASS_DAMAGE')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'FIRE_OR_EXPLOSION', 'LOSSCAUSE_GROUP_FIRE_OR_EXPLOSION')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'WATER_DAMAGE', 'LOSSCAUSE_GROUP_WATER_DAMAGE')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'OTHER', 'LOSSCAUSE_GROUP_OTHER')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', '0', 'LOSSCAUSE_GROUP_0')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'HUMAN_VIOLENCE', 'LOSSCAUSE_GROUP_HUMAN_VIOLENCE')
        self.categorize_var(AUTO_FINAL_DATA25_8, 'LOSSCAUSE_GROUP', 'THEFT', 'LOSSCAUSE_GROUP_THEFT')



        AUTO_FINAL_DATA25_8.columns = [col.upper() for col in AUTO_FINAL_DATA25_8.columns]
        AUTO_FINAL_DATA25_8 = AUTO_FINAL_DATA25_8.fillna(0)

        AUTO_FINAL_DATA25_9 = AUTO_FINAL_DATA25_8.copy()
        AUTO_FINAL_DATA25_9 = AUTO_FINAL_DATA25_9.rename(columns={'TOPIC_NEW_0': 'TOPIC_MODEL_0',
                                                                  'TOPIC_NEW_1': 'TOPIC_MODEL_1',
                                                                  'TOPIC_NEW_2': 'TOPIC_MODEL_2',
                                                                  'TOPIC_NEW_3': 'TOPIC_MODEL_3',
                                                                  'TOPIC_NEW_4': 'TOPIC_MODEL_4',
                                                                  'TOPIC_NEW_5': 'TOPIC_MODEL_5',
                                                                  'TOPIC_NEW_6': 'TOPIC_MODEL_6',
                                                                  'TOPIC_NEW_7': 'TOPIC_MODEL_7',
                                                                  'TOPIC_NEW_8': 'TOPIC_MODEL_8',
                                                                  'TOPIC_NEW_9': 'TOPIC_MODEL_9',
                                                                  'TOPIC_NEW_10': 'TOPIC_MODEL_10',
                                                                  'TOPIC_NEW_11': 'TOPIC_MODEL_11',
                                                                  'TOPIC_NEW_12': 'TOPIC_MODEL_12',
                                                                  'TOPIC_NEW_13': 'TOPIC_MODEL_13',
                                                                  'TOPIC_NEW_14': 'TOPIC_MODEL_14',
                                                                  'TOPIC_NEW_15': 'TOPIC_MODEL_15',
                                                                  'TOPIC_NEW_16': 'TOPIC_MODEL_16',
                                                                  'TOPIC_NEW_17': 'TOPIC_MODEL_17',
                                                                  'TOPIC_NEW_18': 'TOPIC_MODEL_18',
                                                                  'TOPIC_NEW_19': 'TOPIC_MODEL_19',

                                                                  'KEY_PHRASE_NEW_0': 'KEY_PHRASE_0',
                                                                  'KEY_PHRASE_NEW_1': 'KEY_PHRASE_1',
                                                                  'KEY_PHRASE_NEW_2': 'KEY_PHRASE_2',
                                                                  'KEY_PHRASE_NEW_3': 'KEY_PHRASE_3',
                                                                  'KEY_PHRASE_NEW_4': 'KEY_PHRASE_4',
                                                                  'KEY_PHRASE_NEW_5': 'KEY_PHRASE_5',
                                                                  'KEY_PHRASE_NEW_6': 'KEY_PHRASE_6',
                                                                  'KEY_PHRASE_NEW_7': 'KEY_PHRASE_7',
                                                                  'KEY_PHRASE_NEW_8': 'KEY_PHRASE_8',
                                                                  'KEY_PHRASE_NEW_9': 'KEY_PHRASE_9',
                                                                  'KEY_PHRASE_NEW_10': 'KEY_PHRASE_10',
                                                                  'KEY_PHRASE_NEW_11': 'KEY_PHRASE_11',
                                                                  'KEY_PHRASE_NEW_12': 'KEY_PHRASE_12',
                                                                  'KEY_PHRASE_NEW_13': 'KEY_PHRASE_13',
                                                                  'KEY_PHRASE_NEW_14': 'KEY_PHRASE_14',
                                                                  'KEY_PHRASE_NEW_15': 'KEY_PHRASE_15',

                                                                  'EXT_LOWIMPACTINCIDENT': 'EXT_LOWIMPACTINCIDENT_F',
                                                                  'LOSSPARTYID_10001': 'LOSSPARTYID_10001.0',
                                                                  'LOSSPARTYID_10002': 'LOSSPARTYID_10002.0',
                                                                  'VEHICLEDRIVEABLE': 'VEHICLEDRIVEABLE_F',
                                                                  'AVG_NOTES_PERDAY': 'AVG_NOTES_PER_DAY',
                                                                  'EFFECTIVEDATE_N': 'PREV_RENEWALS',
                                                                  'CHIRO_UNST_F2': 'CHIRO_FLAG_UNST',
                                                                  'THEFT_FLAG': 'THEFT_FLAG_FINAL',
                                                                  'ATTORNEY_UNST_F2': 'ATTORNEY_FLAG_UNST',
                                                                  'FIRE_FLAG': 'FIRE_FLAG_FINAL',
                                                                  'TOTALLOSS': 'TOTALLOSS_FINAL',
                                                                  'COLLISION_INC_DESC': 'COLLISION_F2',
                                                                  'FIRE_UNST_F2': 'FIRE_FLAG_UNST',
                                                                  'THEFT_UNST_F2': 'THEFT_FLAG_UNST',
                                                                  'TOTALLOSS_UNST_F2': 'TOTAL_FLAG_UNST'})

        var = [
            'BI',
            'CLAIMANTDENORMID_10009',
            'CLAIMANTDENORMID_10006',
            'CLAIMANTDENORMID_10014',
            'CLAIMANTDENORMID_10001',
            'CLAIMANTDENORMID_10004',
            'CLAIMANTDENORMID_10010',
            'CLAIMANTDENORMID_10002',
            'CLAIMANTDENORMID_10003',
            'CLAIMANTDENORMID_10005',
            'CLAIMANTDENORMID_10007',
            'CLAIMANTDENORMID_10011',
            'CLAIMANTDENORMID_10008',

            'COLLISION_F2',
            'COV_ROLLUP_3_ALL OTHER',
            'COV_ROLLUP_3_AUTO BI',
            'COV_ROLLUP_3_AUTO PD',
            'COV_ROLLUP_3_BUILDING',
            'COV_ROLLUP_3_COLLISION',
            'COV_ROLLUP_3_COMPREHENSIVE',
            'COV_ROLLUP_3_CONTENTS',
            'COV_ROLLUP_3_GL BI',
            'COV_ROLLUP_3_GL PD',
            'COV_ROLLUP_3_INLAND MARINE',
            'COV_ROLLUP_3_LOSS OF USE',
            'COV_ROLLUP_3_MED PAY',
            'COV_ROLLUP_3_PIP',
            'COV_ROLLUP_3_RENTAL REIMBURSEMENT',
            'COV_ROLLUP_3_TOWING AND LABOR',
            'COV_ROLLUP_3_UIM/UM',
            'COV_ROLLUP_3_NAN',
            'EXT_LOWIMPACTINCIDENT_F',

            'ISO_ADDRESS',
            'ISO_DL',
            'ISO_LICENSEPLATENUMBER',
            'ISO_LOSSLOCATION',
            'ISO_NAME',
            'ISO_PHONE',
            'ISO_SSN',
            'ISO_VIN',
            'LOCATIONOFTHEFT_DESC',
            #  'LOSSCAUSEID',
            'LOSSPARTYID_10001.0',
            'LOSSPARTYID_10002.0',
            'LOSSPARTYID_NAN',
            #  'REAR_END_FINAL',
            'REPORT_LAG',

            'VEHICLEDRIVEABLE_F',
            'INSURED_OWNED',
            'TP_NEW_V',
            'TP_OTHER',
            'INSURED_RENTED',
            'INSURED_LISTED',
            'TP_RENTED',
            'TP_LISTED',
            'LOSSLOC_CITY_FP_F',
            'LOSSLOC_CITY_TP_F',
            #  'LOSSLOC_CITY_FP_TP_F',
            'LOSSLOC_ZIP_FP_F',
            'LOSSLOC_ZIP_TP_F',
            #  'LOSSLOC_ZIP_FP_TP_F',
            'LOSSLOC_STATE_FP_F',
            'LOSSLOC_STATE_TP_F',
            #  'LOSSLOC_STATE_FP_TP_F',
            'FOLL_CLOSELY_PC1',
            'INTERSECTION_PC1',
            'PARKED_VEHICLE',
            'REAR_END_PC_F',
            'SPEED_HEADON_PC_F',
            'GLASS_DAMAGE_F',
            'BODY_HEAD',
            'BODY_NECK',
            'BODY_UPPER_EXT',
            'BODY_TRUNK',
            'BODY_LOWER_EXT',
            'BODY_UNKNOWN',
            'BODY_MULTIPLE',
            'FP_V_AGE',
            'TP_V_AGE',
            'PC_ISSUE_REPORT_LAG',
            'PC_REPORT_EXPRN_LAG',
            'PC_LOSS_EXPRN_LAG',
            'AVG_NOTES_PER_DAY',
            'EXPOSUREID',
            'INCIDENTID',
            'INSURED_NEW_V',

            'CLAIM_CNT_NAME_CLM',
            'CLAIM_CNT_NAME_INS',
            'PREV_RENEWALS',
            'CLM_TENURE',
            'TP_OWNED',

            'THEFT_FLAG_UNST',
            'FIRE_FLAG_UNST',
            'TOTAL_FLAG_UNST',
            'CHIRO_FLAG_UNST',
            # 'SWOOP_SQUAT_UNST',
            # 'SIDE_SWIPE_UNST'  ,
            'THEFT_FLAG_FINAL',
            'ATTORNEY_FLAG_UNST',
            'PREV_POL_CLAIMS',

            'FIRE_FLAG_FINAL',
            'TOTALLOSS_FINAL',

            'CLAIMTIERID_10002',
            'CLAIMTIERID_10004',
            'CLAIMTIERID_10005',
            'CLAIMTIERID_-1',
            'EXT_LOCATIONOFLOSS_DESC_IN TRANSIT - MOTOR VEHICLES',
            'EXT_LOCATIONOFLOSS_DESC_PREMISES OF OTHERS',
            "EXT_LOCATIONOFLOSS_DESC_INSURED'S PREMISES",
            'EXT_LOCATIONOFLOSS_DESC_IN TRANSIT - MESSENGERS/SALESMAN',
            'EXT_LOCATIONOFLOSS_DESC_IN TRANSIT - AIR/WATERBORNE',
            'EXT_LOCATIONOFLOSS_DESC_IN TRANSIT - RAILROAD',
            'EXT_LOCATIONOFLOSS_DESC_IN TRANSIT - SURFACE MAIL',
            'FAULTRATING_0.0',
            'FAULTRATING_10005.0',
            'FAULTRATING_10003.0',
            'FAULTRATING_10001.0',
            'FAULTRATING_10004.0',
            'FAULTRATING_-1.0',
            'FAULTRATING_10002.0',
            'LITIGATIONSTATUSID_3',
            'LITIGATIONSTATUSID_-1',
            'LITIGATIONSTATUSID_1',
            'LITIGATIONSTATUSID_2',
            'LITIGATIONSTATUSID_10002',
            'RESPONSIBLEPARTY_DESC_0',
            'RESPONSIBLEPARTY_DESC_UNKNOWN',
            'RESPONSIBLEPARTY_DESC_INSURED',
            'RESPONSIBLEPARTY_DESC_OTHER',
            'RESPONSIBLEPARTY_DESC_VANDALS, VAGRANTS, CRIMINALS',
            'RESPONSIBLEPARTY_DESC_EMPLOYEE',
            'RESPONSIBLEPARTY_DESC_ADDITIONAL INSURED',
            'RESPONSIBLEPARTY_DESC_FAMILY MEMBER',
            'RESPONSIBLEPARTY_DESC_TENANT',
            'RESPONSIBLEPARTY_DESC_CUSTOMER',
            'RESPONSIBLEPARTY_DESC_SUBCONTRACTOR',
            'RESPONSIBLEPARTY_DESC_VENDOR',
            'RESPONSIBLEPARTY_DESC_PUBLIC PROTECTION',
            'SUBROGATIONSTATUSID_-1',
            'SUBROGATIONSTATUSID_10001',
            'SUBROGATIONSTATUSID_10002',
            'LOSS_TIMEOFDAY_0 TO 5',
            'LOSS_TIMEOFDAY_18 TO 23',
            'LOSS_TIMEOFDAY_6 TO 11',
            'LOSS_TIMEOFDAY_12 TO 17',
            'LOSSCAUSE_GROUP_CONVERTED',
            'LOSSCAUSE_GROUP_ANIMAL',
            'LOSSCAUSE_GROUP_MOTOR_VEHICLE_ACCIDENT',
            'LOSSCAUSE_GROUP_CATASTROPHE',
            'LOSSCAUSE_GROUP_NON_ACCIDENT_DAMAGE',
            'LOSSCAUSE_GROUP_TOWING_AND_LABOR',
            'LOSSCAUSE_GROUP_GLASS_DAMAGE',
            'LOSSCAUSE_GROUP_FIRE_OR_EXPLOSION',
            'LOSSCAUSE_GROUP_WATER_DAMAGE',
            'LOSSCAUSE_GROUP_OTHER',
            'LOSSCAUSE_GROUP_0',
            'LOSSCAUSE_GROUP_HUMAN_VIOLENCE',
            'LOSSCAUSE_GROUP_THEFT',

            'TOPIC_MODEL_0', 'TOPIC_MODEL_1',
            'TOPIC_MODEL_2', 'TOPIC_MODEL_3', 'TOPIC_MODEL_4', 'TOPIC_MODEL_5',
            'TOPIC_MODEL_6', 'TOPIC_MODEL_7', 'TOPIC_MODEL_8', 'TOPIC_MODEL_9',
            'TOPIC_MODEL_10', 'TOPIC_MODEL_11', 'TOPIC_MODEL_12', 'TOPIC_MODEL_13',
            'TOPIC_MODEL_14', 'TOPIC_MODEL_15', 'TOPIC_MODEL_16', 'TOPIC_MODEL_17',
            'TOPIC_MODEL_18', 'TOPIC_MODEL_19'
            #         , 'TOPIC_MODEL_20', 'TOPIC_MODEL_21',
            #        'TOPIC_MODEL_22', 'TOPIC_MODEL_23', 'TOPIC_MODEL_24', 'TOPIC_MODEL_25',
            #        'TOPIC_MODEL_26', 'TOPIC_MODEL_27', 'TOPIC_MODEL_28', 'TOPIC_MODEL_29'
            , 'KEY_PHRASE_0',
            'KEY_PHRASE_1',
            'KEY_PHRASE_2',
            'KEY_PHRASE_3',
            'KEY_PHRASE_4',
            'KEY_PHRASE_5',
            'KEY_PHRASE_6',
            'KEY_PHRASE_7',
            'KEY_PHRASE_8',
            'KEY_PHRASE_9',
            'KEY_PHRASE_10',
            'KEY_PHRASE_11',
            'KEY_PHRASE_12',
            'KEY_PHRASE_13',
            'KEY_PHRASE_14',
            'KEY_PHRASE_15']
        X_scoring = AUTO_FINAL_DATA25_9[var]
        X_scoring = [self.getFeatures(X_scoring, var, idx) for idx in X_scoring.index]
        vec = DictVectorizer()
        X_scoring = vec.fit_transform(X_scoring)
        imp = Imputer(missing_values=np.nan, strategy='mean', axis=0)
        X_scoring = imp.fit_transform(X_scoring)
        X_scoring = X_scoring.todense()

        output_df = AUTO_FINAL_DATA25_8[['CLAIMID_X', 'CLAIMNUMBER', 'REPORTEDDATE']]
        # output_df['Remaining Reserve'] = None
        # output_df['JurisdictionState'] = None

        return {'info': output_df,
                'data': X_scoring}
