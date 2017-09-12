import re
import nltk
from nltk.corpus import stopwords
from stemming.porter2 import stem
from nltk import word_tokenize
from sklearn import decomposition
import operator
import pandas as pd
import numpy as np
from sklearn.preprocessing import Imputer
from scipy.sparse import csr_matrix
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from sklearn.ensemble.partial_dependence import partial_dependence
from sklearn.ensemble.partial_dependence import plot_partial_dependence
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
# from utility import mygini,returnFeatureImportance,generatePDP
import gzip, pickle, os, time
from sklearn.preprocessing import binarize
import scipy.stats
from sklearn.feature_extraction.text import TfidfVectorizer
import txt2struct as ts
txt_process = ts.TextToStructure()

from config import Config
conf = Config()

import pyodbc as pyodbc
cnxn = pyodbc.connect(conf.db_config)
cursor = cnxn.cursor()

INPUT_DIR = conf.input_folder
OUTPUT_DIR = conf.output_folder
LDA_MODEL_LOCATION = conf.LDAModel_path


def stem_tokens(tokens):
    stemmed = []
    for item in tokens:
        stemmed.append(stem(item))
    return stemmed


def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = stem_tokens(tokens)
    return stems


def exec_query():
    cursor.execute("""select claim.*,
    exposure.CoverageID,
    --exposure.Ext_KindOfLoss,
    --exposure.LostPropertyType,
    --cctl_lostpropertytype.[description] as lostprop_desc,
    exposure.LossParty,
    cctl_losspartytype.[DESCRIPTION] as lossparty_desc,
    --exposure.ISOStatus as exp_isostatus,
    --cctl_isostatus.[DESCRIPTION] as exp_isostatus_desc,
    exposure.ClaimantType,
    cctl_claimanttype.[DESCRIPTION] AS ClaimantType_DESC,
    --exposure.LossCategory,
    --exposure.OtherCoverageInfo,
    --exposure.CloseDate as exp_closedate,
    --exposure.AssignmentDate,
    exposure.ClaimantDenormID as ClaimantDenormID_exp,
    --exposure.Segment as exp_segment,
    exposure.CoverageSubType,
    cctl_coveragesubtype.[DESCRIPTION] as covsub_desc,
    exposure.PrimaryCoverage,
    cctl_coveragetype.[DESCRIPTION] as coveragetype_desc,
    exposure.ID as exposureid,
    --exposure.[State] as exposurestatus_code,
    --cctl_exposurestate.[DESCRIPTION] as exposurestatus_desc,
    exposure.ExposureType,
    cctl_exposuretype.[DESCRIPTION] as exposuretype_desc,
    exposure.IncidentID 
    into #claim_exp
    from GWCC_ProdCopy.dbo.cc_exposure exposure inner join #claim_fin claim
    on claim.claimid=exposure.claimid

    --left join GWCC_ProdCopy.dbo.cctl_lostpropertytype
    --on exposure.LostPropertyType=GWCC_ProdCopy.dbo.cctl_lostpropertytype.ID

    left join GWCC_ProdCopy.dbo.cctl_exposurestate
    on exposure.[State]=GWCC_ProdCopy.dbo.cctl_exposurestate.ID

    left join GWCC_ProdCopy.dbo.cctl_claimanttype cctl_claimanttype
    on exposure.ClaimantType=cctl_claimanttype.ID

    left join GWCC_ProdCopy.dbo.cctl_exposuretype
    on exposure.exposuretype=GWCC_ProdCopy.dbo.cctl_exposuretype.ID

    left join GWCC_ProdCopy.dbo.cctl_losspartytype
    on exposure.lossparty=GWCC_ProdCopy.dbo.cctl_losspartytype.ID

    left join GWCC_ProdCopy.dbo.cctl_coveragesubtype
    on exposure.coveragesubtype=GWCC_ProdCopy.dbo.cctl_coveragesubtype.ID

    left join GWCC_ProdCopy.dbo.cctl_isostatus
    on exposure.isostatus=GWCC_ProdCopy.dbo.cctl_isostatus.ID

    left join GWCC_ProdCopy.dbo.cctl_coveragetype
    on exposure.PrimaryCoverage=GWCC_ProdCopy.dbo.cctl_coveragetype.ID""")

    cursor.execute("""select #claim_exp.*,
    pol.EffectiveDate,
    pol.OrigEffectiveDate, 
    pol.Ext_Underwritingstate,
    pol.ExpirationDate, 
    pol.Ext_ClaimsMade,
    pol.PolicyType,
    pol.PolicyNumber,
    pol.ReportingDate,
    pol.Ext_CustomerNumber,
    pol.UnderwritingGroup,
    pol.TotalVehicles,
    pol.ext_broadlobcode as broadlobcode,
    Lob.[description] as ext_broadlobcode_desc,
    pol.Ext_UWrtSubLOBCodes as sublobcode,
    sublob.[description] as Ext_UWrtSubLOBCodes_desc

    into #claim_exp_pol


    from #claim_exp 
    left join GWCC_ProdCopy.dbo.cc_policy pol
    on #claim_exp.policyid=pol.id

    left join GWCC_ProdCopy.dbo.cctl_ext_broadlobcode lob
    on pol.ext_broadlobcode=lob.id

    left join GWCC_ProdCopy.dbo.cctl_ext_uwrtsublobcodes sublob
    on pol.Ext_UWrtSubLOBCodes=sublob.id""")

    cursor.execute("""select incident.VehicleDriveable,
    incident.Ext_EntireSideDamaged,
    incident.VehicleParked,
    --incident.Ext_VehDamageEstDetermined,
    incident.Collision,
    incident.Ext_RearEndCollision,
    incident.TotalLoss,
    --incident.FireBurnDash,
    incident.VehicleID,
    incident.VehicleType,
    veh_type.[DESCRIPTION] as vehicletype_desc,
    --incident.[description] as incident_desc,
    incident.Ext_LowImpactIncident,
    incident.VehicleLossParty,
    incident.speed,
    incident.Ext_DidAccidentInvolve,
    --incident.Ext_UnlistedDriver,
    incident.ID as incident_id,
    vehicle.Model,
    vehicle.Vin,
    vehicle.[Year] as vehicle_year,
    vehicle.Make
    into #incident_f
    from 
    GWCC_ProdCopy.dbo.cc_incident incident left join GWCC_ProdCopy.dbo.cc_vehicle vehicle
    on incident.vehicleid=vehicle.id

    left join 
    GWCC_ProdCopy.dbo.cctl_vehicletype veh_type
    on incident.vehicletype=veh_type.id""")

    cursor.execute("""select cepe.*, incident.*
    into #claim_exp_pol_inc
    from #claim_exp_pol cepe left join #incident_f incident
    on cepe.incidentid=incident.incident_id""")

    cursor.execute("""select #claim_exp_pol_inc.*, 
    exposurerpt.RemainingReserves,
    exposurerpt.TotalPayments,
    exposurerpt.TotalRecoveries,
    exposurerpt.FuturePayments,
    exposurerpt.OpenReserves
    into #claim_exp_pol_inc_rpt
    from #claim_exp_pol_inc left join GWCC_ProdCopy.dbo.cc_exposurerpt exposurerpt
    on #claim_exp_pol_inc.exposureid=exposurerpt.exposureid""")

    cursor.execute("""select a.*,
    con.NumDependents,
             con.DateOfBirth,
             con.Gender,
             con.Occupation,
             upper(con.Name) as Name,
             upper(INS.NAME) AS INSURED_NAME,
             upper(CLM.NAME) AS CLAIMANT_NAME,
             con.ID as contactid,
             con.primaryaddressID
             into #claim_exp_pol_inc_rpt_cont_1
             from #claim_exp_pol_inc_rpt a left join GWCC_ProdCopy.dbo.cc_contact con
             on a.claimantdenormid_exp=con.id

             left join GWCC_ProdCopy.dbo.cc_contact INS
             on a.InsuredDenormID=INS.id

             left join GWCC_ProdCopy.dbo.cc_contact CLM
             on a.ClaimantDenormID=CLM.id""")

    cursor.execute("""select *, (case when Name IS null and CLAIMANT_NAME is not null then CLAIMANT_NAME 
    when Name is null and CLAIMANT_NAME is null and lossparty=('10001') then INSURED_NAME else Name end) as final_name
             into #claim_exp_pol_inc_rpt_cont
             from #claim_exp_pol_inc_rpt_cont_1""")

    cursor.execute("""select #claim_exp_pol_inc_rpt_cont.*, 
    addrz.ID as addressid,
    addrz.AddressLine1 as contact_address,
    addrz.city as contact_city, 
    addrz.PostalCode as contact_postalcode,
    addrz.[State] as contact_state,
    cctl_state.[DESCRIPTION] as contact_state_Desc, 
    addrz.county as contact_county
    into #cc_final_data
    from #claim_exp_pol_inc_rpt_cont left join GWCC_ProdCopy.dbo.cc_address addrz
    on #claim_exp_pol_inc_rpt_cont.primaryaddressID=addrz.ID

    left join GWCC_ProdCopy.dbo.cctl_state cctl_state 
    on addrz.[State]=cctl_state.ID""")

    cursor.execute("""select #cc_final_data.*,
    (year(lossdate) - vehicle_year) as vehicle_age, 
    datediff(day, EffectiveDate,Reporteddate) as PC_issue_report,
    datediff(day, Reporteddate, Expirationdate) as PC_report_expire,
    (case when ReportedDate>EffectiveDate and ReportedDate<ExpirationDate then 1 else 0 end) as report_interval,
    coalesce(datediff(day,reporteddate,CloseDate), datediff(day,reporteddate, getdate())) as Claim_Opening_days,
    (case when lossloc_postalcode=contact_postalcode then 1 else 0 end) as postal_code_match,
    (case when lossloc_city=contact_city then 1 else 0 end) as city_match,
    (case when lossloc_state=contact_state then 1 else 0 end) as state_match,
    (OpenReserves+TotalPayments-TotalRecoveries) as TotalIncurredNet,
    (OpenReserves+TotalPayments) as  TotalIncurredGross

    into #cc_final_data_v2
    from #cc_final_data""")

    cursor.execute("""select Name, COUNT(distinct claimid) as Pre_claims_cnt 
    into #prev_claims
    from #cc_final_Data 
    group by Name""")

    cursor.execute("""select a.claimid, a.ClaimantDenormID, upper(b.name) as name, UPPER(Ext_EmailDisplayName) as Ext_EmailDisplayName
    into #names
    from GWCC_ProdCopy.dbo.cc_exposure a left join GWCC_ProdCopy.dbo.cc_contact b
    on a.ClaimantDenormID=b.ID
    order by a.ClaimID""")

    cursor.execute("""select distinct claimid, name, 1 as prev_claims
    into #unique_names
    from #names""")

    cursor.execute("""select name, SUM(prev_claims) as prev_claims
    into #prev_claims_qbe
    from #unique_names
    where name is not null
    group by name""")

    cursor.execute("""select *
      into #null_names
    from #names
    where name is null""")

    cursor.execute("""select distinct claimid, ClaimantDenormID, 1 as prev_claims
    into #null_unique_names
    from #null_names""")

    cursor.execute("""select ClaimantDenormID, SUM(prev_claims) as prev_claims
    into #prev_claims_qbe_null
    from #null_unique_names
    where ClaimantDenormID is not null
    group by ClaimantDenormID""")

    cursor.execute("""SELECT POLICYNUMBER, COUNT(distinct EffectiveDate ) as freq
    into #pol_eff
    FROM GWCC_ProdCopy.DBO.cc_policy
    group by PolicyNumber""")

    cursor.execute("""select a.*, b.prev_claims as prev_claims_1, d.prev_claims as prev_claims_2 , c.freq as prev_renewals
    into #cc_final_data_v3_1_1
    from #cc_final_data_v2 a left join #prev_claims_qbe b
    on a.Final_Name=b.Name
    left join #prev_claims_qbe_null d
    on a.claimantdenormid_exp=d.claimantdenormid
    left join #pol_eff c
    on a.PolicyNumber=c.PolicyNumber""")

    cursor.execute("""select a.*, coalesce((case when prev_claims_1 is not null then prev_claims_1 else prev_claims_2 end),0) as Pre_claims_cnt
    into #cc_final_data_v3_1_2
    from #cc_final_data_v3_1_1 a""")

    cursor.execute("""select a.claimid, c.policyid, d.OrigEffectiveDate, d.EffectiveDate, a.ClaimantDenormID, upper(b.name) as name
    into #association
             from GWCC_ProdCopy.dbo.cc_exposure a left join GWCC_ProdCopy.dbo.cc_contact b
             on a.ClaimantDenormID=b.ID
             left join GWCC_ProdCopy.dbo.cc_claim c
             on a.ClaimID=c.id
             left join GWCC_ProdCopy.dbo.cc_policy d
             on c.PolicyID=d.ID
             order by a.ClaimID""")

    cursor.execute("""select *, (case when OrigEffectiveDate IS null or YEAR(OrigEffectiveDate)<1991 then EffectiveDate else OrigEffectiveDate end) as oldest_date
    into #association_1
             from #association""")

    cursor.execute("""select Name, MIN(oldest_date) as oldest_date
    into #association_2
             from #association_1
             where name is not null and name not in ('Suspense Policy')
             group by name""")

    cursor.execute("""select * into #null_assoc from #association where name is null or name in ('Suspense Policy')""")

    cursor.execute("""select *, (case when OrigEffectiveDate IS null or YEAR(OrigEffectiveDate)<1991 then EffectiveDate else OrigEffectiveDate end) as oldest_date
    into #null_assoc_1
             from #null_assoc""")

    cursor.execute("""select ClaimantDenormID, MIN(oldest_date) as oldest_date
    into #null_assoc_2
             from #null_assoc_1
             where ClaimantDenormID is not null
             group by ClaimantDenormID
             """)

    cursor.execute("""select a.*, b.oldest_date as oldest_date_1, c.oldest_date as oldest_date_2
    into #cc_final_data_v3_1_3
             from #cc_final_data_v3_1_2 a left join #association_2 b
             on a.final_name=b.name
             left join #null_assoc_2 c
             on a.claimantdenormid_exp=c.claimantdenormid""")

    cursor.execute("""
    select a.*, (case when a.oldest_date_1 is not null and YEAR(a.oldest_date_1)>1990 then a.oldest_date_1
    when year(a.oldest_date_1)<1991 and a.oldest_date_2 is null then a.OrigEffectiveDate else a.oldest_date_2 end) as oldest_date_f
    into #cc_final_data_v3_1_4
    from #cc_final_data_v3_1_3 a""")

    cursor.execute("""select *, (case when oldest_date_f is null and OrigEffectiveDate is not null and YEAR(OrigEffectiveDate)>1990 then OrigEffectiveDate 
               when oldest_date_f is null and OrigEffectiveDate is not null and YEAR(OrigEffectiveDate)<1991 then EffectiveDate
               else oldest_date_f  end) as oldest_date_f_1
    into #cc_final_data_v3_1_5
    from #cc_final_data_v3_1_4""")

    cursor.execute("""select a.*, datediff(day, a.oldest_date_f_1, a.ReportedDate) as claimant_tenure,
    (case when datediff(year, a.oldest_date_f_1, a.ReportedDate)=0 then 1 else datediff(year, a.oldest_date_f_1, a.ReportedDate) end) as claimant_tenure_yr
             into #cc_final_data_v3_1_NEW
             from #cc_final_data_v3_1_5 a""")

    cursor.execute("""select *, ROUND(CAST(Pre_claims_cnt AS FLOAT)/CAST(claimant_tenure_yr AS FLOAT),2) AS PREV_CLM_PER_YR
             INTO #cc_final_data_v3_1
    FROM #cc_final_data_v3_1_NEW
             """)

    cursor.execute("""select a.claimid, a.ClaimantDenormID, upper(b.name) as name, c.reporteddate
    into #names_l
    from GWCC_ProdCopy.dbo.cc_exposure a left join GWCC_ProdCopy.dbo.cc_contact b
    on a.ClaimantDenormID=b.ID
    left join GWCC_ProdCopy.dbo.cc_claim c
    on a.ClaimID=c.ID
    order by a.ClaimID""")

    cursor.execute("""select * into #names_l_p
    from #names_l
             where name is not null and name not in ('Suspense Policy')""")

    cursor.execute("""select distinct claimid, claimantDenormID, name, reporteddate
             into #names_l_p_0
             from #names_l_p""")

    cursor.execute("""SELECT * 
             into #names_l_p_1
             FROM (
             select name, claimid, ReportedDate, ROW_NUMBER() OVER (PARTITION BY name ORDER BY reporteddate) AS RN
             from #names_l_p_0
             )Sub""")

    cursor.execute("""select *, rn+1 as rn_1
             into #names_l_p_2
             from #names_l_p_1""")

    cursor.execute("""select a.*, b.reporteddate as report_old
             into #names_l_p_3
             from #names_l_p_2 a left join #names_l_p_2 b
             on a.name=b.name
             and a.rn=b.rn_1""")

    cursor.execute("""select *, coalesce(datediff(day, report_old, ReportedDate),0) AS prev_claim_dur
             into #names_l_p_4
             from #names_l_p_3""")

    cursor.execute("""select * into #names_l_p_null
    from #names_l
             WHERE name is null or name in ('Suspense Policy')""")

    cursor.execute("""select distinct claimid, claimantDenormID, reporteddate
             into #names_l_p_0_null
    from #names_l_p_null""")

    cursor.execute("""SELECT * 
             into #names_l_p_1_null
             FROM (
             select claimantDenormID, claimid, ReportedDate, ROW_NUMBER() OVER (PARTITION BY claimantDenormID ORDER BY reporteddate) AS RN
             from #names_l_p_0_null
             )Sub""")

    cursor.execute("""select *, rn+1 as rn_1
             into #names_l_p_2_null
             from #names_l_p_1_null""")

    cursor.execute("""select a.*, b.reporteddate as report_old
             into #names_l_p_3_null
             from #names_l_p_2_null a left join #names_l_p_2_null b
             on a.claimantDenormID=b.claimantDenormID
             and a.rn=b.rn_1""")

    cursor.execute("""select *, coalesce(datediff(day, report_old, ReportedDate),0) AS prev_claim_dur
             into #names_l_p_4_null
             from #names_l_p_3_null""")

    cursor.execute("""select a.*, b.prev_claim_dur as prev_claim_dur_1, d.prev_claim_dur as prev_claim_dur_2
             into #cc_final_data_v3_2_lbo
             from #cc_final_data_v3_1 a left join #names_l_p_4 b
             on a.Final_Name=b.Name
             left join #names_l_p_4_null d
             on a.claimantdenormid_exp=d.claimantdenormid
             """)

    cursor.execute("""select a.*, coalesce((case when prev_claim_dur_1 is not null then prev_claim_dur_1 else prev_claim_dur_2 end),0) as prev_claim_duration
    into #cc_final_data_v3_2_l
             from #cc_final_data_v3_2_lbo a""")

    cursor.execute("""select *, (case	when PrimaryCoverage=' ' and exposuretype_desc='Bodily Injury' then 'NA'
    when PrimaryCoverage=' ' and exposuretype_desc='EmployerLiability' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Ext_BusinessIncome' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='General' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='LostWages' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Med Pay' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Personal Property' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='PIP' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Property' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Towing and Labor' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='Vehicle' then 'NA'
             when PrimaryCoverage=' ' and exposuretype_desc='WCInjuryDamage' then 'NA'
             when PrimaryCoverage='10002' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10002' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10004' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10006' and exposuretype_desc='General' then 'Contents'
             when PrimaryCoverage='10006' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10011' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10012' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10012' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10012' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10013' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10013' and exposuretype_desc='General' then 'GL BI'
             when PrimaryCoverage='10013' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10013' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10014' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10016' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10017' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10021' and exposuretype_desc='General' then 'Loss of Use'
             when PrimaryCoverage='10022' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10022' and exposuretype_desc='Dwelling' then 'GL PD'
             when PrimaryCoverage='10022' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10024' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10024' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10024' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10024' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10025' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10025' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10030' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10033' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10034' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10036' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10037' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10040' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10040' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10040' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10043' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10047' and exposuretype_desc='Towing and Labor' then 'Collision'
             when PrimaryCoverage='10047' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10048' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10049' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10049' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10049' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10050' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10050' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10051' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10051' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10051' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10052' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10053' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10054' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10057' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10058' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10060' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10060' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10061' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10063' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10064' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10064' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10064' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10065' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10065' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10066' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10066' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10066' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10067' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10067' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10069' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10071' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10071' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10078' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10081' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10082' and exposuretype_desc='Vehicle' then 'Rental Reimbursement'
             when PrimaryCoverage='10085' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10087' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='10088' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10089' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10089' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='10090' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10091' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10092' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10094' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10095' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10095' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10096' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10096' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10097' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10098' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10100' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10102' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10102' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10103' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10103' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10103' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10104' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10105' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10106' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10107' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10108' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10109' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10111' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10111' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10112' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10112' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10113' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10114' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10115' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10116' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10117' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10118' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10119' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10121' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10123' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10124' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10124' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10125' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10126' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10127' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10128' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10130' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10130' and exposuretype_desc='Property' then 'Prof Liab'
             when PrimaryCoverage='10132' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10132' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10133' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10133' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10135' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10136' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10140' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10145' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10149' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10153' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10155' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10156' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='10157' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10159' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10159' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10160' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10161' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10161' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10163' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='10163' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10163' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10167' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10168' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10168' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10168' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10168' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10168' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10168' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10169' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10170' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10170' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10170' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10170' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10173' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10175' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10182' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10182' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10183' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10184' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10193' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10194' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10197' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10199' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10199' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10199' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10199' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10202' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10202' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10202' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10204' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10211' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10211' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10211' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10211' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10213' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10214' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10216' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10216' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10217' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10218' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10221' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10221' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10226' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10226' and exposuretype_desc='Personal Property' then 'Building'
             when PrimaryCoverage='10226' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10229' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10229' and exposuretype_desc='General' then 'Building'
             when PrimaryCoverage='10229' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10229' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10230' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10230' and exposuretype_desc='General' then 'Building'
             when PrimaryCoverage='10230' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10230' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10232' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10232' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10232' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10232' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10232' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10232' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10232' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10233' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10234' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10235' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10236' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10239' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10240' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10240' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10242' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10242' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10243' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10245' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10246' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10247' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10248' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10248' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10250' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10250' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10251' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10251' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10251' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10251' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10251' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10251' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10251' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10251' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10252' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10253' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10254' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10255' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10261' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10262' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10262' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10264' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10265' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10266' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10267' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10267' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10270' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='10271' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10271' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10273' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10273' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10276' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10278' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10278' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10279' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10280' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10281' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10282' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10283' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10286' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10287' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10287' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10287' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10287' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10287' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10288' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10288' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10288' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10288' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10289' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10289' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10289' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10289' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10290' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10290' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10290' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10290' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10292' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10292' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10293' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10294' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10295' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10295' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10296' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10296' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10298' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10299' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10304' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='10305' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10305' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10306' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10306' and exposuretype_desc='General' then 'GL BI'
             when PrimaryCoverage='10307' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10307' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10307' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10307' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10311' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10311' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10311' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10311' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10311' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10312' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10312' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10312' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10312' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10312' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10314' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10314' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10314' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10315' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10315' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10315' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10315' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10315' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10316' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10316' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10316' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10316' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10316' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10318' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='10318' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10319' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10320' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10325' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10327' and exposuretype_desc='Content' then 'All Other'
             when PrimaryCoverage='10331' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10332' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10333' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10334' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10336' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10338' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10338' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10338' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10339' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10341' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10341' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10342' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10342' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10342' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10342' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10342' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10346' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10349' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10352' and exposuretype_desc='General' then 'Contents'
             when PrimaryCoverage='10355' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10359' and exposuretype_desc='Personal Property' then 'GL PD'
             when PrimaryCoverage='10360' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10361' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10362' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10367' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10367' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10367' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10367' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10368' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10369' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10370' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10371' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10371' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='10372' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10372' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10373' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10374' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10374' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10374' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='10375' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10376' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10376' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='10380' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10381' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10381' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10381' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10382' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10383' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10385' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10387' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10388' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10389' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10390' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10392' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='10392' and exposuretype_desc='Other Structure' then 'All Other'
             when PrimaryCoverage='10394' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10394' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10397' and exposuretype_desc='Bodily Injury' then 'D & O'
             when PrimaryCoverage='10397' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='10398' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='10408' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10409' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10410' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10410' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10411' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10413' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10415' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10416' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10416' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10421' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10428' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10429' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10432' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10436' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10439' and exposuretype_desc='Bodily Injury' then 'Other Liab'
             when PrimaryCoverage='10440' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10442' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10443' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10443' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10443' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10445' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10447' and exposuretype_desc='Bodily Injury' then 'Other Liab'
             when PrimaryCoverage='10448' and exposuretype_desc='Bodily Injury' then 'Other Liab'
             when PrimaryCoverage='10448' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10449' and exposuretype_desc='Bodily Injury' then 'Other Liab'
             when PrimaryCoverage='10449' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10450' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10451' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10451' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10453' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10455' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='10455' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10455' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10456' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10457' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='10458' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10460' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10461' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10462' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10463' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10465' and exposuretype_desc='Bodily Injury' then 'E & O'
             when PrimaryCoverage='10465' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10465' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10470' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10471' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10472' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10472' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10474' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10475' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10477' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10478' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10478' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10479' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10482' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10483' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10484' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10484' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10485' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10486' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10486' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='10487' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10488' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10489' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10490' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='10496' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10496' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10496' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10506' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10507' and exposuretype_desc='General' then 'Loss of Use'
             when PrimaryCoverage='10509' and exposuretype_desc='General' then 'Building'
             when PrimaryCoverage='10510' and exposuretype_desc='General' then 'Building'
             when PrimaryCoverage='10511' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10513' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10513' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10514' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10515' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10515' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10516' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10516' and exposuretype_desc='Personal Property' then 'Building'
             when PrimaryCoverage='10517' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10517' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10520' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10520' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10522' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10523' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10524' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10525' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10527' and exposuretype_desc='Personal Property' then 'Building'
             when PrimaryCoverage='10527' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10532' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10535' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10535' and exposuretype_desc='Property' then 'Loss of Use'
             when PrimaryCoverage='10536' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10537' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10537' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10537' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10537' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10538' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10538' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10538' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10538' and exposuretype_desc='Personal Property' then 'GL PD'
             when PrimaryCoverage='10538' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10538' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10539' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10542' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10545' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10547' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10548' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10549' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10549' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10549' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10550' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10553' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10554' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10555' and exposuretype_desc='LossOfUseDamage' then 'Loss of Use'
             when PrimaryCoverage='10555' and exposuretype_desc='Personal Property' then 'Loss of Use'
             when PrimaryCoverage='10555' and exposuretype_desc='Property' then 'Loss of Use'
             when PrimaryCoverage='10556' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10556' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10557' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10558' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10560' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10561' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10561' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10561' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10562' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10562' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10564' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10566' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10570' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10573' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10574' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10575' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10576' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10578' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10581' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10581' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10582' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10583' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10585' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10587' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10587' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10587' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10587' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10588' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10588' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10593' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10595' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10596' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10597' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10601' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10603' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10604' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10606' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10607' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10611' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10615' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10615' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10615' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10616' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10617' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10620' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10626' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10627' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10628' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10630' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10631' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10632' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10632' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='10632' and exposuretype_desc='Property' then 'Prof Liab'
             when PrimaryCoverage='10633' and exposuretype_desc='Property' then 'Collision'
             when PrimaryCoverage='10633' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10634' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10634' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10634' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10635' and exposuretype_desc='Property' then 'COMPREHENSIVE'
             when PrimaryCoverage='10635' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10637' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10638' and exposuretype_desc='Property' then 'Collision'
             when PrimaryCoverage='10638' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10639' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='10640' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10640' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='10642' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10643' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10646' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10649' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10653' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10653' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10654' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10654' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10655' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10655' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10655' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10655' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10655' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10656' and exposuretype_desc='Bodily Injury' then 'Med Pay'
             when PrimaryCoverage='10656' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10656' and exposuretype_desc='Vehicle' then 'Med Pay'
             when PrimaryCoverage='10657' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10659' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10660' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10661' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10661' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10663' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10665' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10666' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10667' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10669' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10669' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10669' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10669' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10674' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10675' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10675' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10678' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10681' and exposuretype_desc='Bodily Injury' then 'GL PD'
             when PrimaryCoverage='10681' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10681' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10684' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10684' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10684' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10685' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10685' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10687' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10688' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10690' and exposuretype_desc='General' then 'Contents'
             when PrimaryCoverage='10691' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10699' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10701' and exposuretype_desc='Vehicle' then 'Contents'
             when PrimaryCoverage='10703' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10706' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10723' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10728' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10728' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10737' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10737' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10737' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10737' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10738' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10739' and exposuretype_desc='Content' then 'All Other'
             when PrimaryCoverage='10742' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10746' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10748' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10749' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10749' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10749' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10751' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10753' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10753' and exposuretype_desc='Vehicle' then 'Contents'
             when PrimaryCoverage='10754' and exposuretype_desc='Vehicle' then 'Contents'
             when PrimaryCoverage='10755' and exposuretype_desc='Vehicle' then 'Contents'
             when PrimaryCoverage='10758' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10758' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10761' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10762' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10766' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10769' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10771' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10771' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10773' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10775' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10775' and exposuretype_desc='Property' then 'Other Liab'
             when PrimaryCoverage='10776' and exposuretype_desc='Bodily Injury' then 'Other Liab'
             when PrimaryCoverage='10776' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='10777' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10777' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10777' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10777' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10777' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10778' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10778' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10779' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10780' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10780' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10780' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10781' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10781' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10781' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10781' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10783' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10783' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10784' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10784' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10784' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10784' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10784' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10785' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10785' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10785' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10785' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10786' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10786' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='10786' and exposuretype_desc='Property' then 'Prof Liab'
             when PrimaryCoverage='10787' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10787' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10787' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10788' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10789' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10789' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10790' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10790' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10794' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10794' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10794' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10795' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10795' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10795' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10800' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10800' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10800' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10801' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10801' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10801' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10803' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10803' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10805' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10805' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10805' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10807' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10807' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10811' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10811' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10811' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10811' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10811' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10812' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10814' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10815' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10816' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10816' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10816' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10816' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10822' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10825' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10826' and exposuretype_desc='General' then 'Loss of Use'
             when PrimaryCoverage='10827' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10827' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10829' and exposuretype_desc='Property' then 'Loss of Use'
             when PrimaryCoverage='10830' and exposuretype_desc='General' then 'Loss of Use'
             when PrimaryCoverage='10830' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10831' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10834' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10834' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10834' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10835' and exposuretype_desc='Content' then 'All Other'
             when PrimaryCoverage='10835' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='10835' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='10835' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10836' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='10836' and exposuretype_desc='Other Structure' then 'All Other'
             when PrimaryCoverage='10838' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10840' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10841' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10843' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10844' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10846' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10847' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10849' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10850' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10851' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10862' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10864' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10865' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10867' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10867' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10867' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10868' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10868' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10868' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10869' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10870' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10873' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10873' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10874' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10875' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10877' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10878' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10879' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10881' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10895' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10895' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10896' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10896' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10896' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10897' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10900' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10903' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='10904' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10905' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10906' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10907' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10907' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10907' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10907' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10909' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10909' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10910' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10911' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='10912' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='10912' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10916' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10916' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10916' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10917' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10917' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10918' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10919' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10920' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10921' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10923' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='10925' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10926' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10927' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10927' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10927' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10927' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10927' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10928' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10928' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10928' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='10928' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10928' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='10930' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='10931' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10932' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10932' and exposuretype_desc='General' then 'Building'
             when PrimaryCoverage='10932' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='10932' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10933' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10933' and exposuretype_desc='General' then 'Contents'
             when PrimaryCoverage='10934' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10935' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10937' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='10938' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10939' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='10940' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10941' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='10943' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='10944' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10945' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10945' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10945' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10946' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10948' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10954' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10956' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10956' and exposuretype_desc='General' then 'GL BI'
             when PrimaryCoverage='10956' and exposuretype_desc='Property' then 'GL BI'
             when PrimaryCoverage='10959' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10959' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10959' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10960' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10965' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10965' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10967' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10967' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10968' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10977' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='10978' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10979' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10981' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10982' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10982' and exposuretype_desc='Vehicle' then 'PIP'
             when PrimaryCoverage='10984' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10985' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10986' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10987' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10989' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='10992' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='10992' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='10993' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='10994' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10995' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10997' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='10997' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='10997' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='10998' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='10999' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='10999' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='10999' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11000' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11000' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11000' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11001' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11002' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='11002' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11003' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='11004' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11004' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11004' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11004' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11004' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11005' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11007' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11008' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11008' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11008' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11008' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11008' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11009' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11009' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11009' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11009' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11009' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11011' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11011' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11011' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11012' and exposuretype_desc='Property' then 'E & O'
             when PrimaryCoverage='11013' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11014' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11017' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11017' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11017' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11017' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11017' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11018' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11018' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11018' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11018' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11018' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11019' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11019' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11019' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11019' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11021' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11022' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11026' and exposuretype_desc='Personal Property' then 'Building'
             when PrimaryCoverage='11026' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11030' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11031' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11032' and exposuretype_desc='Personal Property' then 'Building'
             when PrimaryCoverage='11032' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11036' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11041' and exposuretype_desc='Other Structure' then 'All Other'
             when PrimaryCoverage='11041' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11042' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11045' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11045' and exposuretype_desc='Property' then 'Loss of Use'
             when PrimaryCoverage='11046' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11047' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11048' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11049' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11050' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11053' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11054' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11055' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='11055' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11055' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11056' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11056' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11057' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11057' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11060' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11061' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11062' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11063' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11063' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11069' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11069' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11069' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11071' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11072' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11074' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11077' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11079' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11082' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11085' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11086' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11087' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11090' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='11090' and exposuretype_desc='Ext_ExtraExpense' then 'All Other'
             when PrimaryCoverage='11090' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11090' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11091' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11093' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11093' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11094' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11100' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11102' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11103' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11104' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11105' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11105' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11105' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11106' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11106' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11106' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11106' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11109' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11109' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='11112' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11112' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11113' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11114' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11117' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11119' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11119' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11120' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11120' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11122' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11122' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11125' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11126' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11129' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11134' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11135' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11137' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11143' and exposuretype_desc='Vehicle' then 'Rental Reimbursement'
             when PrimaryCoverage='11144' and exposuretype_desc='Vehicle' then 'Rental Reimbursement'
             when PrimaryCoverage='11151' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11156' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11156' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11156' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='11156' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11156' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11158' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='11158' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11158' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11159' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11160' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11161' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11164' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11169' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11171' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='11172' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11174' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11175' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11176' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11179' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11181' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11182' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11183' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11185' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11191' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11195' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11195' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11196' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11196' and exposuretype_desc='General' then 'All Other'
             when PrimaryCoverage='11196' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='11196' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11198' and exposuretype_desc='Ext_ExtraExpense' then 'Loss of Use'
             when PrimaryCoverage='11199' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11200' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11201' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11202' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11203' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11204' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11206' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='11207' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11208' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='11209' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11209' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11210' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11217' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='11217' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11219' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11223' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11225' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11225' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11229' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='11229' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11229' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11230' and exposuretype_desc='Ext_BusinessIncome' then 'All Other'
             when PrimaryCoverage='11230' and exposuretype_desc='Ext_ExtraExpense' then 'All Other'
             when PrimaryCoverage='11230' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11230' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11231' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11244' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11248' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='11249' and exposuretype_desc='Towing and Labor' then 'Towing and Labor'
             when PrimaryCoverage='11250' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11251' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11252' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11256' and exposuretype_desc='Dwelling' then 'All Other'
             when PrimaryCoverage='11257' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11257' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11258' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11258' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11259' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11259' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11260' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11262' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11263' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11264' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11265' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11265' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11266' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11266' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11267' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11268' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11268' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11269' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11269' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='11270' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11271' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11271' and exposuretype_desc='Med Pay' then 'Auto BI'
             when PrimaryCoverage='11273' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11273' and exposuretype_desc='Vehicle' then 'Auto BI'
             when PrimaryCoverage='11274' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11274' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11275' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11275' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11276' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11276' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11276' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11278' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11279' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11281' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11281' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11282' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11283' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11283' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11284' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11287' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11288' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11288' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11290' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11290' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11290' and exposuretype_desc='Property' then 'Prof Liab'
             when PrimaryCoverage='11295' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11296' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11297' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11297' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11298' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11298' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11300' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11300' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11300' and exposuretype_desc='LivingExpenses' then 'Loss of Use'
             when PrimaryCoverage='11301' and exposuretype_desc='Content' then 'Contents'
             when PrimaryCoverage='11301' and exposuretype_desc='Dwelling' then 'Building'
             when PrimaryCoverage='11306' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11306' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11306' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11306' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='11307' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11312' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11314' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='11315' and exposuretype_desc='Content' then 'All Other'
             when PrimaryCoverage='11315' and exposuretype_desc='Vehicle' then 'All Other'
             when PrimaryCoverage='11333' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11333' and exposuretype_desc='General' then 'GL BI'
             when PrimaryCoverage='11333' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11333' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11334' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11335' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11335' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11335' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11350' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11353' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11365' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='11374' and exposuretype_desc='Vehicle' then 'COMPREHENSIVE'
             when PrimaryCoverage='11497' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='11546' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11551' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11564' and exposuretype_desc='LostWages' then 'WC Lost Time'
             when PrimaryCoverage='11564' and exposuretype_desc='WCInjuryDamage' then 'WC Med Only'
             when PrimaryCoverage='11568' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11591' and exposuretype_desc='EmployerLiability' then 'Emp Liab'
             when PrimaryCoverage='11606' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11610' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11614' and exposuretype_desc='Vehicle' then 'PIP'
             when PrimaryCoverage='11620' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11624' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11625' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11626' and exposuretype_desc='Property' then 'Contents'
             when PrimaryCoverage='11634' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11644' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11644' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11665' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11666' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11668' and exposuretype_desc='Other Structure' then 'Building'
             when PrimaryCoverage='11668' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11670' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11684' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11687' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11688' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11689' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11689' and exposuretype_desc='Property' then 'D & O'
             when PrimaryCoverage='11693' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='11694' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='11696' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='11698' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='11700' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='11711' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11712' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11714' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11714' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11715' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11715' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11716' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11717' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11717' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11718' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11723' and exposuretype_desc='Personal Property' then 'Contents'
             when PrimaryCoverage='11724' and exposuretype_desc='General' then 'Auto PD'
             when PrimaryCoverage='11726' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11728' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11734' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11737' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11737' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11737' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11739' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11740' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11741' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11741' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11761' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11761' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='11761' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='11761' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='11762' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11767' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11770' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11771' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11781' and exposuretype_desc='General' then 'Prof Liab'
             when PrimaryCoverage='11792' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11793' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11794' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11795' and exposuretype_desc='Property' then 'All Other'
             when PrimaryCoverage='11799' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11831' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11833' and exposuretype_desc='Ext_BusinessIncome' then 'Loss of Use'
             when PrimaryCoverage='11835' and exposuretype_desc='Personal Property' then 'All Other'
             when PrimaryCoverage='11838' and exposuretype_desc='Property' then 'Building'
             when PrimaryCoverage='11880' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='11881' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='11882' and exposuretype_desc='Vehicle' then 'Collision'
             when PrimaryCoverage='11883' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='11884' and exposuretype_desc='Bodily Injury' then 'Auto BI'
             when PrimaryCoverage='11885' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='11886' and exposuretype_desc='Property' then 'Auto PD'
             when PrimaryCoverage='11887' and exposuretype_desc='Vehicle' then 'Auto PD'
             when PrimaryCoverage='11889' and exposuretype_desc='PIP' then 'PIP'
             when PrimaryCoverage='11903' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11904' and exposuretype_desc='General' then 'D & O'
             when PrimaryCoverage='11906' and exposuretype_desc='General' then 'Other Liab'
             when PrimaryCoverage='11918' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11919' and exposuretype_desc='Bodily Injury' then 'Prof Liab'
             when PrimaryCoverage='11929' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='11933' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12049' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12050' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12051' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12057' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12059' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12070' and exposuretype_desc='General' then 'E & O'
             when PrimaryCoverage='12079' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12081' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12081' and exposuretype_desc='Vehicle' then 'GL PD'
             when PrimaryCoverage='12085' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12085' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12085' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12086' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12088' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12090' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12099' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12099' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12099' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12100' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12101' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12102' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12105' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12106' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12106' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12113' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12113' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12113' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12114' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12114' and exposuretype_desc='General' then 'Hull'
             when PrimaryCoverage='12116' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12117' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12118' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12126' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12126' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12126' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='12126' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12128' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12129' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12132' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12134' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12135' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12140' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12140' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='12140' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12148' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12148' and exposuretype_desc='General' then 'GL PD'
             when PrimaryCoverage='12148' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12150' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12150' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12153' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12157' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12158' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12158' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12160' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12164' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12164' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12166' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12169' and exposuretype_desc='Med Pay' then 'Med Pay'
             when PrimaryCoverage='12173' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12174' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12174' and exposuretype_desc='Property' then 'GL PD'
             when PrimaryCoverage='12178' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12180' and exposuretype_desc='Bodily Injury' then 'GL BI'
             when PrimaryCoverage='12180' and exposuretype_desc='Ext_Aviation' then 'GL BI'
             when PrimaryCoverage='12181' and exposuretype_desc='Ext_Aviation' then 'Hull'
             when PrimaryCoverage='12186' and exposuretype_desc='Med Pay' then 'Med Pay'

             end) as cov_rollup_3
             into #cc_final_data_v3_2
             from #cc_final_data_v3_2_l""")

    cursor.execute("""select a.*, a.BI_amount_1+a.BI_amount_2 as BI_Amount 
    into #cc_final_data_v3
    from (select *, 
    (case when cov_rollup_3 in ('Auto BI', 'PIP')  then TotalIncurredGross else 0 end) as BI_amount_1,
    (case when cov_rollup_3 in ('Med Pay') and LossParty in ('10002')  then TotalIncurredGross else 0 end) as BI_amount_2,
    (case when cov_rollup_3 in ('Auto PD', 'COMPREHENSIVE', 'Collision', 'Rental Reimbursement', 'Towing and Labor','NULL','GL BI','GL PD','Contents','All Other','Building') then TotalIncurredGross else 0 end) as PD_Amount

    from #cc_final_data_v3_2) a""")

    cursor.execute("""select a.claimid, a.id as incidentid, b.primarybodypart
    into #body_part
    from GWCC_ProdCopy.dbo.cc_incident a left join GWCC_ProdCopy.dbo.cc_bodypart b
    on a.ID=b.IncidentID
    where a.ClaimID in (select distinct ClaimID from #cc_final_data)""")

    cursor.execute("""select claimid as claim_id, 
    coalesce(count(case when primarybodypart='10001' then claimid end),0) as Body_Head,
    coalesce(count(case when primarybodypart='10002' then claimid end),0) as Body_Neck,
    coalesce(count(case when primarybodypart='10003' then claimid end),0) as Body_Upper_ext,
    coalesce(count(case when primarybodypart='10004' then claimid end),0) as Body_Trunk,
    coalesce(count(case when primarybodypart='10005' then claimid end),0) as Body_Lower_ext,
    coalesce(count(case when primarybodypart='10006' then claimid end),0) as Body_Unknown,
    coalesce(count(case when primarybodypart='10007' then claimid end),0) as Body_Multiple
    into #body_check
    from #body_part
    group by ClaimID""")

    cursor.execute("""select a.*, b.*
    into #cc_final_body
    from #cc_final_data_v3 a left join #body_check b
    on a.claimid=b.claim_id""")

    cursor.execute("""select distinct
    SubrogationStatus,
    SubrogationStatus_desc,
    SIEscalateSIU,
    ReportedDate,
    Report_lag,
    Claim_Opening_days,
    PC_issue_report,
    PC_report_expire,
    Report_interval,
    Ext_ResponsibleParty,
    LossLocationID,
    SIUStatus,
    SIUStatus_desc,
    Ext_LossLocState,
    Ext_LossLocState_desc,
    JurisdictionState,
    JurisdictionState_desc,
    ISOReceiveDate,
    PolicyID,
    prev_renewals,
    LossType,
    LossType_desc,
    Ext_MoreThan3VehsInvolved,
    CloseDate,
    LocationOfTheft,
    LocationOfTheft_desc,
    Ext_LocationofLoss,
    ext_locationofloss_desc,
    LitigationStatus,
    litigationstatus_desc,
    LossCause,
    LossCause_desc,
    losscause_group,
    LossDate,
    ClaimantDenormID,
    INSURED_NAME,
    ISOStatus,
    isostatus_desc,
    ExposureBegan,
    SIScore,
    Ext_SourceClaimNumber,
    Fault,
    ISOKnown,
    claimid,
    ClaimNumber,
    SIULifeCycleState,
    SIULifeCycleState_desc,
    LOBCode,
    LOBcode_desc,
    --PoliceDeptInfo,
    claim_status_code,
    claim_status_desc,
    InsuredDenormID,
    lossloc_Desc,
    lossloc_City,
    lossloc_PostalCode,
    lossloc_State,
    --lossloc_AddressLine1,
    EffectiveDate,
    OrigEffectiveDate,
    Ext_Underwritingstate,
    ExpirationDate,
    Ext_ClaimsMade,
    PolicyType,
    PolicyNumber,
    ReportingDate,
    Ext_CustomerNumber,
    UnderwritingGroup,
    TotalVehicles,
    ext_broadlobcode_desc,
    sublobcode

    into #claim_aggr
    from #CC_final_Data_v3""")

    cursor.execute("""select a.*, b.*
    into #claim_aggr_body
    from #claim_aggr a left join #body_check b
    on a.claimid=b.claim_id""")

    cursor.execute("""select claimid, case when cov_rollup_3 in ('Auto BI') then 1 else 0 end as AUTO_BI,
    case when cov_rollup_3 in ('Auto PD') then 1 else 0 end as AUTO_PD,
    case when cov_rollup_3 in ('Collision') then 1 else 0 end as AUTO_COLLISION,
    case when cov_rollup_3 in ('COMPREHENSIVE') then 1 else 0 end as COMPREHENSIVE,
    case when cov_rollup_3 in ('Med Pay') then 1 else 0 end as MEDPAY,
    case when cov_rollup_3 in ('PIP') then 1 else 0 end as PIP,
    case when cov_rollup_3 in ('Rental Reimbursement') then 1 else 0 end as RENTAL_REIMB,
    case when cov_rollup_3 in ('Towing and Labor') then 1 else 0 end as TOW_LABOR,
    case when cov_rollup_3 in ('All Other') then 1 else 0 end as ALL_OTHER,
    case when cov_rollup_3 in ('Building') then 1 else 0 end as BUILDING,
    case when cov_rollup_3 in ('Contents') then 1 else 0 end as CONTENTS,
    case when cov_rollup_3 in ('GL BI') then 1 else 0 end as GL_BI,
    case when cov_rollup_3 in ('GL PD') then 1 else 0 end as GL_PD

    into #cov_rollup1
    from #cc_final_data_v3""")

    cursor.execute("""select claimid, 
    SUM(AUTO_BI) as AUTO_BI,
    SUM(AUTO_PD) as AUTO_PD,
    SUM(AUTO_COLLISION) as AUTO_COLLISION,
    SUM(COMPREHENSIVE) as COMPREHENSIVE,
    SUM(MEDPAY) as MEDPAY,
    SUM(PIP) as PIP,
    SUM(RENTAL_REIMB) as RENTAL_REIMB,
    SUM(TOW_LABOR) as TOW_LABOR,
    SUM(ALL_OTHER) as ALL_OTHER,
    SUM(BUILDING) as BUILDING,
    SUM(CONTENTS) as CONTENTS,
    SUM(GL_BI) as GL_BI,
    SUM(GL_PD) as GL_PD

    into #cov_rollup2
    from #cov_rollup1
    group by claimid""")

    cursor.execute("""select a.id, max(rear_collision_final) as rear_collision_final into #final_rear_end_coll
    from (select A.ID, (case when rear_end_coll=1 OR ext_rearendcollision=1 then 1 else 0 end) as rear_collision_final
    from (select a.id, a.rear_end_coll, b.ext_rearendcollision
    from (select id, [description],(case when [Description] like '%REAR%' or [Description] like '%RAE%' then 1 else 0 end) as rear_end_coll
    from (
    select id, upper([description]) as [description]
    from GWCC_ProdCopy.dbo.cc_claim
    where ID in (select distinct claimid from #cc_final_data_v3)) a) a left join #cc_final_data_v3 b
    on a.ID=b.claimid) a)a
             group by a.id""")

    # "COLLISION"
    cursor.execute("""select a.id, max(collision_final) as collision_final into #final_coll_ind
    from (select A.ID, (case when coll_from_desc=1 OR collision=1 then 1 else 0 end) as collision_final
    from (select a.id, a.coll_from_desc, b.collision
    from (select id, [description],(case when [Description] like '%REAR%' or [Description] like '%RAE%'
     or [Description] like '%COLL%'
     or [Description] like '%COLI' then 1 else 0 end) as coll_from_desc
    from (
    select id, upper([description]) as [description]
    from GWCC_ProdCopy.dbo.cc_claim
    where ID in (select distinct claimid from #cc_final_data_v3)) a) a left join #cc_final_data_v3 b
    on a.ID=b.claimid) a)a
             group by a.id""")

    # "Primary contribution cause GROUP"
    cursor.execute("""select a.claimid, b.pricontributingfactors , c.[description]
    into #pricontrib
    from #cc_final_data_v3 a left join GWCC_ProdCopy.dbo.cc_contribfactor b
    on a.claimid=b.claimid
    left join GWCC_ProdCopy.dbo.cctl_pricontributingfactors c
    on b.PriContributingFactors=c.ID""")
    cursor.execute("""select [DESCRIPTION], COUNT(distinct claimid) as count_c from #pricontrib
    group by [DESCRIPTION]""")

    # "Vehicle Driveable"
    cursor.execute("""select claimid, b.collision as VehicleDriveable 
    into #VehicleDriveable
    from
    (select a.*
    	from 
    			( select claimid, coalesce(count(distinct(case when VehicleDriveable='1' then claimid end) ),0) as collision
    	         	from (select claimid, VehicleDriveable from #cc_final_data_v3
                          where VehicleDriveable is not null) a
                  group by claimid) a
     union 

    (select * from ( select distinct claimid, VehicleDriveable as collsion from #cc_final_data_v3
    				where VehicleDriveable is null) a
     where claimid not in (select distinct claimid from ( select claimid, coalesce(count(distinct(case when VehicleDriveable='1' then claimid end) ),0) as collision
    	         				from (select claimid, VehicleDriveable from #cc_final_data_v3
    				        			where VehicleDriveable is not null) a
    						  group by claimid)  a))  ) b""")

    # Ext_EntireSideDamaged#
    cursor.execute("""select claimid, b.collision as Ext_EntireSideDamaged
    into #Ext_EntireSideDamaged
    from
    (select a.*
    	from 
    			( select claimid, coalesce(count(distinct(case when Ext_EntireSideDamaged='1' then claimid end) ),0) as collision
    	         	from (select claimid, Ext_EntireSideDamaged from #cc_final_data_v3
                          where Ext_EntireSideDamaged is not null) a
                  group by claimid) a
     union 

    (select * from ( select distinct claimid, Ext_EntireSideDamaged as collsion from #cc_final_data_v3
    				where Ext_EntireSideDamaged is null) a
     where claimid not in (select distinct claimid from ( select claimid, coalesce(count(distinct(case when Ext_EntireSideDamaged='1' then claimid end) ),0) as collision
    	         				from (select claimid, Ext_EntireSideDamaged from #cc_final_data_v3
    				        			where Ext_EntireSideDamaged is not null) a
    						  group by claimid)  a))  ) b""")

    ##VehicleParked
    cursor.execute("""select claimid, b.collision as VehicleParked
    into #VehicleParked
    from
    (select a.*
    	from 
    			( select claimid, coalesce(count(distinct(case when VehicleParked='1' then claimid end) ),0) as collision
    	         	from (select claimid, VehicleParked from #cc_final_data_v3
                          where VehicleParked is not null) a
                  group by claimid) a
     union 

    (select * from ( select distinct claimid, VehicleParked as collsion from #cc_final_data_v3
    				where VehicleParked is null) a
     where claimid not in (select distinct claimid from ( select claimid, coalesce(count(distinct(case when VehicleParked='1' then claimid end) ),0) as collision
    	         				from (select claimid, VehicleParked from #cc_final_data_v3
    				        			where VehicleParked is not null) a
    						  group by claimid)  a))  ) b""")

    ##TotalLoss
    cursor.execute("""select claimid, b.collision as TotalLoss
    into #TotalLoss
    from
    (select a.*
    	from 
    			( select claimid, coalesce(count(distinct(case when TotalLoss='1' then claimid end) ),0) as collision
    	         	from (select claimid, TotalLoss from #cc_final_data_v3
                          where TotalLoss is not null) a
                  group by claimid) a
     union 

    (select * from ( select distinct claimid, TotalLoss as collsion from #cc_final_data_v3
    				where TotalLoss is null) a
     where claimid not in (select distinct claimid from ( select claimid, coalesce(count(distinct(case when TotalLoss='1' then claimid end) ),0) as collision
    	         				from (select claimid, TotalLoss from #cc_final_data_v3
    				        			where TotalLoss is not null) a
    						  group by claimid)  a))  ) b""")

    ##Ext_LowImpactIncident
    cursor.execute("""select claimid, b.collision as Ext_LowImpactIncident
    into #Ext_LowImpactIncident
    from
    (select a.*
    	from 
    			( select claimid, coalesce(count(distinct(case when Ext_LowImpactIncident='1' then claimid end) ),0) as collision
    	         	from (select claimid, Ext_LowImpactIncident from #cc_final_data_v3
                          where Ext_LowImpactIncident is not null) a
                  group by claimid) a
     union 

    (select * from ( select distinct claimid, Ext_LowImpactIncident as collsion from #cc_final_data_v3
    				where Ext_LowImpactIncident is null) a
     where claimid not in (select distinct claimid from ( select claimid, coalesce(count(distinct(case when Ext_LowImpactIncident='1' then claimid end) ),0) as collision
    	         				from (select claimid, Ext_LowImpactIncident from #cc_final_data_v3
    				        			where Ext_LowImpactIncident is not null) a
    						  group by claimid)  a))  ) b""")

    cursor.execute("""select claimid, matchreasons
    into #iso_data
    from GWCC_ProdCopy.dbo.cc_claimisomatchreport
    where ClaimID in (select distinct ClaimID from #cc_final_data_v3)""")

    cursor.execute("""select distinct claimid, matchreasons into #iso_distinct
    from 
    #iso_data
    select * from #iso_distinct
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

    cursor.execute("""select a.*, #final_rear_end_coll.rear_collision_final,
    #final_coll_ind.collision_final,
    #VehicleDriveable.VehicleDriveable,
    #Ext_EntireSideDamaged.Ext_EntireSideDamaged,
    #VehicleParked.VehicleParked,
    #TotalLoss.TotalLoss,
    #Ext_LowImpactIncident.Ext_LowImpactIncident,
    #concat_iso.iso_concat,
    #iso_matchreason_f.ISO_address,
    #iso_matchreason_f.iso_dl,
    #iso_matchreason_f.ISO_Licenseplatenumber,
    #iso_matchreason_f.ISO_Losslocation,
    #iso_matchreason_f.ISO_Name,
    #iso_matchreason_f.ISO_Phone,
    #iso_matchreason_f.ISO_SSN,
    #iso_matchreason_f.ISO_VIN,
    #cov_rollup2.AUTO_BI,
    #cov_rollup2.AUTO_PD,
    #cov_rollup2.AUTO_COLLISION,
    #cov_rollup2.COMPREHENSIVE,
    #cov_rollup2.MEDPAY,
    #cov_rollup2.PIP,
    #cov_rollup2.RENTAL_REIMB,
    #cov_rollup2.TOW_LABOR,
    #cov_rollup2.ALL_OTHER,
    #cov_rollup2.BUILDING,
    #cov_rollup2.CONTENTS,
    #cov_rollup2.GL_BI,
    #cov_rollup2.GL_PD
    into #claim_add_var
    from #claim_aggr_body a

    left join #final_rear_end_coll
    on a.claimid=#final_rear_end_coll.ID
    left join #final_coll_ind
    on a.claimid=#final_coll_ind.ID
    left join #VehicleDriveable
    on a.claimid=#VehicleDriveable.claimid
    left join #Ext_EntireSideDamaged
    on a.claimid=#Ext_EntireSideDamaged.claimid
    left join #VehicleParked
    on a.claimid=#VehicleParked.claimid
    left join #TotalLoss
    on a.claimid=#TotalLoss.claimid
    left join #Ext_LowImpactIncident
    on a.claimid=#Ext_LowImpactIncident.claimid
    left join #concat_iso
    on a.claimid=#concat_iso.claimid
    left join #iso_matchreason_f
    on a.claimid=#iso_matchreason_f.claimid
    left join #cov_rollup2
    on a.claimid=#cov_rollup2.claimid""")

    cursor.execute("""select claimid as claim_id_r,
    sum(RemainingReserves) as RemainingReserves,
    sum(TotalPayments) as TotalPayments,
    sum(TotalRecoveries) as TotalRecoveries,
    sum(FuturePayments) as FuturePayments,
    sum(OpenReserves) as OpenReserves,
    SUM(TotalIncurredNet) as TotalIncurredNet,
    sum(TotalIncurredGross) as TotalIncurredGross,
    sum(BI_Amount) as BI_Amount,
    sum(PD_Amount) as PD_Amount,
    COUNT(distinct exposureid) as exposure_Count,
    COUNT(distinct incidentid) as incident_Count,
    COUNT(distinct ClaimantDenormID_exp) as claimant_count,
    MAX(SPEED) as max_speed,
    coalesce(count(distinct (case when BI_Amount>0 then claimantdenormid_exp end)),0) as Injured_passengers,
    coalesce(count(distinct (case when lossparty='10001' then claimantdenormid_exp end)),0) as insured_cnt,
    coalesce(count(distinct (case when lossparty='10002' then claimantdenormid_exp end)),0) as thirdparty_cnt,

    coalesce(count(distinct (case when claimanttype='10001' then claimantdenormid_exp end)),0) as Insured_c_cnt,
    coalesce(count(distinct (case when claimanttype='10002' then claimantdenormid_exp end)),0) as MOIH_cnt,
    coalesce(count(distinct (case when claimanttype='10003' then claimantdenormid_exp end)),0) as DOIV_cnt,
    coalesce(count(distinct (case when claimanttype='10004' then claimantdenormid_exp end)),0) as OOV_cnt,
    coalesce(count(distinct (case when claimanttype='10005' then claimantdenormid_exp end)),0) as DOV_cnt,
    coalesce(count(distinct (case when claimanttype='10006' then claimantdenormid_exp end)),0) as OCIV_cnt,
    coalesce(count(distinct (case when claimanttype='10007' then claimantdenormid_exp end)),0) as OCOV_cnt,
    coalesce(count(distinct (case when claimanttype='10008' then claimantdenormid_exp end)),0) as PED_cnt,
    coalesce(count(distinct (case when claimanttype='10014' then claimantdenormid_exp end)),0) as Other_third_cnt,


    coalesce(count(distinct (case when exposuretype='1' then exposureid end)),0) as Expo_BI,
    coalesce(count(distinct (case when exposuretype='3' then exposureid end)),0) as Expo_Pers_Prop,
    coalesce(count(distinct (case when exposuretype='4' then exposureid end)),0) as Expo_Property,
    coalesce(count(distinct (case when exposuretype='5' then exposureid end)),0) as Expo_Vehicle,
    coalesce(count(distinct (case when exposuretype='9' then exposureid end)),0) as Expo_General,
    coalesce(count(distinct (case when exposuretype='10' then exposureid end)),0) as Expo_PIP,
    coalesce(count(distinct (case when exposuretype='11' then exposureid end)),0) as Expo_MEDPAY,
    coalesce(count(distinct (case when exposuretype='10009' then exposureid end)),0) as Expo_tow_labr,

    coalesce(count(distinct(case when lossparty='10001' and vehicletype='10003' then claimid  end) ),0) as Insured_New_veh,
    coalesce(count(distinct(case when lossparty='10002' and vehicletype='10003' then claimid  end) ),0) as Thirdparty_New_veh,
    coalesce(count(distinct(case when lossparty='10001' and vehicletype in ('10005','10006') then claimid  end) ),0) as Insured_rented,
    coalesce(count(distinct(case when lossparty='10002' and vehicletype in ('10005','10006') then claimid  end) ),0) as Thirdparty_rented,
    coalesce(count(distinct(case when lossparty='10001' and vehicletype='10004' then claimid  end) ),0) as Insured_owned,
    coalesce(count(distinct(case when lossparty='10002' and vehicletype='10004' then claimid  end) ),0) as Thirdparty_owned,
    coalesce(count(distinct(case when lossparty='10001' and vehicletype='10002' then claimid  end) ),0) as Insured_listed,
    coalesce(count(distinct(case when lossparty='10002' and vehicletype='10002' then claimid  end) ),0) as Thirdparty_listed,
    coalesce(count(distinct(case when lossparty='10001' and vehicletype='10009' then claimid  end) ),0) as Insured_other,
    coalesce(count(distinct(case when lossparty='10002' and vehicletype='10009' then claimid  end) ),0) as Thirdparty_other,

    MAX(vehicle_age) as max_vehicle_age,
    COUNT(distinct vehicleid) as vehicle_count,
    MAX(Pre_claims_cnt) as max_Pre_claims_cnt,
    Max(claimant_tenure) as claimant_tenure_wt_qbe,
    max(prev_claim_duration) as previous_claim_duration,
    MAX(PREV_CLM_PER_YR) AS MAX_PREV_CLM_PER_YR,

    coalesce(count(distinct(case when lossparty='10001' and postal_code_match=1 then claimid  end) ),0) as insured_postal_match,
    coalesce(count(distinct(case when lossparty='10002' and postal_code_match=1 then claimid  end) ),0) as thirdparty_postal_match,
    coalesce(count(distinct(case when lossparty='10001' and city_match=1 then claimid  end) ),0) as insured_city_match,
    coalesce(count(distinct(case when lossparty='10002' and city_match=1 then claimid  end) ),0) as thirdparty_city_match,
    coalesce(count(distinct(case when lossparty='10001' and state_match=1 then claimid  end) ),0) as insured_state_match,
    coalesce(count(distinct(case when lossparty='10002' and state_match=1 then claimid  end) ),0) as thirdparty_state_match

    into #clm_aggr_var
    from #cc_final_data_v3
    group by claimid""")

    cursor.execute("""select a.*,b.*
    into #claim_aggr_final
    from #claim_add_var a left join #clm_aggr_var b
    on a.claimid=b.claim_id_r""")

    cursor.execute("""select ClaimID, sum(Ext_ITDCreditToExpenseRecovery) as credit_to_expense_rec,
    sum(Ext_ITDCreditToLossRecovery) as credit_to_loss_rec,
             sum(Ext_ITDDeductibleRecovery) as deductible_rec,
             sum(Ext_ITDPaid) as Total_Paid,
             sum(Ext_ITDRecovered) as total_recovered,
             sum(Ext_ITDSalvageRecovery) as salvage_rec,
             sum(Ext_ITDSubrogationRecovery) as subro_rec

    into #recovery_details
    from GWCC_ProdCopy.dbo.cc_reserveline where claimid in 
             (select distinct claimid from #claim_aggr_final)
             group by ClaimID""")

    cursor.execute("""select a.*, b.credit_to_expense_rec,
    b.credit_to_loss_rec,
             b.deductible_rec,
             b.Total_Paid,
             b.total_recovered,
             b.salvage_rec,
             b.subro_rec
             into #claim_aggr_final_sub
    from #claim_aggr_final a left join #recovery_details b
    on a.claimid=b.claimid""")

    cursor.execute("""select ClaimID, PriContributingFactors INTO #CONTRIB 
    from GWCC_ProdCopy.dbo.cc_contribfactor
    WHERE ClaimID IN (SELECT DISTINCT ClaimID FROM #claim_aggr_final_sub)""")

    cursor.execute("""select claimid, (case when PriContributingFactors in ('10077','10078','10079') then 1 else 0 end) as glass_damage,
    (case when PriContributingFactors in ('10153','10024') then 1 else 0 end) as rear_end_pc,
    (case when PriContributingFactors in ('10094') then 1 else 0 end) as intersection_accident,
    (case when PriContributingFactors in ('10083','10154','10169') then 1 else 0 end) as speed_headon_racing,
    (case when PriContributingFactors in ('10141') then 1 else 0 end) as parked_vehicle,
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
    (case when glass_damage=0 then 0 else 1 end) as glass_damage,
    (case when rear_end_pc=0 then 0 else 1 end) as rear_end_pc,
    (case when intersection_accident=0 then 0 else 1 end) as intersection_accident,
    (case when speed_headon_racing=0 then 0 else 1 end) as speed_headon_racing,
    (case when parked_vehicle=0 then 0 else 1 end) as parked_vehicle,
    (case when following_closely=0 then 0 else 1 end) as following_closely
             into #claim_contri
             from #claim_contri_1""")

    cursor.execute("""select a.*, 
    coalesce(b.glass_damage, 0) as glass_damage, 
    coalesce(b.rear_end_pc, 0) as rear_end_pc, 
    coalesce(b.intersection_accident, 0) as intersection_accident,
    coalesce(b.speed_headon_racing,0) as speed_headon_racing, 
    coalesce(b.parked_vehicle,0) as parked_vehicle, 
    coalesce(b.following_closely,0) as following_closely
    into #contri_aggr
    from #claim_aggr_final_sub a left join #claim_contri b
    on a.claimid=b.ClaimID""")

    cursor.execute("""select *,
    (case when DATEPART(hour, lossdate) = 0  and DATEPART(MINUTE, lossdate) = 0 and DATEPART(second, lossdate) > 0 then 'twelveam'
    when DATEPART(hour, lossdate) >= 22 or DATEPART(hour, lossdate) < 4  then 'midnight'
     when DATEPART(hour, lossdate) >=4 and DATEPART(hour, lossdate)<8 then 'early morning'
     when DATEPART(hour, lossdate)>=8 and DATEPART(hour, lossdate) <22 then 'dayhours' 

     end) as tod, 
    (case when collision_final=1 or AUTO_COLLISION>0 then 1 else 0 end) as Collision_indicator,
    (case when rear_collision_final=1 or rear_end_pc=1 then 1 else 0 end) as rear_end_Coll_ind,
    (case when losscause_group='10046' or glass_damage=1 then 1 else 0 end) as glass_damage_ind
    into #claim_aggr_new_data_1
    from #contri_aggr""")

    cursor.execute("""select a.*, (case when a.thirdparty_cnt>0 then 'thirdparty_claim' else 'insured_claim' end) as claimtype
    into  #claim_aggr_new_data_1_1
    from  #claim_aggr_new_data_1 a""")

    cursor.execute("""select *, (case when Collision_indicator=1 or rear_end_Coll_ind=1 then 1 else 0 end) as collision_ind_f,
    		 (case when COMPREHENSIVE>0 and AUTO_BI=0 and 	AUTO_PD=0 and 	AUTO_COLLISION=0 and MEDPAY=0 and 	PIP=0 and 	RENTAL_REIMB=0 and 	TOW_LABOR=0 and 	ALL_OTHER=0 and 	BUILDING=0 and 	CONTENTS=0 and 	GL_BI=0 and 	GL_PD=0 then 1 else 0 end) as COMPREHENSIVE_only,
             (case when AUTO_COLLISION>0 and AUTO_BI=0 and 	AUTO_PD=0 and 	COMPREHENSIVE=0 and MEDPAY=0 and 	PIP=0 and 	RENTAL_REIMB=0 and 	TOW_LABOR=0 and 	ALL_OTHER=0 and 	BUILDING=0 and 	CONTENTS=0 and 	GL_BI=0 and 	GL_PD=0 then 1 else 0 end) as collision_only,
             (case when datediff(day, EffectiveDate, ExpirationDate)>359 then 1 else 0 end) as policy_period_gt_0,
             (case when Datepart(DW,ReportedDate) in (1,6,7) then 1 else 0 end) as weekend_flag,
             (case when Datepart(M,ReportedDate)>6 then 1 else 0 end) as jul_dec,
             (case when ReportedDate<= DATEADD(DAY,round(0.2*(datediff(day, EffectiveDate, ExpirationDate)),0), EffectiveDate) or
             ReportedDate>= DATEADD(DAY,-round(0.3*(datediff(day, EffectiveDate, ExpirationDate)),0), ExpirationDate) then 1 else 0 end) as policy_period_extreme
             into #claim_aggr_new_data_99
             from #claim_aggr_new_data_1_1 """)

    cursor.execute("""select *, (collision_ind_f + rear_end_Coll_ind) as collision_overall,
             (case when collision_ind_f=1 and vehicle_count>2 then 1 else 0 end) as suspicious_collision
              into #claim_aggr_new_data
    from #claim_aggr_new_data_99
             """)

    cursor.execute("""select * into #str_dat from #claim_aggr_new_data""")

    cursor.execute("""select ID as claimid

             into #test_b
             from GWCC_ProdCopy.dbo.cc_claim
             where  CloseDate is null
    		and (Losstype='10001'
    		and LOBCode in ('10008', '10014', '10016') )

             """)

    cursor.execute("""select * into #claim_filt
    from GWCC_ProdCopy.dbo.cc_claim
    where  CloseDate is null
     and (Losstype='10001'
    and LOBCode in ('10008', '10014', '10016') )
    and (LitigationStatus not in ('1','2') or LitigationStatus is null)
    """)

    cursor.execute("""select claim.SubrogationStatus,
    cctl_subrogationstatus.[DESCRIPTION] as SubrogationStatus_desc,
    claim.SIEscalateSIU,
    claim.ReportedDate,
    datediff(day, claim.LossDate, claim.ReportedDate) AS Report_lag,
    claim.Ext_ResponsibleParty,
    cctl_ext_responsibleparty.[DESCRIPTION] AS responsibleparty_desc,
    --cc_contribfactor.PriContributingFactors ,
    --cctl_pricontributingfactors.[DESCRIPTION] AS pricontribfactor_desc,
    claim.LossLocationID,
    claim.SIUStatus,
    cctl_siustatus.[DESCRIPTION] as SIUStatus_desc,
    claim.Ext_LossLocState,
    cctl_state.[DESCRIPTION] as Ext_LossLocState_desc,
    claim.JurisdictionState,
    cctl_state_j.[DESCRIPTION] as JurisdictionState_desc,
    claim.ISOReceiveDate,
    claim.PolicyID,
    claim.LossType,
    cctl_losstype.[DESCRIPTION] as LossType_desc,
    claim.Fault,
    claim.FaultRating,
    claim.Ext_MoreThan3VehsInvolved,
    claim.CloseDate,
    claim.LocationOfTheft,
    cctl_locationoftheft.[DESCRIPTION] as LocationOfTheft_desc,
    claim.Ext_LocationofLoss,
    cctl_ext_locationofloss.[DESCRIPTION] as ext_locationofloss_desc,
    claim.LitigationStatus,
    cctl_litigationstatus.[DESCRIPTION] as litigationstatus_desc,
    claim.LossCause,
    cctl_losscause.[DESCRIPTION] as LossCause_desc,
    (case when cctl_losscause.[DESCRIPTION]='Converted' then 'Converted'
    when cctl_losscause.[DESCRIPTION]='Motor Vehicle Accident' then 'Motor_vehicle_accident'
    when cctl_losscause.[DESCRIPTION]='Glass Damage Other Than Breakage' then 'Glass_Damage'
    when cctl_losscause.[DESCRIPTION]='Motor Vehicle Damage Other Than Collision' then 'Motor_vehicle_accident'
    when cctl_losscause.[DESCRIPTION]='Towing and Labor' then 'Towing_and_Labor'
    when cctl_losscause.[DESCRIPTION]='Weather Event' then 'Catastrophe'
    when cctl_losscause.[DESCRIPTION]='Animal Caused Damage or Injury' then 'Animal'
    when cctl_losscause.[DESCRIPTION]='Fire Damage' then 'Fire_or_Explosion'
    when cctl_losscause.[DESCRIPTION]='Struck by Falling Object' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Loading Or Unloading Damage' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Animal Damage or Injury' then 'Animal'
    when cctl_losscause.[DESCRIPTION]='Mechanical Defect Or Failure' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Negligent Maintenance' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Water Damage' then 'Water_Damage'
    when cctl_losscause.[DESCRIPTION]='Hanging Lines Struck ' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Flood' then 'Catastrophe'
    when cctl_losscause.[DESCRIPTION]='Weight Overload of Equipment' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Pollution - Environmental Damage' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Water Vehicle Crash' then 'Motor_vehicle_accident'
    when cctl_losscause.[DESCRIPTION]='Explosion / Blowout or Cratering' then 'Fire_or_Explosion'
    when cctl_losscause.[DESCRIPTION]='Riot Civil Commotion & Strike' then 'Human_violence'
    when cctl_losscause.[DESCRIPTION]='Collapse / Sinkhole / Subsidence ' then 'Non_accident_damage'
    when cctl_losscause.[DESCRIPTION]='Volcanic Action' then 'Catastrophe'
    when cctl_losscause.[DESCRIPTION]='Earth Movement / Landslide' then 'Catastrophe'
    when cctl_losscause.[DESCRIPTION]='Vandalism and or Malicious Mischief' then 'Human_violence'
    when cctl_losscause.[DESCRIPTION]='Water Vehicle Marine Perils' then 'Water_Damage'
    when cctl_losscause.[DESCRIPTION]='Earthquake' then 'Catastrophe'
    when cctl_losscause.[DESCRIPTION]='Terrorism Loss' then 'Human_violence'
     end) as losscause_group,
    claim.LossDate,
    claim.ClaimantDenormID,
    claim.ISOStatus,
    cctl_isostatus.[DESCRIPTION] as isostatus_desc,
    claim.ExposureBegan,
    claim.SIScore,
    claim.Ext_SourceClaimNumber,
    claim.ISOKnown,
    claim.ID as claimid,
    claim.ClaimNumber,
    claim.SIULifeCycleState,
    cctl_claimlifecyclestate.[DESCRIPTION] as SIULifeCycleState_desc,
    claim.LOBCode,
    cctl_lobcode.[DESCRIPTION] as LOBcode_desc,
    --claim.[DESCRIPTION] as claimdesc,
    --claim.PoliceDeptInfo,
    claim.[State] as claim_status_code ,
    cctl_claimstate.[DESCRIPTION] as claim_status_desc,
    claim.InsuredDenormID,
    lossloc.[DESCRIPTION] as lossloc_DESC,
    lossloc.City as lossloc_City,
    lossloc.PostalCode as lossloc_PostalCode,
    lossloc.[State] as lossloc_State,
    lossloc.AddressLine1 as lossloc_AddressLine1

    into #claim_f
    from #claim_filt claim left join GWCC_ProdCopy.dbo.cctl_lobcode
    on claim.LOBcode=GWCC_ProdCopy.dbo.cctl_lobcode.ID

    left join GWCC_ProdCopy.dbo.cctl_claimstate
    on claim.[State]=GWCC_ProdCopy.dbo.cctl_claimstate.ID

    left join GWCC_ProdCopy.dbo.cctl_ext_responsibleparty cctl_ext_responsibleparty
    on claim.ext_responsibleparty=cctl_ext_responsibleparty.ID

    left join GWCC_ProdCopy.dbo.cctl_subrogationstatus
    on claim.SubrogationStatus=GWCC_ProdCopy.dbo.cctl_subrogationstatus.ID

    --left join GWCC_ProdCopy.dbo.cc_contribfactor cc_contribfactor
    --on claim.ID=cc_contribfactor.ClaimID

    --LEFT join GWCC_ProdCopy.dbo.cctl_pricontributingfactors cctl_pricontributingfactors
    --on cc_contribfactor.PriContributingFactors=cctl_pricontributingfactors.[ID]

    left join GWCC_ProdCopy.dbo.cctl_siustatus
    on claim.SIUStatus=GWCC_ProdCopy.dbo.cctl_siustatus.ID

    left join GWCC_ProdCopy.dbo.cctl_state
    on claim.Ext_LossLocState=GWCC_ProdCopy.dbo.cctl_state.ID

    left join GWCC_ProdCopy.dbo.cctl_losstype
    on claim.LossType=GWCC_ProdCopy.dbo.cctl_losstype.ID

    left join GWCC_ProdCopy.dbo.cctl_locationoftheft
    on claim.LocationOfTheft=GWCC_ProdCopy.dbo.cctl_locationoftheft.ID

    left join GWCC_ProdCopy.dbo.cctl_litigationstatus
    on claim.litigationstatus=GWCC_ProdCopy.dbo.cctl_litigationstatus.ID

    left join GWCC_ProdCopy.dbo.cctl_losscause
    on claim.LossCause=GWCC_ProdCopy.dbo.cctl_losscause.ID

    left join GWCC_ProdCopy.dbo.cctl_isostatus
    on claim.isostatus=GWCC_ProdCopy.dbo.cctl_isostatus.ID

    left join GWCC_ProdCopy.dbo.cctl_claimlifecyclestate
    on claim.SIULifeCycleState=GWCC_ProdCopy.dbo.cctl_claimlifecyclestate.ID

    left join GWCC_ProdCopy.dbo.cctl_state cctl_state_j
    on claim.JurisdictionState=cctl_state_j.ID

    left join GWCC_ProdCopy.dbo.cctl_ext_locationofloss
    on claim.ext_locationofloss=GWCC_ProdCopy.dbo.cctl_ext_locationofloss.ID

    left join GWCC_ProdCopy.dbo.cc_address lossloc
    on claim.losslocationid=lossloc.ID""")

    cursor.execute("""select * into #claim_fin from #claim_f where litigationstatus_desc is null or  
    litigationstatus_desc not in ('In litigation','Litigation complete')""")


def get_query(cnxn, query):
    cursor = cnxn.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        name = cursor.fetchall()
        for i in range(0, len(name)):
            name[i] = tuple(name[i])
        return pd.DataFrame(name, columns=names)
    finally:
        if cursor is not None:
            cursor.close()


def unescape(s):
    s = str(s)
    s = re.sub(r"<.*?>", "", s)
    s = s.replace("&lt;", "less")
    s = s.replace("&gt;", "greater")
    # this has to be last:
    s = s.replace("&amp;", "and")
    s = s.replace("x0D", "")
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")
    s = s.replace("*", " ")
    s = s.replace("&#;", '')
    s = s.replace("\t", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("null,", "")
    s = s.replace('\'', "")
    # next step removes all characters other than alphanumerical and space
    s = re.sub('[^A-Za-z0-9,.[space]]+', '', s)
    s = re.sub('[ \t]+', ' ', s)
    return s


def getFeatures(value, featureList, idx):
    features = {}
    if idx % 10000 == 0: print(idx)
    for f in featureList:
        features[f] = value[f][idx]
    return features


def save(path, ext='png', close=True, verbose=True):
    directory = os.path.split(path)[0]
    filename = "%s.%s" % (os.path.split(path)[1], ext)
    if directory == '':
        directory = '.'

    if not os.path.exists(directory):
        os.makedirs(directory)

    savepath = os.path.join(directory, filename)

    if verbose:
        print("Saving figure to '%s'..." % savepath),

    plt.savefig(savepath)

    if close:
        plt.close()

    if verbose:
        print("Done")


def roc_plot_test(fpr, tpr):
    fig = plt.plot(fpr, tpr)
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.title('ROC curve using test data for SIU classifier')
    plt.xlabel('False Positive Rate (1 - Specificity)')
    plt.ylabel('True Positive Rate (Sensitivity)')
    plt.grid(True)
    save(output + "roc_test", ext="png", verbose=True)


def roc_plot_train(fpr, tpr):
    fig = plt.plot(fpr, tpr)
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.title('ROC curve using train data for SIU classifier')
    plt.xlabel('False Positive Rate (1 - Specificity)')
    plt.ylabel('True Positive Rate (Sensitivity)')
    plt.grid(True)
    save(output + "roc_train", ext="png", verbose=True)


def charac(actual, pred):
    # accuracy of the model

    res_1 = np.column_stack((actual, pred))
    y_1 = res_1[res_1[:, 0] == 1]
    y_0 = res_1[res_1[:, 0] == 0]
    y_1_i = y_1[:, 0]
    y_1_j = y_1[:, 1]
    y_0_i = y_0[:, 0]
    y_0_j = y_0[:, 1]
    acc_0 = metrics.accuracy_score(y_0_i, y_0_j)
    acc_1 = metrics.accuracy_score(y_1_i, y_1_j)

    acc = metrics.accuracy_score(actual, pred)

    # Null accuracy of the model. Accuracy should be much higher than Null accuracy
    n_acc = max(actual.mean(), 1 - actual.mean())

    # Confusion Matrix
    confusion = metrics.confusion_matrix(actual, pred)

    TP = confusion[1, 1]
    TN = confusion[0, 0]
    FP = confusion[0, 1]
    FN = confusion[1, 0]

    # misclassification rate
    mis = (FP + FN) / float(TP + TN + FP + FN)

    # sensitivity or re-call score or true positive rate
    sensitivity = metrics.recall_score(actual, pred)

    # specificity
    specificity = TN / float(TN + FP)

    # false positive rate=1-specificity
    fpr = 1 - specificity

    # False negative rate
    fnr = FN / (FN + TP)

    # F1 score
    f1 = 2 * TP / ((2 * TP) + FP + FN)

    # Precision
    precision = TP / float(TP + FP)

    # Precision_0
    precision_0 = TN / float(TN + FN)

    # Area under curve for test
    AUC_test = roc_auc_score(actual, pred)

    return acc_0, acc_1, acc, n_acc, confusion, mis, sensitivity, specificity, fpr, fnr, f1, precision, precision_0, AUC_test


def returnFeatureImportance(gbm_model, vec):
    feature_importance = gbm_model.feature_importances_
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    sorted_idx = np.argsort(feature_importance)
    featuresNames = []
    featureImps = []
    for item in sorted_idx[::-1][:]:
        featuresNames.append(np.asarray(vec.feature_names_)[item])
        featureImps.append(feature_importance[item])
    featureImportance = pd.DataFrame([featuresNames, featureImps]).transpose()
    featureImportance.columns = ['FeatureName', 'Relative Importance']
    return featureImportance


def featureplot(gbm_model, vec):
    feature_importance = gbm_model.feature_importances_
    # make importances relative to max importance
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    sorted_idx = np.argsort(feature_importance)
    pos = np.arange(sorted_idx.shape[0]) + .5
    plt.subplot(1, 2, 2)
    plt.barh(pos, feature_importance[sorted_idx], align='center')
    plt.yticks(pos, vec.feature_names[sorted_idx])
    plt.xlabel('Relative Importance')
    plt.title('Variable Importance')
    plt.show()
    save(output + "fp", ext="png", verbose=True)


def generatePDP(modelObj, featureVector, trainingX, outputFolder, importance):
    # Create Partial Dependenct directory to hold all PD plots
    pdDir = outputFolder + "PDPlots\\"
    # if the output Partial Dependency Directory doesn't exist, create it
    if not os.path.exists(os.path.dirname(pdDir)):
        print("Output Directory: " + pdDir + " Doesn't exist. Creating it now")
        os.mkdir(os.path.dirname(pdDir))
    # to generate feature importance
    featureImportanceDF = returnFeatureImportance(modelObj, featureVector)
    featureImportanceDF.to_csv(outputFolder + "FeatureImportance.csv")
    # Select only the important features
    featureImportanceDF = featureImportanceDF[featureImportanceDF['Relative Importance'] > importance]
    # to generate PDP, create a list of features
    featureId = []
    featureName = []
    for k, feature in enumerate(featureVector.feature_names_, ):
        featureId.append(k)
        featureName.append(feature)
    features = pd.DataFrame([featureId, featureName]).transpose()
    features.columns = ['FeatureId', 'FeatureName']
    # Get the feature id for the important features
    featureImportanceDF = pd.merge(featureImportanceDF, features, how='left', on='FeatureName')

    # Generate PD Plots
    for i in range(featureImportanceDF['FeatureName'].size):
        feature = [featureImportanceDF['FeatureId'][i]]
        featName = featureImportanceDF['FeatureName'][i].replace('/', '_')
        fig, axs = plot_partial_dependence(modelObj, trainingX, feature, featureVector.feature_names_, n_jobs=-1)
        plt.subplots_adjust(top=0.9)
        # axs.set_xlabel(featName)
        # save the plot in the output directory with the feature name as file name
        fig.savefig(pdDir + featName + "_PD.png")
        plt.close(fig)


def qtls(df, col_name, ptl):
    # col1 = col_name + '_ot'
    df[col_name] = scipy.stats.mstats.winsorize(df[col_name], limits=ptl)


def get_new_dist(new_docs=None, index=None, model=None):
    from tqdm import tqdm
    from gensim import corpora

    if model is None or new_docs is None or index is None:
        return None
    lda_data_all = pickle.load(open(model, 'rb'))
    lda_model_trained = lda_data_all['model']
    del lda_data_all
    print('Tokenizing documents...\n')
    text_tokenized = [txt.split() for txt in tqdm(new_docs)]

    dic_new = corpora.Dictionary(text_tokenized)
    print('Creating new corpus...\n')
    corpus_new = [dic_new.doc2bow(txt) for txt in tqdm(text_tokenized)]

    new_doc_topic_dist = np.array(lda_model_trained.get_document_topics(corpus_new, minimum_probability=0))[:, :, 1]
    print('Generated new document distribution, saved as: new_doc_dist2017.csv')
    return pd.DataFrame(data=new_doc_topic_dist, index=index)


# Prepare data by executing SQL queries
exec_query()

q1 = """select LOBCode, ID as claimid, [Description],
         ClaimNumber,ReportedDate 

         from GWCC_ProdCopy.dbo.cc_claim
         where  CloseDate is null
		and (Losstype='10001'
		and LOBCode in ('10008', '10014', '10016') )

         """
loss_desc = get_query(cnxn, q1)

q2 = """select  claimid as claimid,authoringdate,
         body

         from GWCC_ProdCopy.dbo.cc_note
         where claimid in (select distinct claimid from #test_b)

         """
adj_notes = get_query(cnxn, q2)

adj_notes = adj_notes.set_index('claimid')

adj_notes = pd.DataFrame(adj_notes['body'].astype(str).groupby(adj_notes.index).agg(' , '.join)).reset_index()

new_unstr = loss_desc.merge(adj_notes, how='left', on='claimid')

new_unstr.columns = ['LOBCode', 'ClaimID', 'Description', 'ClaimNumber', 'ReportedDate', 'Adjuster_Notes']

new_unstr['Adjuster_Notes'] = new_unstr['Adjuster_Notes'].apply(unescape)
# del(new_unstr['body'])
new_unstr['Description'] = new_unstr['Description'].apply(unescape)

new_topic_dist = get_new_dist(new_docs=new_unstr['Adjuster_Notes'], index=new_unstr['ClaimID'], model=LDA_MODEL_LOCATION)

qw = """select * from #claim_f"""
we = pd.read_sql(qw, cnxn)

zx = """select * from #claim_fin"""
xc = pd.read_sql(zx, cnxn)
claim_aggr_new_data = """select * from #str_dat"""
Str_Data = pd.read_sql(claim_aggr_new_data, cnxn)

Selected_Col = ["claimid", "ClaimNumber", "ReportedDate",
                "LossDate", "CloseDate", "PolicyID", "PolicyNumber",
                "OrigEffectiveDate", "EffectiveDate", "ExpirationDate",
                "claim_status_code", "LOBCode", "ext_broadlobcode_desc",
                "Report_interval", "Report_lag", "LossType", "SubrogationStatus",
                "Claim_Opening_days", "SIEscalateSIU", "PC_issue_report",
                "PC_report_expire", "Ext_ResponsibleParty", "LossLocationID",
                "SIUStatus", "Ext_LossLocState", "JurisdictionState",
                "ISOReceiveDate", "prev_renewals", "Ext_MoreThan3VehsInvolved",
                "LocationOfTheft", "Ext_LocationofLoss", "LitigationStatus",
                "LossCause", "losscause_group", "ISOStatus", "SIScore",
                "Ext_SourceClaimNumber", "Fault", "ISOKnown", "SIULifeCycleState",
                "InsuredDenormID", "lossloc_City", "lossloc_PostalCode",
                "lossloc_State", "Ext_Underwritingstate", "Ext_ClaimsMade",
                "PolicyType", "Ext_CustomerNumber", "TotalVehicles", "sublobcode",
                "Body_Head", "Body_Neck", "Body_Upper_ext", "Body_Trunk",
                "Body_Lower_ext", "Body_Unknown", "Body_Multiple", "rear_collision_final",
                "collision_final", "VehicleDriveable", "Ext_EntireSideDamaged",
                "VehicleParked", "TotalLoss", "Ext_LowImpactIncident", "iso_concat",
                "ISO_address", "iso_dl", "ISO_Licenseplatenumber", "ISO_Losslocation",
                "ISO_Name", "ISO_Phone", "ISO_SSN", "ISO_VIN", "RemainingReserves",
                "TotalPayments", "TotalRecoveries", "FuturePayments", "OpenReserves",
                "TotalIncurredNet", "TotalIncurredGross", "exposure_Count",
                "incident_Count", "claimant_count", "max_speed", "insured_cnt",
                "thirdparty_cnt", "Insured_c_cnt", "MOIH_cnt", "DOIV_cnt", "OOV_cnt",
                "DOV_cnt", "OCIV_cnt", "OCOV_cnt", "PED_cnt", "Other_third_cnt", "Expo_BI",
                "Expo_Pers_Prop", "Expo_Property", "Expo_Vehicle", "Expo_General",
                "Expo_PIP", "Expo_MEDPAY", "Expo_tow_labr", "Insured_New_veh",
                "Thirdparty_New_veh", "Insured_rented", "Thirdparty_rented",
                "Insured_owned", "Thirdparty_owned", "Insured_listed", "Thirdparty_listed",
                "Insured_other", "Thirdparty_other", "max_vehicle_age", "vehicle_count",
                "max_Pre_claims_cnt", "insured_postal_match", "thirdparty_postal_match",
                "insured_city_match", "thirdparty_city_match", "insured_state_match",
                "thirdparty_state_match", "AUTO_BI", "AUTO_PD", "AUTO_COLLISION",
                "COMPREHENSIVE", "MEDPAY", "PIP", "RENTAL_REIMB", "TOW_LABOR", "ALL_OTHER",
                "BUILDING", "CONTENTS", "GL_BI", "GL_PD", "credit_to_expense_rec",
                "credit_to_loss_rec", "deductible_rec", "Total_Paid", "total_recovered",
                "salvage_rec", "subro_rec", "glass_damage",
                "rear_end_pc",
                "intersection_accident",
                "speed_headon_racing",
                "parked_vehicle",
                "following_closely",
                "tod",
                "collision_ind_f",
                "rear_end_Coll_ind",
                "glass_damage_ind", "BI_Amount", "PD_Amount", "Injured_passengers", "claimtype", "INSURED_NAME",
                "COMPREHENSIVE_only", "collision_only", "policy_period_gt_0", "weekend_flag", "jul_dec",
                "policy_period_extreme", "claimant_tenure_wt_qbe", "collision_overall", "suspicious_collision",
                "MAX_PREV_CLM_PER_YR", "previous_claim_duration"]

Str_Data = Str_Data[Selected_Col]

# region = pd.read_csv("/home/e06278e/Production/Input/state_mapping.csv", low_memory=False)
region = pd.read_csv(os.path.join(INPUT_DIR, 'state_mapping.csv'), low_memory=False)

Final_Data = pd.merge(Str_Data, region, how='left', on='lossloc_State')

Final_Data['SIU_FLAG'] = 0

order = ["claimid", "ClaimNumber", "SIU_FLAG", "claimtype", "credit_to_expense_rec", "credit_to_loss_rec",
         "deductible_rec", "BI_Amount", "PD_Amount", "Total_Paid", "total_recovered", "salvage_rec", "subro_rec",
         "ReportedDate", "LossDate", "CloseDate", "PolicyID", "PolicyNumber", "INSURED_NAME", "OrigEffectiveDate",
         "EffectiveDate", "ExpirationDate", "claim_status_code", "LOBCode", "ext_broadlobcode_desc", "Report_interval",
         "Report_lag", "LossType", "SubrogationStatus", "Claim_Opening_days", "SIEscalateSIU", "PC_issue_report",
         "PC_report_expire", "Ext_ResponsibleParty", "LossLocationID", "SIUStatus", "Ext_LossLocState",
         "JurisdictionState", "ISOReceiveDate", "prev_renewals", "Ext_MoreThan3VehsInvolved", "LocationOfTheft",
         "Ext_LocationofLoss", "LitigationStatus", "LossCause", "losscause_group", "ISOStatus", "SIScore",
         "Ext_SourceClaimNumber", "Fault", "ISOKnown", "SIULifeCycleState", "InsuredDenormID", "lossloc_City",
         "lossloc_PostalCode", "lossloc_State", "STATE_ABBR", "REGION", "DIVISION", "Ext_Underwritingstate",
         "Ext_ClaimsMade", "PolicyType", "Ext_CustomerNumber", "TotalVehicles", "sublobcode", "Body_Head", "Body_Neck",
         "Body_Upper_ext", "Body_Trunk", "Body_Lower_ext", "Body_Unknown", "Body_Multiple", "VehicleDriveable",
         "Ext_EntireSideDamaged", "VehicleParked", "TotalLoss", "Ext_LowImpactIncident", "iso_concat", "ISO_address",
         "iso_dl", "ISO_Licenseplatenumber", "ISO_Losslocation", "ISO_Name", "ISO_Phone", "ISO_SSN", "ISO_VIN",
         "RemainingReserves", "TotalPayments", "TotalRecoveries", "FuturePayments", "OpenReserves", "TotalIncurredNet",
         "TotalIncurredGross", "exposure_Count", "incident_Count", "claimant_count", "Injured_passengers", "max_speed",
         "insured_cnt", "thirdparty_cnt", "Insured_c_cnt", "MOIH_cnt", "DOIV_cnt", "OOV_cnt", "DOV_cnt", "OCIV_cnt",
         "OCOV_cnt", "PED_cnt", "Other_third_cnt", "Expo_BI", "Expo_Pers_Prop", "Expo_Property", "Expo_Vehicle",
         "Expo_General", "Expo_PIP", "Expo_MEDPAY", "Expo_tow_labr", "Insured_New_veh", "Thirdparty_New_veh",
         "Insured_rented", "Thirdparty_rented", "Insured_owned", "Thirdparty_owned", "Insured_listed",
         "Thirdparty_listed", "Insured_other", "Thirdparty_other", "max_vehicle_age", "vehicle_count",
         "max_Pre_claims_cnt", "claimant_tenure_wt_qbe", "insured_postal_match", "thirdparty_postal_match",
         "insured_city_match", "thirdparty_city_match", "insured_state_match", "thirdparty_state_match", "AUTO_BI",
         "AUTO_PD", "AUTO_COLLISION", "COMPREHENSIVE", "MEDPAY", "PIP", "RENTAL_REIMB", "TOW_LABOR", "ALL_OTHER",
         "BUILDING", "CONTENTS", "GL_BI", "GL_PD", "glass_damage_ind", "rear_end_pc", "intersection_accident",
         "speed_headon_racing", "parked_vehicle", "following_closely", "tod", "collision_ind_f", "rear_end_Coll_ind",
         "COMPREHENSIVE_only", "collision_only", "policy_period_gt_0", "weekend_flag", "jul_dec",
         "policy_period_extreme", "collision_overall", "suspicious_collision", "MAX_PREV_CLM_PER_YR",
         "previous_claim_duration"]

Final_Data = Final_Data[order]

rd_col = ['ClaimNumber', 'ReportedDate']
rd = Final_Data[rd_col]

unstr_claim_adj_df_full = pd.read_csv(os.path.join(INPUT_DIR, 'Claim_adjuster_notes_final_1.csv'),
                                      encoding='ISO-8859-1')
str_claim_adj_df = pd.read_csv(os.path.join(INPUT_DIR, 'subset_6.csv'), encoding='ISO-8859-1')

if 'Unnamed: 0' in pd.Series(list(Final_Data.columns)).unique():
    del Final_Data['Unnamed: 0']

if 'Unnamed: 0' in pd.Series(list(Final_Data.columns)).unique():
    del Final_Data['Unnamed: 0']

join_claim_adj_df_new = new_unstr.merge(Final_Data, how='inner',
                                        left_on=new_unstr.ClaimNumber,
                                        right_on=Final_Data.ClaimNumber,
                                        left_index=False,
                                        right_index=False)

col_list_2 = ['claimid',
              'ClaimNumber_x',
              'Description',
              'Adjuster_Notes',
              'SIU_FLAG']
join_claim_adj_df_new = join_claim_adj_df_new[col_list_2]
# rename columns
join_claim_adj_df_new.columns = ['ClaimId',
                                 'ClaimNumber_x',
                                 'Description',
                                 'Adjuster_Notes',
                                 'SIU_FLAG']

join_claim_adj_df = unstr_claim_adj_df_full.merge(str_claim_adj_df, how='inner',
                                                  left_on=unstr_claim_adj_df_full.ClaimNumber,
                                                  right_on=str_claim_adj_df.ClaimNumber,
                                                  left_index=False,
                                                  right_index=False)

col_list = ['claimid', 'ClaimNumber_x', 'Description', 'Adjuster_Notes', 'SIU_FLAG']
join_claim_adj_df = join_claim_adj_df[col_list]
# rename columns
join_claim_adj_df.columns = ['ClaimId', 'ClaimNumber_x', 'Description', 'Adjuster_Notes', 'SIU_FLAG']

join_claim_adj_df['SIU_FLAG'].fillna(0, inplace=True)

join_claim_adj_df_new['SIU_FLAG'].fillna(0, inplace=True)

map_dict = {0: 0, 1: 1, 2: 1}
join_claim_adj_df["SIU_FLAG_2"] = join_claim_adj_df["SIU_FLAG"].map(map_dict)

map_dict = {0: 0, 1: 1, 2: 1}
join_claim_adj_df_new["SIU_FLAG_2"] = join_claim_adj_df_new["SIU_FLAG"].map(map_dict)

join_claim_adj_df_fin = join_claim_adj_df.append(join_claim_adj_df_new)

week_2_n = Final_Data.merge(new_unstr, how='inner',
                         left_on=Final_Data.ClaimNumber,
                         right_on=new_unstr.ClaimNumber,
                         left_index=False,
                         right_index=False)

all_col = ['ClaimNumber_x', 'Description', 'Adjuster_Notes', 'claimid', 'claimtype', 'ext_broadlobcode_desc',
           'Report_interval', 'Report_lag', 'prev_renewals', 'LocationOfTheft', 'LitigationStatus', 'losscause_group',
           'Fault', 'REGION',
           'PolicyType', 'Body_Head', 'Body_Neck', 'Body_Upper_ext', 'Body_Trunk', 'Body_Lower_ext', 'Body_Unknown',
           'Body_Multiple', 'ISO_address', 'ISO_Name', 'ISO_Phone', 'ISO_VIN', 'exposure_Count', 'claimant_count',
           'Injured_passengers', 'insured_cnt', 'vehicle_count', 'max_Pre_claims_cnt', 'claimant_tenure_wt_qbe',
           'insured_state_match', 'thirdparty_state_match', 'AUTO_BI', 'AUTO_PD', 'AUTO_COLLISION', 'COMPREHENSIVE',
           'MEDPAY', 'PIP', 'COMPREHENSIVE_only',
           'collision_only', 'policy_period_gt_0', 'weekend_flag', 'jul_dec', 'policy_period_extreme',
           'collision_overall', 'suspicious_collision', 'previous_claim_duration',
           ]

week_2 = week_2_n[all_col]
week_2.columns = ['ClaimNumber', 'Description', 'Adjuster_Notes', 'claimid', 'claimtype', 'ext_broadlobcode_desc',
                  'Report_interval', 'Report_lag', 'prev_renewals', 'LocationOfTheft', 'LitigationStatus',
                  'losscause_group', 'Fault', 'REGION',
                  'PolicyType', 'Body_Head', 'Body_Neck', 'Body_Upper_ext', 'Body_Trunk', 'Body_Lower_ext',
                  'Body_Unknown', 'Body_Multiple', 'ISO_address', 'ISO_Name', 'ISO_Phone', 'ISO_VIN', 'exposure_Count',
                  'claimant_count',
                  'Injured_passengers', 'insured_cnt', 'vehicle_count', 'max_Pre_claims_cnt', 'claimant_tenure_wt_qbe',
                  'insured_state_match', 'thirdparty_state_match', 'AUTO_BI', 'AUTO_PD', 'AUTO_COLLISION',
                  'COMPREHENSIVE', 'MEDPAY', 'PIP', 'comprehensive_only',
                  'collision_only', 'policy_period_gt_0', 'weekend_flag', 'jul_dec', 'policy_period_extreme',
                  'collision_overall', 'suspicious_collision', 'previous_claim_duration',
                  ]

week_1_n = pd.read_csv(os.path.join(INPUT_DIR, 'week_1.csv'), encoding='ISO-8859-1')

all_col_o = ['ClaimNumber', 'Description', 'Adjuster_Notes', 'claimid', 'claimtype', 'ext_broadlobcode_desc',
             'Report_interval', 'Report_lag', 'prev_renewals', 'LocationOfTheft', 'LitigationStatus', 'losscause_group',
             'Fault', 'REGION',
             'PolicyType', 'Body_Head', 'Body_Neck', 'Body_Upper_ext', 'Body_Trunk', 'Body_Lower_ext', 'Body_Unknown',
             'Body_Multiple', 'ISO_address', 'ISO_Name', 'ISO_Phone', 'ISO_VIN', 'exposure_Count', 'claimant_count',
             'Injured_passengers', 'insured_cnt', 'vehicle_count', 'max_Pre_claims_cnt', 'claimant_tenure_wt_qbe',
             'insured_state_match', 'thirdparty_state_match', 'AUTO_BI', 'AUTO_PD', 'AUTO_COLLISION', 'COMPREHENSIVE',
             'MEDPAY', 'PIP', 'comprehensive_only',
             'collision_only', 'policy_period_gt_0', 'weekend_flag', 'jul_dec', 'policy_period_extreme',
             'collision_overall', 'suspicious_collision', 'previous_claim_duration',
             ]

week_1 = week_1_n[all_col_o]

week_1.columns = ['ClaimNumber', 'Description', 'Adjuster_Notes', 'claimid', 'claimtype', 'ext_broadlobcode_desc',
                  'Report_interval', 'Report_lag', 'prev_renewals', 'LocationOfTheft', 'LitigationStatus',
                  'losscause_group', 'Fault', 'REGION',
                  'PolicyType', 'Body_Head', 'Body_Neck', 'Body_Upper_ext', 'Body_Trunk', 'Body_Lower_ext',
                  'Body_Unknown', 'Body_Multiple', 'ISO_address', 'ISO_Name', 'ISO_Phone', 'ISO_VIN', 'exposure_Count',
                  'claimant_count',
                  'Injured_passengers', 'insured_cnt', 'vehicle_count', 'max_Pre_claims_cnt', 'claimant_tenure_wt_qbe',
                  'insured_state_match', 'thirdparty_state_match', 'AUTO_BI', 'AUTO_PD', 'AUTO_COLLISION',
                  'COMPREHENSIVE', 'MEDPAY', 'PIP', 'comprehensive_only',
                  'collision_only', 'policy_period_gt_0', 'weekend_flag', 'jul_dec', 'policy_period_extreme',
                  'collision_overall', 'suspicious_collision', 'previous_claim_duration',
                  ]

week_2_1 = pd.DataFrame(week_2.loc[week_2['ClaimNumber'].isin(week_1['ClaimNumber'])])
week_1_2 = pd.DataFrame(week_1.loc[week_1['ClaimNumber'].isin(week_2['ClaimNumber'])])

week_2_2_2 = week_2_1.append(week_1_2)
week_2_2_3 = week_2_2_2.drop_duplicates()
d = pd.DataFrame(week_2_2_3['ClaimNumber'].value_counts())

d['cn'] = d.index
d.columns = ['changed_value', 'ClaimNumber']

week_2_tot = week_2.merge(d, how='left',
                          left_on=week_2.ClaimNumber,
                          right_on=d.ClaimNumber,
                          left_index=False,
                          right_index=False)

week_2_tot['changed_value'] = week_2_tot['changed_value'].fillna(0)
del week_2_tot['ClaimNumber_y']
week_2_tot.rename(columns={'ClaimNumber_x': 'ClaimNumber'}, inplace=True)
week_col = ['ClaimNumber', 'changed_value']
week_2_cv = week_2_tot[week_col]

triage_sheet_1 = pd.read_csv(os.path.join(INPUT_DIR, 'GSSC_PreTriage_File.csv'), encoding='ISO-8859-1')
g4s_1_all = pd.read_csv(os.path.join(INPUT_DIR, 'g4s_1.csv'), encoding='ISO-8859-1')
g4s_col = ['Claim Number',
           'Classification']
g4s_1 = g4s_1_all[g4s_col]
g4s_1.columns = ['ClaimNumber_g', 'Classification']

triage_1_col = ['Claim Number',
                'Previous Referral',
                'Model Referral',
                'SIU Referral',
                'Delivery ID']

triage_1_final = triage_sheet_1[triage_1_col]
triage_1_final.columns = ['ClaimNumber_g',
                          'Previous Referral',
                          'Model Referral',
                          'SIU Referral',
                          'Delivery ID 1']

triage_1_fin_1 = triage_1_final.merge(g4s_1, how='left',
                                      left_on=triage_1_final.ClaimNumber_g,
                                      right_on=g4s_1.ClaimNumber_g,
                                      left_index=False,
                                      right_index=False)

tri_col = ['ClaimNumber_g_x',
           'Previous Referral',
           'Model Referral',
           'SIU Referral',
           'Classification',
           'Delivery ID 1']

triage_1_fin = triage_1_fin_1[tri_col]

triage_1_fin.columns = ['ClaimNumber_t', 'Previous Referral 1', 'Model Referral 1', 'SIU Referral 1',
                        'SIU Classification Status 1', 'Delivery ID 1']

# --------------------------Creating topics from text [] ------------------------------------

stops = set(stopwords.words("english"))
stops.update(('iv', 'iviv', 'ov', 'ov1', 'ov2', 'ov3', 'ov4', 'veh1', 'veh2',
              'veh', 'iv1', 'iv2', 'iv3', 'cv1', 'cv2', 'cv', 'v2', '2010',
              'robert', '2008', '2007', '2006', '2005', '2004', '2000',
              '2003', '2002', '2009', '2001', 'robert', 'william',
              'adam', 'kleindickert', 'mark', 'paul', 'jeff', 'ashley',
              '000000', '010000', '025000', '005000', '04', '05', 'insured',
              'vehicle', '1', '2', '3', 'vehicle', 'claimant', 'clmnt', 'car',
              'dmg', 'insd', 'insur', 'id', 'toyota', 'dodge', 'chevi', 'cause', 'damage'))

clean_description_notes_ld = []
for note_ld in join_claim_adj_df_fin['Description']:
    note_text_ld = re.sub(r'[^\sa-zA-Z0-9]', '', str(note_ld))
    words_ld = note_text_ld.lower().split()
    words_ld = [w for w in words_ld if not w in stops]
    clean_description_notes_ld.append(' '.join(words_ld))

vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, ngram_range=(2, 3),
                             smooth_idf=False, tokenizer=tokenize,
                             stop_words='english', max_features=500)

# vectorize and re-weight
description_data_features_ld = vectorizer.fit_transform(clean_description_notes_ld)
# convert to array
description_data_features_ld = description_data_features_ld.toarray()

# store feature names and tf-idf values
features_names_ld = vectorizer.get_feature_names()
idf_ld = vectorizer.idf_
# map feature names and tf-idf values side-by-side
map_features_idf_ld = dict(zip(features_names_ld, idf_ld))
# sort map

sorted_map_features_idf_ld = sorted(map_features_idf_ld.items(),
                                    key=operator.itemgetter(1))

num_topics = 10
num_top_words = 20

# apply the scikit-learn implementation of NMF
# set k = num_topics and run NMF for 500 iterations
# then get the factors W and H from the resulting model
clf = decomposition.NMF(n_components=num_topics, random_state=1, max_iter=500)
W_ld = clf.fit_transform(description_data_features_ld)
H_ld = clf.components_

for topic_index_ld in range(H_ld.shape[0]):
    top_indices_ld = np.argsort(H_ld[topic_index_ld, :])[::-1][0:num_top_words]
    term_ranking_ld = [features_names_ld[i] for i in top_indices_ld]
    print("Topic_ld %d: %s" % (topic_index_ld, ", ".join(term_ranking_ld)))

###############################################################################

W_scaled_ld = W_ld / np.sum(W_ld, axis=1, keepdims=True)

claim_numbers_ld = []
for clnum_ld in join_claim_adj_df_fin['ClaimNumber_x']:
    claim_numbers_ld.append(clnum_ld)

# turn this into an array so we can use NumPy functions
claim_numbers_ld = np.asarray(claim_numbers_ld)

W_scaled_orig_ld = W_scaled_ld.copy()

num_groups_ld = len(set(claim_numbers_ld))

W_grouped_ld = np.zeros((num_groups_ld, num_topics))

for i, num in enumerate(sorted(set(claim_numbers_ld))):
    W_grouped_ld[i, :] = np.mean(W_scaled_ld[claim_numbers_ld == num, :], axis=0)

W_scaled_ld = W_grouped_ld

# convert to list
sorted_claim_numbers_ls_ld = sorted(set(claim_numbers_ld))
W_scaled_ls_ld = list(W_scaled_ld)

# convert lists to dataframes
W_scaled_df_ld = pd.DataFrame(W_scaled_ls_ld)
sorted_claim_numbers_df_ld = pd.DataFrame({'Claim_Number': sorted_claim_numbers_ls_ld})

# combine to one dataframe
probability_proportion_clnum_df_ld = pd.concat([sorted_claim_numbers_df_ld, W_scaled_df_ld], axis=1)

# impute NaN values with zeroes
probability_proportion_clnum_df_ld.fillna(0, inplace=True)

df_unstr_LD = probability_proportion_clnum_df_ld

#################################################################################################################################################################
# ADJUSTER NOTES TOPICS PREPARATION FOR FRESH CLAIMS
#################################################################################################################################################################

clean_description_notes_an = []

for note_an in join_claim_adj_df_fin['Adjuster_Notes']:
    note_text_an = re.sub(r'[^\sa-zA-Z0-9]', '', str(note_an))
    words_an = note_text_an.lower().split()
    words_an = [w for w in words_an if not w in stops]
    clean_description_notes_an.append(' '.join(words_an))

vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, ngram_range=(2, 3),
                             smooth_idf=False, tokenizer=tokenize,
                             stop_words='english', max_features=500)

# vectorize and re-weight
description_data_features_an = vectorizer.fit_transform(clean_description_notes_an)
# convert to array
description_data_features_an = description_data_features_an.toarray()
# return array's shape and size



# store feature names and tf-idf values
features_names_an = vectorizer.get_feature_names()
idf_an = vectorizer.idf_
# map feature names and tf-idf values side-by-side
map_features_idf_an = dict(zip(features_names_an, idf_an))
# sort map

sorted_map_features_idf_an = sorted(map_features_idf_an.items(),
                                    key=operator.itemgetter(1))

num_topics = 10
num_top_words = 20

# apply the scikit-learn implementation of NMF
# set k = num_topics and run NMF for 500 iterations
# then get the factors W and H from the resulting model
clf = decomposition.NMF(n_components=num_topics, random_state=1, max_iter=500)
W_an = clf.fit_transform(description_data_features_an)
H_an = clf.components_

for topic_index_an in range(H_an.shape[0]):
    top_indices_an = np.argsort(H_an[topic_index_an, :])[::-1][0:num_top_words]
    term_ranking_an = [features_names_an[i] for i in top_indices_an]
    print("Topic_an %d: %s" % (topic_index_an, ", ".join(term_ranking_an)))

###############################################################################

W_scaled_an = W_an / np.sum(W_an, axis=1, keepdims=True)

claim_numbers_an = []
for clnum_an in join_claim_adj_df_fin['ClaimNumber_x']:
    claim_numbers_an.append(clnum_an)

# turn this into an array so we can use NumPy functions
claim_numbers_an = np.asarray(claim_numbers_an)

W_scaled_orig_an = W_scaled_an.copy()

num_groups_an = len(set(claim_numbers_an))

W_grouped_an = np.zeros((num_groups_an, num_topics))

for i, num in enumerate(sorted(set(claim_numbers_an))):
    W_grouped_an[i, :] = np.mean(W_scaled_an[claim_numbers_an == num, :], axis=0)

W_scaled_an = W_grouped_an

# convert to list
sorted_claim_numbers_ls_an = sorted(set(claim_numbers_an))
W_scaled_ls_an = list(W_scaled_an)

# convert lists to dataframes
W_scaled_df_an = pd.DataFrame(W_scaled_ls_an)
sorted_claim_numbers_df_an = pd.DataFrame({'Claim_Number': sorted_claim_numbers_ls_an})

# combine to one dataframe
probability_proportion_clnum_df_an = pd.concat([sorted_claim_numbers_df_an, W_scaled_df_an], axis=1)

# impute NaN values with zeroes
probability_proportion_clnum_df_an.fillna(0, inplace=True)

df_unstr_AN = probability_proportion_clnum_df_an

old_str = pd.read_csv(INPUT_DIR + "subset_6.csv", low_memory=False)

if 'Unnamed: 0' in pd.Series(list(old_str.columns)).unique():
    del old_str['Unnamed: 0']

if 'DT_seg' in pd.Series(list(old_str.columns)).unique():
    del old_str['DT_seg']

if 'Unnamed: 0' in pd.Series(list(unstr_claim_adj_df_full.columns)).unique():
    del unstr_claim_adj_df_full['Unnamed: 0']

Final_Data['claimid'] = 9999999

Final_Data.rename(columns={'COMPREHENSIVE_only': 'comprehensive_only'}, inplace=True)
Final_Data.rename(columns={'Expo_MEDPAY': 'Expo_MedPay'}, inplace=True)

old_str['cd_cn'] = old_str['ClaimNumber']

Final_Data['cd_cn'] = '9999999' + Final_Data['ClaimNumber'].astype(str)

df_str = old_str.append(Final_Data)

# read original unstructured model dataset
input_unstr_orig = "/home/e06278e/Production/Input/"
df_unstr_old = pd.read_csv(input_unstr_orig + "Claim_adjuster_notes_final_1.csv", encoding='ISO-8859-1')
df_unstr_old.columns = ['claimID', 'ClaimNumber', 'Description', 'Adjuster_Notes']

new_unstr['cd_cn'] = '9999999' + new_unstr['ClaimNumber'].astype(str)

df_unstr_old['cd_cn'] = df_unstr_old['ClaimNumber']

df_unstr_orig = df_unstr_old.append(new_unstr)

df_unstr_LD.columns = ['Claim_Number', 'Topic_LD_1', 'Topic_LD_2', 'Topic_LD_3', 'Topic_LD_4', 'Topic_LD_5',
                       'Topic_LD_6', 'Topic_LD_7', 'Topic_LD_8', 'Topic_LD_9', 'Topic_LD_10']
df_unstr_AN.columns = ['Claim_Number', 'Topic_AN_1', 'Topic_AN_2', 'Topic_AN_3', 'Topic_AN_4', 'Topic_AN_5',
                       'Topic_AN_6', 'Topic_AN_7', 'Topic_AN_8', 'Topic_AN_9', 'Topic_AN_10']

df_unstr_all = df_unstr_LD.merge(df_unstr_AN, how='outer',
                                 left_on=df_unstr_LD.Claim_Number,
                                 right_on=df_unstr_AN.Claim_Number,
                                 left_index=False,
                                 right_index=False)

df_initial = df_str.merge(df_unstr_all, how='left',
                          left_on=df_str.ClaimNumber,
                          right_on=df_unstr_all.Claim_Number_x,
                          left_index=False,
                          right_index=False)

df = df_unstr_orig.merge(df_initial, how='inner',
                         left_on=df_unstr_orig.cd_cn,
                         right_on=df_initial.cd_cn,
                         left_index=False, right_index=False)

df['totalloss_flag'] = (df['Description'].str.lower().str.contains("total" and "loss", na=False)).astype(int)
df['glass_flag'] = (df['Description'].str.lower().str.contains("glass" and ("replac | repair"), na=False)).astype(int)
df['rear_flag'] = (df['Description'].str.lower().str.contains("rear", na=False)).astype(int)
df['unknownperson_flag'] = (df['Description'].str.lower().str.contains("unknown" and "person", na=False)).astype(int)
df['remark_flag'] = (df['Description'].str.lower().str.contains("remark" and ("claim | shop"), na=False)).astype(int)
df['stolen_flag'] = (df['Description'].str.lower().str.contains("stolen", na=False)).astype(int)

# Adjuster Notes KEYWORDS
df['policereport_flag'] = (df['Adjuster_Notes'].str.lower().str.contains("polic" and ("repo|rpt"), na=False)).astype(
    int)
df['med_flag'] = (df['Adjuster_Notes'].str.lower().str.contains("med" and ("pay | auth | record"), na=False)).astype(
    int)
df['wageloss_flag'] = (df['Adjuster_Notes'].str.lower().str.contains("wage" and "loss", na=False)).astype(int)
df['injuri_flag'] = (df['Adjuster_Notes'].str.lower().str.contains("injuri" and ("protect | person"), na=False)).astype(
    int)
df['softtissue_flag'] = (
    df['Adjuster_Notes'].str.lower().str.contains(("physic" and "therapi") or ("soft" and "tissu"), na=False)).astype(
    int)
df['attorney_flag'] = (df['Adjuster_Notes'].str.lower().str.contains(
    ("claiman" or "clmt" or "clmnt" or "clt") and ("attorney" or "atty" or "attty" or "attny" or "atny"),
    na=False)).astype(int)
df['keys_flag'] = (df['Adjuster_Notes'].str.lower().str.contains(("key") or ("iv" and "insured"), na=False)).astype(int)

ui = pd.read_csv(os.path.join(INPUT_DIR, 'week_1.csv'))

ch5 = pd.read_csv(os.path.join(OUTPUT_DIR, 'triage_referral_output.csv'))

if 'Unnamed: 0' in pd.Series(list(ch5.columns)).unique():
    del ch5['Unnamed: 0']

ch5['Model ID'] = 'AFV10'
ch5['Delivery ID 1'] = ch5['Delivery ID 1'].fillna('AFV10D10')
ch5['Delivery ID'] = ch5['Delivery ID 1'].str.slice(0, 6) + (
    ch5['Delivery ID 1'].str.slice(6, 10).astype(int) + 1).astype(str)
ch5['Scoring date'] = time.strftime("%Y-%m-%d")
ch5['Red Flags'] = ''
ch5['Claims System'] = 'CC'
ch5['Claim Status'] = 'Open'

ch6 = ch5[(ch5['Model Referral final'] == 1)]
ch6['seq'] = range(1, len(ch6) + 1, 1)
ch6['Pre-Triage Member'] = (ch6['seq'] % 3).map({1: 'Doug', 2: 'Tristan', 0: 'Lihnel'})
ch7 = ch5[(ch5['Model Referral final'] == 0)]
ch7['Pre-Triage Member'] = 'NA'

ch8 = ch6.append(ch7)

ch9 = ch8.merge(rd, how='left',
                left_on=ch8.ClaimNumber_x,
                right_on=rd.ClaimNumber,
                left_index=False,
                right_index=False)

tri_fin_col = ['Model ID', 'Delivery ID', 'ClaimNumber_x', 'changed_value', 'Predicted_prob',
               'SIU Classification Status 1',
               'Previous Referral 2', 'Model Referral final', 'RemainingReserves', 'Pre-Triage Member',
               'SIU Referral 1', 'SIU Referral', 'Red Flags', 'Scoring date', 'Claims System', 'JurisdictionState_code',
               'ReportedDate', 'Claim Status']

GSSC_PreTriage_File = ch9[tri_fin_col]
GSSC_PreTriage_File.columns = ['Model ID', 'Delivery ID', 'Claim Number', 'Claim Number_x', 'Changed Value',
                               'Predicted Probability', 'SIU Previous Classification Status',
                               'Previous Referral', 'Model Referral', 'Remaining Reserve', 'Pre-Triage Member',
                               'SIU Referral old', 'SIU Referral', 'Red Flags', 'Scoring date', 'Claims System',
                               'JurisdictionState', 'ReportedDate', 'Claim Status']
del GSSC_PreTriage_File['Claim Number_x']

GSSC_PreTriage_File['Scoring date'] = pd.to_datetime(GSSC_PreTriage_File['Scoring date'])
GSSC_PreTriage_File['ReportedDate'] = pd.to_datetime(GSSC_PreTriage_File['ReportedDate'])

GSSC_PreTriage_File['Open Days'] = GSSC_PreTriage_File['Scoring date'] - GSSC_PreTriage_File['ReportedDate']
GSSC_PreTriage_File['Pre-Triage Time'] = ''
GSSC_PreTriage_File['Triage Indictor'] = 0

GSSC_PreTriage_File.to_csv(os.path.join(OUTPUT_DIR, 'GSSC_PreTriage_File.csv'))

cnxn.close()


def main():
    input = "/home/e06278e/Production/Input/"  # update directory

    # Importing both subset and entire dataset

    df_n = df.loc[df['cd_cn_x'].isin(old_str['ClaimNumber'])]

    # Converting 2's to 1's in the target variable


    df['SIU_FLAG'] = df['SIU_FLAG'].replace([2], 1)
    df_n['SIU_FLAG'] = df_n['SIU_FLAG'].replace([2], 1)

    # Creating hour of the day column from Reporteddate

    df['ReportedDate'] = pd.to_datetime(df['ReportedDate_x'])
    df['hod'] = df['ReportedDate'].dt.hour

    # Converting null rows to 0's in selected columns

    iso = [
        'ISO_address',
        'ISO_Name',
        'ISO_Phone',
        'ISO_VIN'

    ]
    for q in iso:
        df[q].fillna(0, inplace=True)

    # Performing outlier treatment on variables

    qtls(df, 'Report_lag', (0.01, 0.01))
    qtls(df, 'prev_renewals', (0, 0.002))
    qtls(df, 'exposure_Count', (0, 0.005))
    qtls(df, 'claimant_count', (0, 0.005))
    qtls(df, 'vehicle_count', (0, 0.005))
    qtls(df, 'max_Pre_claims_cnt', (0, 0.01))
    qtls(df, 'Body_Neck', (0, 0.001))
    qtls(df, 'Body_Trunk', (0, 0.001))
    qtls(df, 'Body_Unknown', (0, 0.001))
    qtls(df, 'AUTO_BI', (0, 0.0005))
    qtls(df, 'AUTO_PD', (0, 0.0005))
    qtls(df, 'AUTO_COLLISION', (0, 0.0005))
    qtls(df, 'ISO_address', (0, 0.001))
    qtls(df, 'ISO_Name', (0, 0.001))
    qtls(df, 'ISO_Phone', (0, 0.001))
    qtls(df, 'ISO_VIN', (0, 0.001))
    qtls(df, 'claimant_tenure_wt_qbe', (0.01, 0))

    # 1. plz put in categorical variables and make sure they are at the correct data format - convert to object
    varlist = [
        'ext_broadlobcode_desc',
        'Report_interval',
        'LocationOfTheft',
        'LitigationStatus',
        'losscause_group',
        'PolicyType',
        'insured_state_match',
        'thirdparty_state_match',
        'tod',
        'hod',
        'rear_end_Coll_ind',
        'STATE_ABBR',
        'REGION',
        'claimtype',
        'collision_ind_f',
        'comprehensive_only',
        'collision_only',
        'policy_period_gt_0',
        'weekend_flag',
        'jul_dec',
        'policy_period_extreme',
        "collision_overall",
        "suspicious_collision",
        'totalloss_flag',
        'glass_flag',
        'rear_flag',
        'unknownperson_flag',
        'remark_flag',
        'stolen_flag',

        'policereport_flag',
        'med_flag',
        'wageloss_flag',
        'injuri_flag',
        'softtissue_flag',
        'attorney_flag',
        'keys_flag'

    ]

    df[varlist].dtypes
    df[varlist] = df[varlist].astype(str)
    df[varlist].dtypes

    # 2. plz put in meaningful response variables
    var = [
        "claimtype",
        "ext_broadlobcode_desc",
        "Report_interval",
        "Report_lag",
        "prev_renewals",
        "LocationOfTheft",
        "LitigationStatus",
        "losscause_group",
        "Fault",
        "REGION",
        "PolicyType",
        "Body_Head",
        "Body_Neck",
        "Body_Upper_ext",
        "Body_Trunk",
        "Body_Lower_ext",
        "Body_Unknown",
        "Body_Multiple",
        "ISO_address",
        "ISO_Name",
        "ISO_Phone",
        "ISO_VIN",
        "exposure_Count",
        "claimant_count",
        "Injured_passengers",
        "insured_cnt",
        "vehicle_count",
        "max_Pre_claims_cnt",
        "claimant_tenure_wt_qbe",
        "insured_state_match",
        "thirdparty_state_match",
        "AUTO_BI",
        "AUTO_PD",
        "AUTO_COLLISION",
        "COMPREHENSIVE",
        "MEDPAY",
        "PIP",
        "hod",
        # "collision_ind_f",
        "comprehensive_only",
        "collision_only",
        "policy_period_gt_0",
        "weekend_flag",
        "jul_dec",
        "policy_period_extreme",
        "collision_overall",
        "suspicious_collision",
        "previous_claim_duration",

        ##############################################################
        ##UN-COMMENT BLOCKS OF CODES BELOW - TO BE USED ONLY FOR TESTING KEYWORDS
        ##COMMENT CODES BELOW WHEN NOT IN USE
        ##############################################################
        # KEYWORDS
        'totalloss_flag',
        # 'glass_flag',
        'rear_flag',
        'unknownperson_flag',
        # 'remark_flag',
        'stolen_flag',
        # 'hit_flag',

        'policereport_flag',
        'med_flag',
        # 'wageloss_flag',
        'injuri_flag',
        'softtissue_flag',
        'attorney_flag',
        # 'keys_flag',
        # 'alcohol_flag',



        # TOPICS
        'Topic_LD_1',
        'Topic_LD_2',
        'Topic_LD_3',
        'Topic_LD_4',
        'Topic_LD_5',
        'Topic_LD_6',
        'Topic_LD_7',
        'Topic_LD_8',
        'Topic_LD_9',
        'Topic_LD_10',
        'Topic_AN_1',
        'Topic_AN_2',
        'Topic_AN_3',
        'Topic_AN_4',
        'Topic_AN_5',
        'Topic_AN_6',
        'Topic_AN_7',
        'Topic_AN_8',
        'Topic_AN_9',
        'Topic_AN_10'

    ]

    X_all = df[var]
    X_all = [getFeatures(X_all, var, idx) for idx in X_all.index]
    vec = DictVectorizer()
    X_all = vec.fit_transform(X_all)
    imp = Imputer(missing_values=np.nan, strategy='mean', axis=0)
    X_all = imp.fit_transform(X_all)
    print("size of X_all", X_all.shape)
    # X_all = csr_matrix(X_all)
    X_all = X_all.todense()

    # Creating a subset of 83909 claims from the entire dataset of 410958 claims

    C = df['claimid']
    C_n = df_n['claimid']
    C_nd = pd.DataFrame(C_n)
    C_nd['ind_n'] = C_n.index
    C_d = pd.DataFrame(C)
    C_d['ind'] = C_d.index
    C_s = pd.merge(C_d, C_nd, how='inner', on='claimid')
    ind_f = C_s['ind']
    X_o = []
    for i in ind_f:
        X_o.append(np.asarray(X_all[i, :]))
    X_u = np.vstack(X_o)
    X_sub = np.matrix(np.array(X_u))

    Y_n = np.array(df_n['SIU_FLAG'])
    Y_n = np.nan_to_num(Y_n)
    Y = np.array(df['SIU_FLAG'])
    Y = np.nan_to_num(Y)
    Cd = np.array(df_n['claimid'])
    Ct = np.array(df['claimid'])
    Cn_t = np.array(df['ClaimNumber_x'])
    R_t = np.array(df['RemainingReserves'])
    Cd_Cn = np.array(df['cd_cn_x'])
    J = np.array(df['JurisdictionState'])

    X_train, X_test, Y_train, Y_test, C_train, C_test = train_test_split(X_sub, Y_n, Cd, test_size=0.3,
                                                                         random_state=398)

    # Creating weights to the model in the ration of 39:1 for 1's and 0's respectively

    Z = np.ones_like(Y_train)
    A = np.column_stack((Y_train, Z))
    for i in range(len(A[:, 0])):

        if A[i, 0] == 1:
            A[i, 1] = 39
    B = A[:, 1]
    # f = gzip.open(output + 'vec.pklz', 'wb')
    # pickle.dump(vec, f)
    # f.close()

    # """
    # Auto tuning
    # """
    # param_grid = {'learning_rate': [0.1, 0.05, 0.02, 0.01],
    #               'max_depth': [2,4,6,8,10],
    #               'n_estimators': [100,200,300,400,500],
    #               'min_samples_leaf': [3, 5, 9, 17],
    #               'random_state':[398]
    #               }
    # est = GradientBoostingRegressor()
    # gs_cv = GridSearchCV(est, param_grid, n_jobs=1).fit(X_train, y_train)
    # print "the best parameters", gs_cv.best_params_
    # params = gs_cv.best_params_

    # Adding parameters and weights to the model

    SEED = 398
    params = {'n_estimators': 400, 'max_depth': 4, 'learning_rate': 0.05, 'verbose': 1, 'min_samples_leaf': 20,
              "random_state": SEED}
    gbm_model = GradientBoostingClassifier(**params)

    # Training the model on train dataset

    gbm_model.fit(X_train, Y_train, B)

    # """
    # save model object
    # """
    # f = gzip.open(output + 'model.pklz', 'wb')
    # pickle.dump(gbm_model, f)
    # f.close()



    # Scoring both train and test datasets


    # Predicting flags
    train_pred = gbm_model.predict(X_train)
    test_pred = gbm_model.predict(X_test)

    # Predicting probabilities
    train_pred_prob = gbm_model.predict_proba(X_train)[:, 1]
    test_pred_prob = gbm_model.predict_proba(X_test)[:, 1]

    # Scoring the entire dataset (backscoring)

    # Predicting flags
    all_pred = gbm_model.predict(X_all)

    # Predicting probabilities
    all_pred_prob = gbm_model.predict_proba(X_all)[:, 1]

    # Creating datasets for viewing all results
    train_res = np.column_stack((C_train, Y_train, train_pred, train_pred_prob))
    test_res = np.column_stack((C_test, Y_test, test_pred, test_pred_prob))
    all_res = np.column_stack((Ct, Cn_t, Cd_Cn, Y, all_pred, all_pred_prob, R_t, J))

    tr_df = pd.DataFrame(train_res)
    te_df = pd.DataFrame(test_res)
    tot_df = tr_df.append(te_df)
    tot_df.columns = ['claimid', 'Current_SIU', 'Predicted_SIU', 'Predicted_prob']
    # tot_df.to_csv(output+"tot_df.csv")


    all_df_x = pd.DataFrame(all_res)
    all_df_x.columns = ['claimid', 'ClaimNumber', 'Cd_Cn', 'Current_SIU', 'Predicted_SIU', 'Predicted_prob',
                        'RemainingReserves', 'JurisdictionState_code']
    all_df_col = ['ClaimNumber', 'Cd_Cn', 'Current_SIU', 'Predicted_SIU', 'Predicted_prob', 'RemainingReserves',
                  'JurisdictionState_code']
    all_df = all_df_x[all_df_col]
    # all_df.to_csv("/home/e06278e/Production/Output/all_df.csv",index=False, header=True)
    new_result_df = all_df.loc[all_df['Cd_Cn'].isin(Final_Data['cd_cn'])]

    week_2_res = week_2.loc[week_2['ClaimNumber'].isin(new_result_df['ClaimNumber'])]
    week_2_res.to_csv("/home/e06278e/Production/Output/week_old.csv", index=False, header=True)

    week_2_mod = week_2_res.merge(new_result_df, how='inner',
                                  left_on=week_2_res.ClaimNumber,
                                  right_on=new_result_df.ClaimNumber,
                                  left_index=False,
                                  right_index=False)

    week_2_final = week_2_mod.merge(week_2_cv, how='inner',
                                    left_on=week_2_mod.ClaimNumber_x,
                                    right_on=week_2_cv.ClaimNumber,
                                    left_index=False,
                                    right_index=False)

    week_2_fin = week_2_final.merge(triage_1_fin, how='left',
                                    left_on=week_2_mod.ClaimNumber_x,
                                    right_on=triage_1_fin.ClaimNumber_t,
                                    left_index=False,
                                    right_index=False)

    week_2_fin['Previous Referral 1'] = week_2_fin['Previous Referral 1'].fillna(0)
    week_2_fin['Model Referral 1'] = week_2_fin['Model Referral 1'].fillna(0)
    week_2_fin['SIU Referral 1'] = week_2_fin['SIU Referral 1'].fillna(0)
    week_2_fin['SIU Classification Status 1'] = week_2_fin['SIU Classification Status 1'].fillna(0)

    week_2_fin['Previous Referral 2'] = np.where((((week_2_fin['Previous Referral 1'] == 0) & (
        week_2_fin['SIU Referral 1'] == 1)) | (week_2_fin['Previous Referral 1'] == 1)), 1, 0)

    ch = pd.DataFrame(week_2_fin.sort_index(by=['Predicted_prob'], ascending=[False]))

    ch['Model Referral'] = np.where(
        ch['Predicted_prob'] >= (ch['Predicted_prob'].iloc[int(len(ch['Predicted_prob']) * 0.05)]), 1, 0)

    ch1 = ch[(ch['Model Referral'] == 1)]
    ch2 = ch[(ch['Model Referral'] == 0)]
    ch3 = pd.DataFrame(ch1.sort_index(by=['RemainingReserves'], ascending=[False]))
    ch4 = ch3.append(ch2)

    ch4['Model Referral final'] = np.where((ch4['Model Referral'] == 0) | (
        (ch4['Model Referral'] == 1) & ((ch4['SIU Classification Status 1']).astype(str) == 'SIU Investigation')) | (
                                               (ch4['Model Referral'] == 1) & (
                                                   (ch4['SIU Classification Status 1']).astype(
                                                       str) == 'Rejected Referral') & (
                                                   ch4['changed_value'] == 1)), 0, 1)
    ch4['SIU Referral'] = 0
    ch4.to_csv(OUTPUT_DIR + "triage_referral_output.csv")
    # ch4['ReportedDate']=ch4['ReportedDate'].apply(lambda v: str(v))

    all_df.to_csv(OUTPUT_DIR + "all_df.csv")
    new_result_df.to_csv(OUTPUT_DIR + "new_result_df.csv")

    # Input the threshold vaue for classification, currently 0.5 meaning that probability above 0.5 is 1 and below 0.5 is 0.
    # It can be changed to any value between 0 to 1
    # This is for obtaining all evaluaion metrics


    test_pred_class = binarize(test_pred_prob, 0.5)[0]
    train_pred_class = binarize(train_pred_prob, 0.5)[0]

    # Calculating false positive rate, true positive rate for different threshold values. All values between 0 to 1
    false_pos_rate, true_pos_rate, thresholds = metrics.roc_curve(Y_test, test_pred_prob, pos_label=1)
    false_pos_rate_tr, true_pos_rate_tr, thresholds_tr = metrics.roc_curve(Y_train, train_pred_prob, pos_label=1)

    # plotting ROC Curve for train2
    roc_plot_train(false_pos_rate_tr, true_pos_rate_tr)

    # plotting ROC Curve for test
    roc_plot_test(false_pos_rate, true_pos_rate)

    # calculating different characteriestics of the model
    acc_0, acc_1, acc, n_acc, confusion, mis, sensitivity, specificity, fpr, fnr, f1, precision, precision_0, AUC_test = charac(
        Y_test, test_pred_class)

    acc_0_tr, acc_1_tr, acc_tr, n_acc_tr, confusion_tr, mis_tr, sensitivity_tr, specificity_tr, fpr_tr, fnr_tr, f1_tr, precision_tr, precision_0_tr, AUC_train = charac(
        Y_train, train_pred_class)

    AUC_test_plot = roc_auc_score(Y_test, test_pred_prob)

    AUC_train_plot = roc_auc_score(Y_train, train_pred_prob)


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    total_time = int(end - start)

    print("running time %d" % (end - start))
