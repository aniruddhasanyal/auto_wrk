--
--SELECT   count(*) FROM     EVOPAL.TAL_EVO_CLAV_CA as a left join EVOPAL.TAL_EVO_CLAT_EX as b on 
--a.COMPANY_CODE||a.claim_br||a.claim_no = b.COMPANY_CODE||b.claim_br||b.claim_no
--WHERE    a.COMPANY_CODE='1'
--     AND a.VALID_REC=1
--     AND a.VALID_FLAG<=2
--     AND a.DATE_REPT>='2011-01-01'
--     AND a.RSK_CLASS in ('MPA','MSA','MVA','MVD','MVF')
--	and b.COMPANY_CODE='1'
--     AND b.VALID_REC=1
--     AND b.VALID_FLAG<=2


Select 
ta.*,TE.INCIDENT_dESC,COALESCE(tb.clm_typ,tc.clm_type,td.TYPE_CLAM) as Claim_Type_F
,CASE WHEN COALESCE(tb.clm_typ,tc.clm_type,td.TYPE_CLAM) IN ('20','30') THEN 1 ELSE 0 END AS RECOVERY_fLAG
from 
(select  
  ca.*,ca.COMPANY_CODE||ca.POLICY_BR||ca.POLICY_NO||ca.POLICY_TYP as policyno
  ,ca.COMPANY_CODE||ca.claim_br||ca.claim_no as claimno
  from evopal.tal_evo_clav_ca as ca
  where  company_code =1	
  and valid_flag<=2
  and valid_rec = 1	
  and clav_ca_end_date = '9999-12-31'
  and DATE_rept BETWEEN '2011-01-01' AND '2013-12-31'
  and claim_status = 'C'
  and RSK_CLASS in ('MPA','MSA','MVA','MVD','MVF')) as ta
left join 
(select
  ex.COMPANY_CODE||ex.claim_br||ex.claim_no as claimno_mt
  ,ex.clm_typ
  ,ex.CIRCUMSTANCE as CIRCUMSTANCE1
  ,ex.RISK_STAT as risk_stat1
  ,ex.veh_class
  ,ex.TOT_APP_XS
  ,ex.LIABILITY
  from EVOPAL.TAL_EVO_CLAV_MT as ex
  where company_code = 1
  and valid_flag <= 2
  and valid_rec = 1
  and clav_mt_end_date = '9999-12-31') as tb
on ta.claimno = tb.claimno_mt

left join 
(select
  vc.COMPANY_CODE||vc.claim_br||vc.claim_no as claimno_vc
  ,vc.clm_type
  ,vc.CIRCUMSTANCE as CIRCUMSTANCE2
  ,vc.RISK_STAT as RISK_STAT2
  ,vc.class as class_veh
  ,vc.APPLD_XS
  ,vc.LIABILITY
  from EVOPAL.TAL_EVO_CLAV_VC as vc
  where company_code = 1
  and valid_flag <= 2
  and valid_rec = 1
  and clav_vc_end_date = '9999-12-31') as tc
on ta.claimno = tc.claimno_vc
left join 
(select
  va.COMPANY_CODE||va.claim_br||va.claim_no as claimno_va
  ,va.TYPE_CLAM
  ,va.CIRCUMSTANCE as circumstance3
  ,va.STAT_RISK
  ,va.TOT_AP_EX 
  ,va.LIABILITY
  from EVOPAL.TAL_EVO_CLAV_VA as va
  where company_code = 1
  and valid_flag <= 2
  and valid_rec = 1
  and clav_va_end_date = '9999-12-31') as td
on ta.claimno = td.claimno_va
LEFT JOIN 
(SELECT EX.*,EX.COMPANY_CODE||EX.claim_br||EX.claim_no as claimno
 FROM EVOPAL.TAL_EVO_CLAT_EX AS EX 
WHERE  
	 EX.COMPANY_CODE='1'
     AND EX.VALID_REC=1
     AND EX.VALID_FLAG<=2) AS TE
ON TA.CLAIMNO=TE.CLAIMNO


with ur



  