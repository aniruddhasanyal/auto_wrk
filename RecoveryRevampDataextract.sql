SELECT   a.*,b.incident_desc FROM     EVOPAL.TAL_EVO_CLAV_CA as a left join EVOPAL.TAL_EVO_CLAT_EX as b on 
a.COMPANY_CODE||a.claim_br||a.claim_no = b.COMPANY_CODE||b.claim_br||b.claim_no
WHERE    a.COMPANY_CODE='1'
     AND a.VALID_REC=1
     AND a.VALID_FLAG<=2
     AND a.DATE_REPT>='2011-01-01'
     AND a.RSK_CLASS in ('MPA','MSA','MVA','MVD','MVF')
	and b.COMPANY_CODE='1'
     AND b.VALID_REC=1
     AND b.VALID_FLAG<=2
  