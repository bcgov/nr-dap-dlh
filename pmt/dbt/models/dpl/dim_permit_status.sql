{{ config(materialized='table',
		  unique_key = ['src_sys_code','permit_status_code']
		) 
}}
with ats_data as (
Select 'ATS' as src_sys_code
,aasc.authorization_status_code as permit_status_code
,aasc.name as permit_status_description
from  fdw_ods_ats_replication.ats_authorization_status_codes aasc
where 1=1
)
--insert into pmt_dpl.dim_permit_status
select *
	from ats_data
;
