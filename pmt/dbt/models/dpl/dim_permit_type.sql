{{config(materialized='table',
		  unique_key = ['src_sys_code','permit_type_code']
		) 
}}
with ats_data as (
	Select distinct 'ATS' 					as src_sys_code
	  ,aai.authorization_instrument_id 		as permit_type_code
	  ,aai.authorization_instrument_name 	as permit_type_description
	from fdw_ods_ats_replication.ats_authorization_instruments aai 
	where 1=1
)
,
with fta_data as (
Select distinct 'FTA' as src_sys_code
 ,pfu.file_type_code AS permit_type_code
, ftc.description AS permit_type_description
 from fdw_ods_fta_replication.prov_forest_use pfu
 left join fdw_ods_fta_replication.file_type_code ftc 
	ON (ftc.file_type_code = pfu.file_type_code)
),
with rrs_data as (

)
--insert into pmt_dpl.dim_permit_type
select *
	from ats_data
union ALL
select *
	from fta_data	
union ALL
select *
	from rrs_data	
;