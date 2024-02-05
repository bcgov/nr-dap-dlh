{{config(materialized='table',
		  unique_key = ['src_sys_code','dim_permit_type']
		) 
}}
with ats_data as (
	Select distinct 'ATS' 					as src_sys_code
	  ,aai.authorization_instrument_id 		as permit_type_code
	  ,aai.authorization_instrument_name 	as permit_type_description
	from fdw_ods_ats_replication.ats_authorization_instruments aai 
	where 1=1
)
--insert into pmt_dpl.dim_permit_type
select *
	from ats_data
;

