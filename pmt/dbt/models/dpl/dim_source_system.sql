{{ config(materialized='table',
		  unique_key = ['src_sys_code']
		) 
}}
with ats_data as (
	select 'ATS' as src_sys_code,
	'Authorization Tracking System' as src_sys_description
	union ALL
	select 'FTA' as src_sys_code,
	'	Forest Tenure Administration' as src_sys_description
	union ALL
	select 'RRS' as src_sys_code,
	'Resource Roads System' as src_sys_description
)
--insert into pmt_dpl.dim_source_system
select * from ats_data
;