{{ config(materialized='table',
		  unique_key = ['src_sys_code','project_id']
		) 
}}
with ats_data as (
	select 'ATS' as src_sys_code,
	prj.project_id,  
	--replace(replace(prj.location::text, chr(10), NULL::text), chr(13), NULL::text) AS project_location, ---not sure what these replace values are supposed to do? returns null
	prj.project_name,
	prj.location,
	prj.project_status_code,
	amfr.region_name
	  from fdw_ods_ats_replication.ats_projects prj
	  left join fdw_ods_ats_replication.ats_managing_fcbc_regions amfr  ---dim_regions
		on (prj.managing_fcbc_region_id = amfr.managing_fcbc_region_id )
		where 1=1
		and prj.project_status_code = '1'
)
--insert into pmt_dpl.dim_project
select * from ats_data
;