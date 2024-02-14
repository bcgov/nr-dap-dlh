{% snapshot dim_project %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['project_name','location','project_status_code','region_name'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as (
	select 'ATS' as src_sys_code,
	prj.project_id:: varchar(20),  
	--replace(replace(prj.location::text, chr(10), NULL::text), chr(13), NULL::text) AS project_location, ---not sure what these replace values are supposed to do? returns null
	prj.project_name,
	prj.location,
	prj.project_status_code,
	amfr.region_name as project_region_description,
	'ATS' || '|'||  coalesce(cast(prj.project_id as varchar),'~') as unqid
	  from fdw_ods_ats_replication.ats_projects prj
	  left join fdw_ods_ats_replication.ats_managing_fcbc_regions amfr  ---dim_regions
		on (prj.managing_fcbc_region_id = amfr.managing_fcbc_region_id )
		where 1=1
		and prj.project_status_code = '1'
),
fta_data as (
	Select distinct 'FTA'	as src_sys_code
	,null as project_id
	,null as project_name
	,null as location
	,null as project_status_code
	,null as project_region_description
	,'FTA' || '|'||  coalesce(cast(null as varchar(5)),'~') as unqid

)
,
rrs_rp_data as (
	Select 'RRS_RP'	as src_sys_code
	,null as project_id
	,null as project_name
	,null as location
	,null as project_status_code
	,null as project_region_description
	,'RRS_RP' || '|'||  coalesce(cast(null as varchar(5)),'~') as unqid
)
,
rrs_rup_data as (
	Select 'RRS_RUP'	as src_sys_code
	,null as project_id
	,null as project_name
	,null as location
	,null as project_status_code
	,null as project_region_description
	,'RRS_RUP' || '|'||  coalesce(cast(null as varchar(5)),'~') as unqid
)
--insert into pmt_dpl.dim_project
select * from ats_data

union ALL
select * from fta_data

union ALL
select * from rrs_rup_data

union ALL
select * from rrs_rp_data
;

{% endsnapshot %}