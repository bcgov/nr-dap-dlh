{% snapshot dim_permit_status %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['permit_status_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as (
	Select 'ATS' as src_sys_code
	,null as permit_status_code
	,null as permit_status_description
	,'ATS' || '|'||  coalesce(cast(null as varchar),'~') as unqid
)
,fta_data as (
	Select distinct
	'FTA' as src_sys_code
	,hsc.harvest_auth_status_code as permit_status_code
	,hsc.description as permit_status_description
	,'FTA' || '|'||  coalesce(cast(hsc.harvest_auth_status_code as varchar),'~') as unqid
	FROM fdw_ods_fta_replication.harvest_auth_status_code hsc
)
,rrs_rp_data as (
  Select distinct
  'RRS_RP' as src_sys_code
  ,rtsc.road_tenure_status_code as permit_status_code
  ,rtsc.description AS permit_status_description
  ,'RRS_RP' || '|'||  coalesce(cast(rtsc.road_tenure_status_code as varchar),'~') as unqid
  FROM fdw_ods_rrs_replication.road_tenure_status_code rtsc
)
,rrs_rup_data as (
  Select distinct
  'RRS_RUP' as src_sys_code
  ,null as permit_status_code
  ,null AS permit_status_description
  ,'RRS_RUP' || '|'||  coalesce(cast(null as varchar),'~') as unqid
)
--insert into pmt_dpl.dim_permit_status
select *
	from ats_data

union ALL
select * from fta_data

union ALL
select * from rrs_rup_data

union ALL
select * from rrs_rp_data
;


{% endsnapshot %}
