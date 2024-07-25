{% snapshot dim_location %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['location_code'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as 
(  select distinct
  'ATS' 						as src_sys_code
  ,proj.location 				as location_code
  ,'ATS' || '|'||  coalesce(cast(proj.location as varchar),'~') as unqid
--from fdw_ods_ats_replication.ats_projects proj
--inner join fdw_ods_ats_replication.ats_managing_fcbc_regions amfr
from {{ source('ats','ats_projects') }} proj
inner join {{ source('ats','ats_managing_fcbc_regions') }} amfr
on (proj.managing_fcbc_region_id = amfr.managing_fcbc_region_id)
where proj.project_status_code = '1'
)
, fta_data as 
(--FTA does not have location as of now
  Select 
  'FTA' 						as src_sys_code
  ,null 						as location_code
  ,'FTA' || '|'||  coalesce(cast(null as varchar),'~') as unqid
)
, rrs_rp_data as 
(--RRS_RP does not have location as of now
  Select
  'RRS_RP' 						as src_sys_code
  ,null 						as location_code
  ,'RRS_RP' || '|'||  coalesce(cast(null as varchar),'~') as unqid
)
, rrs_rup_data as 
(--RRS_RUP does not have location as of now
  Select
  'RRS_RUP' 						as src_sys_code
  ,null 						as location_code
  ,'RRS_RUP' || '|'||  coalesce(cast(null as varchar),'~') as unqid
)--insert into pmt_dpl.dim_location
select *  from ats_data

union ALL
select * from fta_data

union ALL
select * from rrs_rup_data

union ALL
select * from rrs_rp_data


{% endsnapshot %}