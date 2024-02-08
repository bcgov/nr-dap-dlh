{% snapshot dim_location %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['project_code'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as 
(  select distinct
  'ATS' as src_sys_code
  ,proj.location as location_code
  ,proj.project_name as project_code
  ,'ATS' || '|'||  coalesce(cast(proj.location as varchar),'~') as unqid
from fdw_ods_ats_replication.ats_projects proj
inner join fdw_ods_ats_replication.ats_managing_fcbc_regions amfr
on (proj.managing_fcbc_region_id = amfr.managing_fcbc_region_id)
where proj.project_status_code = '1'
)
--insert into pmt_dpl.dim_location
select *
	from ats_data
;
;

{% endsnapshot %}