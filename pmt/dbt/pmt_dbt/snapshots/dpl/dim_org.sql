{% snapshot dim_org %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['org_unit_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with corp_data as (
	select 'CORP' as src_sys_code
	  ,org.org_unit_code as org_unit_code
	  ,org.org_unit_name as  org_unit_description
	  ,org.rollup_region_code as rollup_region_code
	  ,null as rollup_region_description
	  ,org.rollup_dist_code as rollup_district_code
	  ,null as  rollup_district_description
	  ,null as roll_up_area_code
	  ,null as roll_up_area_description
	  -- Combine multiple keys into a single composite_key
      ,'CORP' || '|'||  coalesce(cast(org_unit_code as varchar),'~') as unqid
	from fdw_ods_fta_replication.org_unit org
)
--insert into pmt_dpl.dim_org
select *
	from corp_data
;

{% endsnapshot %}