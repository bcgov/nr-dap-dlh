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
	  ,reg.org_unit_name as rollup_region_description
	  ,org.rollup_dist_code as rollup_district_code
	  ,rdis.org_unit_name as  rollup_district_description
	  ,case when org.rollup_dist_code in ('DCC','DQU','DMH','DRM','DSE','DOS','DCS','DKA') then 'SA'
	        when org.rollup_dist_code in ('DFN','DPC','DPG','DMK','DVA','DKM','DSS','DND') then 'NA'
	        when org.rollup_dist_code in ('DSC','DSQ','DCK','DNI','DSI','DQC','DCR') then 'CA' end as roll_up_area_code
	  ,case when org.rollup_dist_code in ('DCC','DQU','DMH','DRM','DSE','DOS','DCS','DKA') then 'South Area'
	        when org.rollup_dist_code in ('DFN','DPC','DPG','DMK','DVA','DKM','DSS','DND') then 'North Area'
	        when org.rollup_dist_code in ('DSC','DSQ','DCK','DNI','DSI','DQC','DCR') then 'Coast Area' end as roll_up_area_description
	  -- Combine multiple keys into a single composite_key
      ,'CORP' || '|'||  coalesce(cast(org.org_unit_code as varchar),'~') as unqid
	from {{ source('fta','org_unit') }} org
	left join {{ source('fta','org_unit') }} reg on (org.rollup_region_code = reg.org_unit_code)
	left join {{ source('fta','org_unit') }} rdis on (org.rollup_dist_code = rdis.org_unit_code)
	)
--insert into pmt_dpl.dim_org
  select * from corp_data

{% endsnapshot %}