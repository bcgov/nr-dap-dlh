{% snapshot dim_org %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='pi_hash_key',
          check_cols='all',
          bind=False,
        )
    }}

    WITH scd_data AS (
  SELECT
    'CORP' as src_sys_code
	  ,org.org_unit_code as org_unit_code
	  ,org.org_unit_name as  org_unit_description
	  ,org.rollup_region_code as rollup_region_code
	  ,null as rollup_region_description
	  ,org.rollup_dist_code as rollup_district_code
	  ,null as  rollup_district_description
	  ,null as roll_up_area_code
	  ,null as roll_up_area_description,
    -- Combine multiple keys into a single composite_key
    md5(coalesce(cast('COPR'					 		as varchar),'~') || '|'|| 
			coalesce(cast(org_unit_code 				  	as varchar),'~')) as pi_hash_key
  FROM fdw_ods_fta_replication.org_unit org
)

SELECT *
FROM scd_data


{% endsnapshot %}
