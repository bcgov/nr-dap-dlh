{% snapshot dim_authorization_status %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['authorization_status_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as (
Select 'ATS' as src_sys_code
,aasc.authorization_status_code as authorization_status_code
,aasc.name as authorization_status_description
,'ATS' || '|'||  coalesce(cast(aasc.authorization_status_code as varchar),'~') as unqid
from  {{source ('ats','ats_authorization_status_codes') }} aasc
where 1=1
),
fta_data as (
	Select distinct
	'FTA' as src_sys_code
	,tasc.tenure_application_state_code as authorization_status_code
	,tasc.description AS authorization_status_description
	,'FTA' || '|'||  coalesce(cast(tasc.tenure_application_state_code as varchar),'~') as unqid
	FROM {{source ('fta','tenure_application_state_code') }} tasc
)
,rrs_rp_data as (
  Select distinct
  'RRS_RP' as src_sys_code
  ,rasc.road_application_status_code as authorization_status_code
  ,rasc.description AS authorization_status_description
  ,'RRS_RP' || '|'||  coalesce(cast(rasc.road_application_status_code as varchar),'~') as unqid
  FROM {{source ('rrs','road_application_status_code') }} rasc
)
,rrs_rup_data as (
  Select distinct
  'RRS_RUP' as src_sys_code
  ,rs.submission_status_code as authorization_status_code
  ,ssc.description AS authorization_status_description
  ,'RRS_RUP' || '|'||  coalesce(cast(rs.submission_status_code as varchar),'~') as unqid
  FROM {{source ('rrs','road_submission') }} rs
  left join  {{source ('rrs','submission_status_code') }} ssc ON (rs.submission_status_code = ssc.submission_status_code)
  where 1=1
  and rs.road_submission_type_code = 'RUP' 
  and rs.submission_status_code is not null
)
--insert into pmt_dpl.dim_authorization_status
select *
	from ats_data

union ALL
select * from fta_data

union ALL
select * from rrs_rup_data

union ALL
select * from rrs_rp_data


{% endsnapshot %}
