{% snapshot dim_permit_type %}

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
	Select distinct 'ATS' 					as src_sys_code
	  ,aai.authorization_instrument_id::varchar(25) 		as permit_type_code
	  ,aai.authorization_instrument_name 	as permit_type_description
	  'src_sys_code','permit_type_code'
	  ,'ATS' || '|'||  coalesce(cast(aasc.authorization_status_code as varchar),'~') as unqid
	from fdw_ods_ats_replication.ats_authorization_instruments aai 
	where 1=1
)
,
fta_data as (
Select distinct 'FTA' as src_sys_code
 ,pfu.file_type_code::varchar(25) AS permit_type_code
, ftc.description AS permit_type_description
,'FTA' || '|'||  coalesce(cast(pfu.file_type_code as varchar(25)),'~') as unqid
 from fdw_ods_fta_replication.prov_forest_use pfu
 left join fdw_ods_fta_replication.file_type_code ftc 
	ON (ftc.file_type_code = pfu.file_type_code)
),
with rrs_data as (

)
--insert into pmt_dpl.dim_permit_type
select *
	from ats_data
union ALL
select *
	from fta_data	
union ALL
select *
	from rrs_data	
;

{% endsnapshot %}