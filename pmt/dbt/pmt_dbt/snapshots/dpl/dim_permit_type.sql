{% snapshot dim_permit_type %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['permit_type_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with ats_data as (
	Select distinct 'ATS'	as src_sys_code
	  ,aai.authorization_instrument_id::varchar(25)	as permit_type_code
	  ,aai.authorization_instrument_name	as permit_type_description
	  ,'ATS' || '|'||  coalesce(cast(aai.authorization_instrument_id as varchar),'~')	as unqid
	from fdw_ods_ats_replication.ats_authorization_instruments aai 
	where 1=1
)
,
fta_data as (
	Select distinct 'FTA'	as src_sys_code
	 ,pfu.file_type_code::varchar(25)	AS permit_type_code
	, ftc.description	AS permit_type_description
	,'FTA' || '|'||  coalesce(cast(pfu.file_type_code as varchar(25)),'~') as unqid
	 from fdw_ods_fta_replication.prov_forest_use pfu
	 left join fdw_ods_fta_replication.file_type_code ftc 
		ON (ftc.file_type_code = pfu.file_type_code)
)
,
rrs_rp_data as (
	Select 'RRS_RP'	as src_sys_code
	,rttc.road_tenure_type_code as permit_type_code
	,rttc.description 			as permit_type_description
	,'RRS_RP' || '|'||  coalesce(cast(rttc.road_tenure_type_code as varchar(25)),'~') as unqid
	from fdw_ods_rrs_replication.road_tenure_type_code rttc
)
,
rrs_rup_data as (
	Select 'RRS_RUP'	as src_sys_code
	,'RUP' as permit_type_code
	,'Road Use Permit' as permit_type_description
	,'RRS_RUP' || '|'||  coalesce(cast('RUP' as varchar(25)),'~') as unqid
)
,
lexis_data as (
	Select 'LEXIS'	as src_sys_code
	,'EXP' as permit_type_code
	,'Log Export Permit' as permit_type_description
	,'LEXIS' || '|'||  coalesce(cast('EXP' as varchar(25)),'~') as unqid
)

--insert into pmt_dpl.dim_permit_type
select *
	from ats_data

union ALL
select * from fta_data

union ALL
select * from rrs_rup_data

union ALL
select * from rrs_rp_data

union all
select * from lexis_data



{% endsnapshot %}