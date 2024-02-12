{% snapshot dim_source_system %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['src_sys_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}

with corp_data as (
	select 'ATS' as src_sys_code,
	'Authorization Tracking System' as src_sys_description,
	'ATS' as unqid
	union ALL
	select 'FTA' as src_sys_code,
	'Forest Tenure Administration' as src_sys_description,
	'FTA' as unqid
	union ALL
	select 'RRS_RP' as src_sys_code,
	'Resource Roads System - Road Permits' as src_sys_description,
	'RRS_RP' as unqid
	union ALL
	select 'RRS_RUP' as src_sys_code,
	'Resource Roads System - Road Use Permits' as src_sys_description,
	'RRS_RUP' as unqid
)
--insert into pmt_dpl.dim_source_system
select * from corp_data
;

{% endsnapshot %}