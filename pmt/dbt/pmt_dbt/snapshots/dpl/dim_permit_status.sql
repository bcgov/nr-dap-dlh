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
,aasc.authorization_status_code as permit_status_code
,aasc.name as permit_status_description
,'ATS' || '|'||  coalesce(cast(aasc.authorization_status_code as varchar),'~') as unqid
from  fdw_ods_ats_replication.ats_authorization_status_codes aasc
where 1=1
)
--insert into pmt_dpl.dim_permit_status
select *
	from ats_data
;

{% endsnapshot %}
