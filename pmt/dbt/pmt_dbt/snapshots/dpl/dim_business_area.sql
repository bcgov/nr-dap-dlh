{% snapshot dim_business_area %}

    {{
        config(
          target_schema='pmt_dpl',
          strategy='check',
          unique_key='unqid',
          check_cols=['business_area_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}
with corp_data as (
	Select 'CORP' as src_sys_code,'AGRI' as business_area_code,'Agriculture' 					as business_area_description,'CORP|AGRI' as unqid union all
	Select 'CORP' as src_sys_code,'FAW'  as business_area_code,'Fish and Wildlife' 				as business_area_description,'CORP|FAW'  as unqid union all
	Select 'CORP' as src_sys_code,'FOR'  as business_area_code,'Forests'	 					as business_area_description,'CORP|FOR'  as unqid union all
	Select 'CORP' as src_sys_code,'LAN'  as business_area_code,'Lands'	 						as business_area_description,'CORP|LAN'  as unqid union all
	Select 'CORP' as src_sys_code,'MIN'  as business_area_code,'Mines'	 						as business_area_description,'CORP|MIN'  as unqid union all
	Select 'CORP' as src_sys_code,'PAR'  as business_area_code,'Parks'	 						as business_area_description,'CORP|PAR'  as unqid union all
	Select 'CORP' as src_sys_code,'ROA'  as business_area_code,'Roads'	 						as business_area_description,'CORP|ROA'  as unqid union all
	Select 'CORP' as src_sys_code,'RECS' as business_area_code,'Recreation Sites and Trails'	as business_area_description,'CORP|RECS' as unqid union all
	Select 'CORP' as src_sys_code,'WAT'  as business_area_code,'Water'	 						as business_area_description,'CORP|WAT'  as unqid
)
--insert into pmt_dpl.dim_business_area
select *
	from corp_data


{% endsnapshot %}