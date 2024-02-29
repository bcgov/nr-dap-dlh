{% snapshot extract_permits %}

    {{
        config(
          target_schema='pmt_dal',
          strategy='check',
          unique_key='unqid',
          check_cols=['ministry_code','business_area_code','permit_status_code','permit_status_date','permit_issue_date','app_received_date','app_update_date','app_decision_date','app_issuance_date','app_accepted_date','app_rejected_date','app_adjudication_date','harvest_area_sq_m','harvest_auth_status','map_feature_id','permit_type_code','src_sys_description','ministry_description','business_area_description','authorization_status_description','permit_status_description','permit_type_description'],
		  invalidate_hard_deletes=True,
          bind=False,
        )
    }}
	
with extract_data as (
	SELECT fpt.src_sys_code,
		   ss.src_sys_description,
		   fpt.ministry_code,
		   mnt.ministry_description,
		   fpt.business_area_code,
		   bar.business_area_description,
		   fpt.application_id,
		   fpt.permit_id,
		   fpt.app_file_id,
		   fpt.authorization_status_code,
		   ast.authorization_status_description,
		   fpt.project_id,
		   prj.project_name,
		   fpt.permit_status_code,
		   pst.permit_status_description,
		   fpt.permit_status_date,
		   fpt.permit_issue_date,
		   fpt.permit_issue_expire_date,
		   fpt.app_received_date,
		   fpt.app_update_date,
		   fpt.app_decision_date,
		   fpt.app_issuance_date,
		   fpt.app_accepted_date,
		   fpt.app_rejected_date,
		   fpt.app_adjudication_date,
		   fpt.harvest_auth_status,
		   fpt.harvest_area_sq_m,
		   fpt.permit_org_unit_code,
		   case when fpt.src_sys_code = 'ATS' then null else org.org_unit_description end as district_description,
		   case when fpt.src_sys_code = 'ATS' then null else org.org_unit_code end as district_code,
		   case when fpt.src_sys_code = 'ATS' then null else org.rollup_district_description end as rollup_district_description,
		   case when fpt.src_sys_code = 'ATS' then null else org.rollup_district_code end as rollup_district_code,
		   case when fpt.src_sys_code = 'ATS' then case when atsorg.org_unit_description is not null then atsorg.org_unit_description else 
		   prj.project_region_description end else org.rollup_region_description end as region_name,
		   case when fpt.src_sys_code = 'ATS' then atsorg.org_unit_code else org.rollup_region_code end as region_code,
       case when fpt.src_sys_code = 'ATS' then atsorg.roll_up_area_code else org.roll_up_area_code end as area_code,
       case when fpt.src_sys_code = 'ATS' then atsorg.roll_up_area_description else org.roll_up_area_description end as area_description,
		   fpt.application_age,
		   fpt.map_feature_id,
		   fpt.permit_type_code,
		   ptp.permit_type_description,
		   fpt.unqid
	FROM pmt_dpl.fact_permits fpt
	left join pmt_dpl.dim_authorization_status ast 
	  on (fpt.authorization_status_code  = ast.authorization_status_code 
		 and ast.src_sys_code = fpt.src_sys_code 
		 and fpt.dbt_valid_to is null 
		 and ast.dbt_valid_to is null)
	left join pmt_dpl.dim_business_area bar 
	  on (bar.business_area_code = fpt.business_area_code
		 and bar.src_sys_code = 'CORP'
		 and fpt.dbt_valid_to is null 
		 and bar.dbt_valid_to is null)
	left join pmt_dpl.dim_ministry mnt 
	  on (mnt.ministry_code = fpt.ministry_code
		 and mnt.src_sys_code = 'CORP'
		 and fpt.dbt_valid_to is null 
		 and mnt.dbt_valid_to is null)     
	left join pmt_dpl.dim_org org 
	  on (org.org_unit_code = fpt.permit_org_unit_code
		 and org.src_sys_code = 'CORP'
		 and fpt.dbt_valid_to is null 
		 and org.dbt_valid_to is null)    
	left join pmt_dpl.dim_permit_status pst
	  on (pst.permit_status_code = fpt.permit_status_code
		 and pst.src_sys_code = fpt.src_sys_code
		 and fpt.dbt_valid_to is null 
		 and pst.dbt_valid_to is null)    
	left join pmt_dpl.dim_permit_type ptp
	  on (ptp.permit_type_code = fpt.permit_type_code
		 and ptp.src_sys_code = fpt.src_sys_code 
		 and fpt.dbt_valid_to is null 
		 and ptp.dbt_valid_to is null)    
	left join pmt_dpl.dim_project prj
	  on (prj.project_id = fpt.project_id
		 and prj.src_sys_code = fpt.src_sys_code 
		 and fpt.dbt_valid_to is null 
		 and prj.dbt_valid_to is null)  
	left join pmt_dpl.dim_org atsorg 
	  on (replace(atsorg.org_unit_description,' Natural Resource Region','') = case when prj.project_region_description = 'Kootenay Boundary' then 'Kootenay-Boundary'
																		when prj.project_region_description ='Thompson Okanagan' then 'Thompson-Okanagan'
																		when prj.project_region_description ='North East' then 'Northeast'
																		else prj.project_region_description end
		and atsorg.src_sys_code = 'CORP'
		and atsorg.org_unit_description !~'OBSOLETE' and atsorg.org_unit_description like '%Natural Resource Region%' 
		and atsorg.dbt_valid_to is null)
	left join pmt_dpl.dim_source_system ss
	  on (ss.src_sys_code = fpt.src_sys_code
		 and fpt.dbt_valid_to is null 
		 and ss.dbt_valid_to is null)
)
--insert into pmt_dal.extract_permits
Select * from extract_data

{% endsnapshot %}