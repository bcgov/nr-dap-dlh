create table pmt_dpl.dim_project
(
	src_sys_code 			varchar(20),
	project_id				varchar(20),
	project_name			varchar(125),
	project_location_code	varchar(200),
	--project_location_description	varchar(1000),
	project_status_code 	varchar(6),
	project_region_description varchar(125)
);

create table pmt_dpl.dim_business_area	
(
src_sys_code 			varchar(20),
business_area_code			varchar(20),
business_area_description	varchar(50)
);

create table pmt_dpl.dim_location
(
src_sys_code 			varchar(20),
location_code			varchar(200),
location_description	varchar(1000)
);

create table pmt_dpl.dim_ministry
(
src_sys_code 			varchar(20),
ministry_code			varchar(20),
ministry_description	varchar(100)
);

create table pmt_dpl.dim_org
(
src_sys_code 			varchar(20),
org_unit_code 			varchar(20),
org_unit_description	varchar(100),
rollup_region_code		varchar(20),
rollup_region_description		varchar(100),
rollup_district_code		varchar(20),
rollup_district_description		varchar(100),
roll_up_area_code				varchar(20),
roll_up_area_description		varchar(100)
);

create table pmt_dpl.dim_permit_status
(
src_sys_code 			varchar(20),
permit_status_code 			varchar(20),
permit_status_description 	varchar(100)
);

create table pmt_dpl.dim_permit_type
(
src_sys_code 			varchar(20),
permit_type_code 		varchar(20),
permit_description		varchar(100)
);

create table pmt_dpl.dim_source
(
Src_sys_code 			varchar(20),
Src_sys_description		varchar(100)
);

create table pmt_dpl.fact_permits(
src_sys_code 			varchar(20)
,permit_id				varchar(20)
,permit_status	varchar(50)
,permit_status_date	timestamp without time zone

,permit_issue_date	timestamp without time zone --potentially duplicate
,permit_issue_expire_date_calc	timestamp without time zone

,app_received_date	timestamp without time zone
,app_update_date	timestamp without time zone
,app_decision_date	timestamp without time zone
,app_issuance_date	timestamp without time zone --potentially duplicate
,app_accepted_date	timestamp without time zone
,app_rejected_date	timestamp without time zone
,app_adjudication_date	timestamp without time zone 
,harvest_auth_status	varchar(25)
,harvest_area_sq_m	numeric(10,2)
,permit_org_unit_code	varchar(50)

,application_age	numeric(10,2)
,app_authorization_id	varchar(20)
,app_auth_status	varchar(20)					--potentially duplicate
,app_instrument_name	varchar(50)
,app_status_code	varchar(20)  --potentially duplicate
,map_feature_id	varchar(20)
,permit_type_code	varchar(20)	--potentially duplicate
,permit_app_authorization_type	varchar(20)	--potentially duplicate
,permit_app_instrument_id	varchar(20)
)