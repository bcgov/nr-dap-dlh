CREATE TABLE IF NOT EXISTS pmt_dpl.dim_business_area
(
   src_sys_code               varchar(20),
   business_area_code         varchar(20),
   business_area_description  varchar(50)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_location
(
   src_sys_code   varchar(20),
   location_code  varchar(200),
   project_code   varchar(125)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_ministry
(
   src_sys_code          varchar(20),
   ministry_code         varchar(20),
   ministry_description  varchar(100)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_org
(
   src_sys_code                 varchar(20),
   org_unit_code                varchar(20),
   org_unit_description         varchar(100),
   rollup_region_code           varchar(20),
   rollup_region_description    varchar(100),
   rollup_district_code         varchar(20),
   rollup_district_description  varchar(100),
   roll_up_area_code            varchar(20),
   roll_up_area_description     varchar(100)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_permit_status
(
   src_sys_code               varchar(20),
   permit_status_code         varchar(20),
   permit_status_description  varchar(100)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_permit_type
(
   src_sys_code        varchar(20),
   permit_type_code    varchar(125),
   permit_description  varchar(125)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_project
(
   src_sys_code                varchar(20),
   project_id                  varchar(20),
   project_name                varchar(125),
   project_location_code       varchar(200),
   project_status_code         varchar(6),
   project_region_description  varchar(125)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_source_system
(
   src_sys_code         varchar(20),
   src_sys_description  varchar(100)
);

CREATE TABLE IF NOT EXISTS pmt_dpl.fact_permits
(
   src_sys_code              text,
   ministry_code             text,
   business_area_code        text,
   permit_id                 numeric(38),
   app_file_id               varchar(32),
   permit_status_code        varchar(6),
   permit_status_date        timestamp(0),
   permit_issue_date         timestamp(0),
   permit_issue_expire_date  date,
   app_received_date         timestamp(0),
   app_update_date           timestamp(0),
   app_decision_date         timestamp(0),
   app_issuance_date         timestamp(0),
   app_accepted_date         timestamp(0),
   app_rejected_date         date,
   app_adjudication_date     timestamp(0),
   harvest_auth_status       text,
   harvest_area_sq_m         integer,
   permit_org_unit_code      text,
   application_age           numeric,
   map_feature_id            text,
   permit_type_code          numeric(38)
);